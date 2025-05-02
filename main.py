import asyncio
import aiofiles
import os
import time
import json
import logging
from typing import Optional, List, Dict
import pandas as pd

SOCKET_PATH = str(os.getenv("USER_DB_PATH", "/tmp/user_db.sock"))
RECORD_LIMIT = 5000
FIELD_SEPARATOR = b'|'
FIELDS = ['id', 'username', 'password_hash', 'ip_reg', 'last_logged', 'last_ip']
MAX_LENGTHS = {
    'username': 24,
    'password_hash': 64,
    'ip_reg': 16,
    'last_logged': 19,
    'last_ip': 16,
}
LOG_LIMIT = 100
RECORD_SIZE = sum(MAX_LENGTHS.get(f, 0) for f in FIELDS[1:]) + (len(FIELDS) - 1) + 10
TTL_SECONDS = 5 * 3600  # 5 hours
IO_TIMEOUT = 5.0  # Timeout for I/O operations in seconds

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class ShardManager:
    def __init__(self, base_path: str = 'shards'):
        self.base_path = base_path
        self._lock = asyncio.Lock()
        self.cache = pd.DataFrame(columns=['ref'] + FIELDS + ['timestamp'])
        try:
            os.makedirs(self.base_path, exist_ok=True)
            if not os.access(self.base_path, os.W_OK | os.R_OK):
                logging.error(f"No read/write permissions for {self.base_path}")
                raise OSError(f"No read/write permissions for {self.base_path}")
        except OSError as e:
            logging.error(f"Failed to create directory {self.base_path}: {e}")
            raise

    @staticmethod
    def _sanitize_shard_prefix(username: str) -> str:
        if not username or not username[0].isalpha():
            return 'q'
        return username[0].lower()

    @staticmethod
    def _clean_field(value: str) -> str:
        return value.replace('\n', '').replace('|', '')

    def _get_shard_info_path(self, prefix: str) -> str:
        return os.path.join(self.base_path, f'{prefix}.info')

    def _get_shard_data_path(self, prefix: str, index: int) -> str:
        return os.path.join(self.base_path, f'{prefix}{index}')

    @staticmethod
    def _validate_ref(ref: str) -> tuple[str, int]:
        try:
            shard, idx = ref.split(':')
            return shard, int(idx)
        except ValueError:
            raise ValueError(f"Invalid reference format: {ref}")

    def _load_info_sync(self, prefix: str) -> Dict:
        path = self._get_shard_info_path(prefix)
        try:
            if not os.path.exists(path):
                return {'shards': 0, 'free': {}, 'log': []}
            if not os.access(path, os.R_OK):
                logging.error(f"No read permission for {path}")
                return {'shards': 0, 'free': {}, 'log': []}
            with open(path, 'r') as f:
                content = f.read()
                if not content.strip():
                    logging.error(f"Empty info file at {path}")
                    return {'shards': 0, 'free': {}, 'log': []}
                try:
                    data = json.loads(content)
                except json.JSONDecodeError as e:
                    logging.error(f"Invalid JSON in {path}: {e}")
                    return {'shards': 0, 'free': {}, 'log': []}
                if not isinstance(data, dict) or 'shards' not in data or 'free' not in data or 'log' not in data:
                    logging.error(f"Invalid info structure at {path}")
                    return {'shards': 0, 'free': {}, 'log': []}
                return data
        except OSError as e:
            logging.error(f"Failed to load info from {path}: {e}")
            return {'shards': 0, 'free': {}, 'log': []}

    async def _load_info(self, prefix: str) -> Dict:
        loop = asyncio.get_running_loop()
        try:
            return await asyncio.wait_for(
                loop.run_in_executor(None, self._load_info_sync, prefix),
                timeout=IO_TIMEOUT
            )
        except asyncio.TimeoutError:
            logging.error(f"Timeout loading info for prefix {prefix}")
            raise

    def _save_info_sync(self, prefix: str, info: Dict):
        path = self._get_shard_info_path(prefix)
        temp_path = path + '.tmp'
        try:
            with open(temp_path, 'w') as f:
                f.write(json.dumps(info, indent=2))
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, path)
            os.chmod(path, 0o644)
        except OSError as e:
            logging.error(f"Failed to save info for {prefix}: {e}")
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    async def _save_info(self, prefix: str, info: Dict):
        loop = asyncio.get_running_loop()
        try:
            await asyncio.wait_for(
                loop.run_in_executor(None, self._save_info_sync, prefix, info),
                timeout=IO_TIMEOUT
            )
        except asyncio.TimeoutError:
            logging.error(f"Timeout saving info for prefix {prefix}")
            raise

    @staticmethod
    async def _get_next_id(data_path: str, info: Dict, shard_index: int) -> int:
        free = info['free'].get(str(shard_index), [])
        if free:
            next_id = free.pop(0)
            return next_id
        try:
            if not os.path.exists(data_path):
                return 0
            async with aiofiles.open(data_path, 'rb') as f:
                await f.seek(0, os.SEEK_END)
                file_size = await f.tell()
                count = file_size // RECORD_SIZE
                return count
        except OSError as e:
            logging.error(f"Failed to count records in {data_path}: {e}")
            raise

    @staticmethod
    async def _write_record(data_path: str, record_id: int, data: bytes):
        try:
            mode = 'r+b' if os.path.exists(data_path) else 'wb'
            async with aiofiles.open(data_path, mode) as f:
                await f.seek(record_id * RECORD_SIZE)
                if len(data) != RECORD_SIZE:
                    logging.error(f"Invalid record size: expected {RECORD_SIZE}, got {len(data)}")
                    raise ValueError(f"Invalid record size: expected {RECORD_SIZE}, got {len(data)}")
                await f.write(data)
                await f.flush()
        except OSError as e:
            logging.error(f"Failed to write record to {data_path}: {e}")
            raise

    @staticmethod
    def _validate_fields(**kwargs) -> None:
        for field, value in kwargs.items():
            if field in MAX_LENGTHS and len(value) > MAX_LENGTHS[field]:
                raise ValueError(f"{field} exceeds maximum length of {MAX_LENGTHS[field]}")
            if not isinstance(value, str):
                raise ValueError(f"{field} must be a string")

    async def _remove_expired_records(self):
        logging.debug("Starting _remove_expired_records")
        current_time = int(time.time())
        if self.cache.empty:
            logging.debug("Cache is empty, no records to remove")
            return

        expired = self.cache[self.cache['timestamp'] < current_time - TTL_SECONDS]
        if expired.empty:
            logging.debug("No expired records found")
            return

        logging.debug(f"Found {len(expired)} expired records")
        expired_by_prefix = {}
        for ref in expired['ref']:
            try:
                shard, record_id = self._validate_ref(ref)
                prefix, shard_index = shard[:-1], int(shard[-1])
                if prefix not in expired_by_prefix:
                    expired_by_prefix[prefix] = {}
                expired_by_prefix[prefix].setdefault(shard_index, []).append(record_id)
            except ValueError as e:
                logging.error(f"Invalid ref in cache: {ref}, error: {e}")
                continue

        async with self._lock:
            logging.debug("Acquired lock in _remove_expired_records")
            for prefix, shards in expired_by_prefix.items():
                logging.debug(f"Processing prefix: {prefix}")
                try:
                    info = await self._load_info(prefix)
                    logging.debug(f"Loaded info for prefix: {prefix}")
                    for shard_index, record_ids in shards.items():
                        path = self._get_shard_data_path(prefix, shard_index)
                        logging.debug(f"Processing shard: {path}")
                        if not os.path.exists(path):
                            logging.warning(f"Shard {path} does not exist")
                            continue
                        try:
                            async with aiofiles.open(path, 'r+b') as f:
                                for record_id in record_ids:
                                    logging.debug(f"Deleting record {prefix}{shard_index}:{record_id}")
                                    await f.seek(record_id * RECORD_SIZE)
                                    await f.write(b'\x00' * RECORD_SIZE)
                                await f.flush()
                            info['free'].setdefault(str(shard_index), []).extend(record_ids)
                            ts = int(time.time())
                            for record_id in record_ids:
                                info['log'].append((ts, f'DELETE {prefix}{shard_index}:{record_id} (TTL)'))
                            info['log'] = info['log'][-LOG_LIMIT:]
                            await self._save_info(prefix, info)
                            logging.debug(f"Updated shard info for {prefix}{shard_index}")
                        except Exception as e:
                            logging.error(f"Failed to delete records in {path}: {e}")
                            continue
                except Exception as e:
                    logging.error(f"Failed to process prefix {prefix}: {e}")
                    continue
            self.cache = self.cache[self.cache['timestamp'] >= current_time - TTL_SECONDS]
            logging.debug("Cache updated, expired records removed")
        logging.debug("Finished _remove_expired_records")

    async def add_record(self, username: str, password_hash: str, ip_reg: str,
                         last_logged: str, last_ip: str) -> str:
        logging.debug(f"Starting add_record for username: {username}")
        if not username or not username[0].isalpha():
            raise ValueError("Username must start with a letter")

        cleaned_fields = {
            'username': self._clean_field(username),
            'password_hash': self._clean_field(password_hash),
            'ip_reg': self._clean_field(ip_reg),
            'last_logged': self._clean_field(last_logged),
            'last_ip': self._clean_field(last_ip)
        }
        self._validate_fields(**cleaned_fields)
        logging.debug("Fields validated")

        prefix = self._sanitize_shard_prefix(cleaned_fields['username'])
        async with self._lock:
            logging.debug("Acquired lock")
            await self._remove_expired_records()
            logging.debug("Expired records removed")
            info = await self._load_info(prefix)
            logging.debug("Shard info loaded")
            shard_index = info['shards'] - 1 if info['shards'] else 0
            data_path = self._get_shard_data_path(prefix, shard_index)

            info['free'].setdefault(str(shard_index), [])
            id_ = await self._get_next_id(data_path, info, shard_index)
            logging.debug(f"Got next ID: {id_}")
            if id_ >= RECORD_LIMIT:
                shard_index += 1
                info['shards'] = shard_index + 1
                data_path = self._get_shard_data_path(prefix, shard_index)
                id_ = 0
                info['free'][str(shard_index)] = []
            elif info['shards'] == 0:
                info['shards'] = 1

            fields = [
                str(id_).ljust(10),
                cleaned_fields['username'].ljust(MAX_LENGTHS['username']),
                cleaned_fields['password_hash'].ljust(MAX_LENGTHS['password_hash']),
                cleaned_fields['ip_reg'].ljust(MAX_LENGTHS['ip_reg']),
                cleaned_fields['last_logged'].ljust(MAX_LENGTHS['last_logged']),
                cleaned_fields['last_ip'].ljust(MAX_LENGTHS['last_ip'])
            ]
            record_parts = []
            for k, f in zip(FIELDS, fields):
                max_len = MAX_LENGTHS.get(k, 10)
                encoded = f.encode()
                padded = encoded.ljust(max_len, b' ')
                record_parts.append(padded)
            record = FIELD_SEPARATOR.join(record_parts)
            logging.debug("Record prepared")

            await self._write_record(data_path, id_, record)
            logging.debug("Record written to shard")
            ref = f'{prefix}{shard_index}:{id_}'
            ts = int(time.time())
            info['log'].append((ts, f'CREATE {ref}'))
            info['log'] = info['log'][-LOG_LIMIT:]
            await self._save_info(prefix, info)
            logging.debug("Shard info saved")

            cache_entry = {'ref': ref, 'timestamp': ts}
            for k, v in zip(FIELDS, fields):
                cache_entry[k] = v.strip()
            self.cache.loc[len(self.cache)] = cache_entry
            logging.debug("Record added to cache")

        logging.debug(f"Finished add_record, returning ref: {ref}")
        return ref

    async def get_record(self, ref: str, fields: Optional[List[str]] = None) -> Optional[Dict[str, str]]:
        logging.debug(f"Starting get_record for ref: {ref}")
        await self._remove_expired_records()
        cache_hit = self.cache[self.cache['ref'] == ref]
        if not cache_hit.empty:
            record = cache_hit.iloc[0].to_dict()
            result = {k: record[k] for k in FIELDS if fields is None or k in fields}
            logging.debug(f"Cache hit for {ref}")
            return result

        try:
            shard, record_id = self._validate_ref(ref)
            prefix, shard_index = shard[:-1], int(shard[-1])
            path = self._get_shard_data_path(prefix, shard_index)
            if not os.path.exists(path):
                logging.debug(f"Shard path {path} does not exist")
                return None
            async with aiofiles.open(path, 'rb') as f:
                await f.seek(record_id * RECORD_SIZE)
                raw = await f.read(RECORD_SIZE)
                if len(raw) != RECORD_SIZE:
                    logging.warning(f"Record {ref} is incomplete or does not exist")
                    return None
                parts = raw.split(FIELD_SEPARATOR)
                if len(parts) != len(FIELDS):
                    logging.error(f"Malformed record at {ref}")
                    return None
                record = {k: v.decode('utf-8', errors='ignore').strip() for k, v in zip(FIELDS, parts)}
                cache_entry = {'ref': ref, 'timestamp': int(time.time())}
                cache_entry.update(record)
                self.cache.loc[len(self.cache)] = cache_entry
                logging.debug(f"Loaded {ref} into cache from shard")
                result = {k: record[k] for k in FIELDS if fields is None or k in fields}
                return result
        except Exception as e:
            logging.error(f"Failed to get record {ref}: {e}")
            return None

    async def delete_record(self, ref: str) -> bool:
        logging.debug(f"Starting delete_record for ref: {ref}")
        try:
            shard, record_id = self._validate_ref(ref)
            prefix, shard_index = shard[:-1], int(shard[-1])
            path = self._get_shard_data_path(prefix, shard_index)
            async with self._lock:
                await self._remove_expired_records()
                info = await self._load_info(prefix)
                async with aiofiles.open(path, 'r+b') as f:
                    await f.seek(record_id * RECORD_SIZE)
                    await f.write(b'\x00' * RECORD_SIZE)
                    await f.flush()
                info['free'].setdefault(str(shard_index), []).append(record_id)
                ts = int(time.time())
                info['log'].append((ts, f'DELETE {shard}:{record_id}'))
                info['log'] = info['log'][-LOG_LIMIT:]
                await self._save_info(prefix, info)
                self.cache = self.cache[self.cache['ref'] != ref]
                logging.debug(f"Deleted {ref} from cache and shard")
            return True
        except (ValueError, OSError) as e:
            logging.error(f"Failed to delete record {ref}: {e}")
            return False

    async def update_record(self, ref: str, updates: Dict[str, str]) -> bool:
        logging.debug(f"Starting update_record for ref: {ref}")
        try:
            shard, record_id = self._validate_ref(ref)
            prefix, shard_index = shard[:-1], int(shard[-1])
            path = self._get_shard_data_path(prefix, shard_index)
            await self._remove_expired_records()
            record = await self.get_record(ref)
            if not record:
                logging.debug(f"Record {ref} not found")
                return False
            cleaned_updates = {k: self._clean_field(v) for k, v in updates.items()}
            self._validate_fields(**cleaned_updates)
            record.update({k: v[:MAX_LENGTHS[k]] for k, v in updates.items() if k in MAX_LENGTHS})
            record['id'] = str(record_id).ljust(10)
            record_parts = []
            for k in FIELDS:
                max_len = MAX_LENGTHS.get(k, 10)
                encoded = record[k].encode()
                padded = encoded.ljust(max_len, b' ')
                record_parts.append(padded)
            data = FIELD_SEPARATOR.join(record_parts)
            await self._write_record(path, record_id, data)
            async with self._lock:
                info = await self._load_info(prefix)
                ts = int(time.time())
                info['log'].append((ts, f'UPDATE {shard}:{record_id}'))
                info['log'] = info['log'][-LOG_LIMIT:]
                await self._save_info(prefix, info)
                cache_idx = self.cache[self.cache['ref'] == ref].index
                if not cache_idx.empty:
                    for k, v in cleaned_updates.items():
                        if k in MAX_LENGTHS:
                            self.cache.loc[cache_idx, k] = v[:MAX_LENGTHS[k]]
                    self.cache.loc[cache_idx, 'timestamp'] = ts
                else:
                    cache_entry = {'ref': ref, 'timestamp': ts}
                    cache_entry.update(record)
                    self.cache.loc[len(self.cache)] = cache_entry
                logging.debug(f"Updated {ref} in cache and shard")
            return True
        except (ValueError, OSError) as e:
            logging.error(f"Failed to update record {ref}: {e}")
            return False

    async def find_records(self, field: str, value: str,
                           fields: Optional[List[str]] = None) -> List[Dict[str, str]]:
        logging.debug(f"Starting find_records for field: {field}, value: {value}")
        if field not in FIELDS:
            raise ValueError(f"Invalid field: {field}")
        await self._remove_expired_records()
        cache_hits = self.cache[self.cache[field] == value]
        result = []
        for _, row in cache_hits.iterrows():
            entry = {'ref': row['ref']}
            if fields:
                entry.update({k: row[k] for k in fields if k in row})
            else:
                entry.update({k: row[k] for k in FIELDS})
            result.append(entry)
        logging.debug(f"Found {len(result)} records in cache")

        try:
            for fname in os.listdir(self.base_path):
                if fname.endswith('.info'):
                    prefix = fname.split('.')[0]
                    info = await self._load_info(prefix)
                    for i in range(info['shards']):
                        path = self._get_shard_data_path(prefix, i)
                        if not os.path.exists(path):
                            continue
                        async with aiofiles.open(path, 'rb') as f:
                            await f.seek(0, os.SEEK_END)
                            file_size = await f.tell()
                            record_count = file_size // RECORD_SIZE
                            for record_id in range(record_count):
                                await f.seek(record_id * RECORD_SIZE)
                                raw = await f.read(RECORD_SIZE)
                                if len(raw) != RECORD_SIZE:
                                    logging.error(
                                        f"Corrupted record size at {prefix}{i}:{record_id}: expected {RECORD_SIZE}, got {len(raw)}")
                                    continue
                                if not raw.strip():
                                    continue
                                values = raw.split(FIELD_SEPARATOR)
                                if len(values) != len(FIELDS):
                                    logging.error(f"Corrupted record at {prefix}{i}:{record_id}")
                                    continue
                                record = {k: v.decode().strip() for k, v in zip(FIELDS, values)}
                                ref = f'{prefix}{i}:{record_id}'
                                if record.get(field) == value and ref not in self.cache['ref'].values:
                                    cache_entry = {'ref': ref, 'timestamp': int(time.time())}
                                    cache_entry.update(record)
                                    self.cache.loc[len(self.cache)] = cache_entry
                                    entry = {'ref': ref}
                                    if fields:
                                        entry.update({k: record[k] for k in fields if k in record})
                                    else:
                                        entry.update(record)
                                    result.append(entry)
            logging.debug(f"Total records found: {len(result)}")
            return result
        except OSError as e:
            logging.error(f"Failed to search records: {e}")
            return []


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, manager: ShardManager):
    addr = writer.get_extra_info('peername')
    logging.info(f"New client connected: {addr}")

    try:
        while True:
            data = await reader.readline()
            if not data:
                break

            request = data.decode().strip()
            logging.debug(f"Received from {addr}: {request}")

            if request.startswith("CREATE "):
                try:
                    parts = request.split(' ', maxsplit=5)
                    if len(parts) != 6:
                        raise ValueError("Invalid CREATE format. Use: CREATE username pwdhash ip time ip2")
                    _, username, pwdhash, ip_reg, last_logged, last_ip = parts
                    ref = await manager.add_record(username, pwdhash, ip_reg, last_logged, last_ip)
                    writer.write(f"OK {ref}\n".encode())
                    logging.info(f"CREATE successful for {addr}: {ref}")
                except Exception as e:
                    writer.write(f"ERROR {str(e)}\n".encode())
                    logging.warning(f"CREATE failed for {addr}: {e}")

            elif request.startswith("GET "):
                try:
                    parts = request.split(' ', maxsplit=2)
                    if len(parts) < 2:
                        raise ValueError("Invalid GET format. Use: GET ref [fields]")
                    ref = parts[1]
                    fields_str = parts[2] if len(parts) == 3 and parts[2] else None
                    fields = fields_str.split(',') if fields_str else None
                    if fields:
                        invalid_fields = [f for f in fields if f not in FIELDS]
                        if invalid_fields:
                            raise ValueError(f"Invalid fields: {', '.join(invalid_fields)}")
                    record = await manager.get_record(ref, fields)
                    if record is None:
                        writer.write(f"ERROR Record {ref} not found\n".encode())
                        logging.debug(
                            f"GET failed for {addr}: Record {ref} not found")
                    else:
                        writer.write(f"OK {json.dumps(record)}\n".encode())
                        logging.debug(f"GET successful for {addr}: {ref}")
                except Exception as e:
                    writer.write(f"ERROR {str(e)}\n".encode())
                    logging.warning(f"GET failed for {addr}: {e}")

            elif request.startswith("DELETE "):
                try:
                    parts = request.split(' ', maxsplit=1)
                    if len(parts) != 2:
                        raise ValueError("Invalid DELETE format. Use: DELETE ref")
                    ref = parts[1]
                    success = await manager.delete_record(ref)
                    if not success:
                        writer.write(f"ERROR Failed to delete record {ref}\n".encode())
                    else:
                        writer.write("OK Deleted\n".encode())
                        logging.info(f"DELETE successful for {addr}: {ref}")
                except Exception as e:
                    writer.write(f"ERROR {str(e)}\n".encode())
                    logging.warning(f"DELETE failed for {addr}: {e}")

            elif request.startswith("UPDATE "):
                try:
                    parts = request.split(' ', maxsplit=2)
                    if len(parts) < 3:
                        raise ValueError("Invalid UPDATE format. Use: UPDATE ref field1=value1 field2=value2 ...")
                    ref = parts[1]
                    updates = {}
                    for pair in parts[2].split():
                        if '=' not in pair:
                            raise ValueError(f"Invalid update pair: {pair}")
                        key, value = pair.split('=', 1)
                        if key not in FIELDS:
                            raise ValueError(f"Invalid field for update: {key}")
                        if key == 'id':
                            raise ValueError("Cannot update 'id' field")
                        updates[key] = value
                    success = await manager.update_record(ref, updates)
                    if not success:
                        writer.write(f"ERROR Failed to update record {ref}\n".encode())
                    else:
                        writer.write("OK Updated\n".encode())
                        logging.info(f"UPDATE successful for {addr}: {ref}")
                except Exception as e:
                    writer.write(f"ERROR {str(e)}\n".encode())
                    logging.warning(f"UPDATE failed for {addr}: {e}")

            elif request.startswith("FIND "):
                try:
                    parts = request.split(' ', maxsplit=3)
                    if len(parts) < 3:
                        raise ValueError("Invalid FIND format. Use: FIND field value [fields]")
                    field, value = parts[1], parts[2]
                    fields_str = parts[3] if len(parts) == 4 and parts[3] else None
                    fields = fields_str.split(',') if fields_str else None
                    if field not in FIELDS:
                        raise ValueError(f"Invalid field for search: {field}")
                    if fields:
                        invalid_fields = [f for f in fields if f not in FIELDS]
                        if invalid_fields:
                            raise ValueError(f"Invalid fields requested: {', '.join(invalid_fields)}")
                    records = await manager.find_records(field, value, fields)
                    writer.write(f"OK {json.dumps(records)}\n".encode())
                    logging.debug(f"FIND successful for {addr}: field={field}, value={value}, found={len(records)}")
                except Exception as e:
                    writer.write(f"ERROR {str(e)}\n".encode())
                    logging.warning(f"FIND failed for {addr}: {e}")

            else:
                writer.write("ERROR UNKNOWN COMMAND\n".encode())
                logging.warning(f"Unknown command from {addr}: {request}")

            await writer.drain()
    except asyncio.CancelledError:
        logging.info(f"Client handler for {addr} cancelled.")
    except ConnectionResetError:
        logging.info(f"Client {addr} disconnected abruptly.")
    except Exception as e:
        logging.error(f"Error during client handling for {addr}: {e}", exc_info=True)
    finally:
        if not writer.is_closing():
            writer.close()
            try:
                await writer.wait_closed()
            except Exception as e:
                logging.error(f"Error closing writer for {addr}: {e}")
        logging.info(f"Client connection closed: {addr}")


async def start_unix_socket_server():
    if os.path.exists(SOCKET_PATH):
        logging.warning(f"Socket file {SOCKET_PATH} already exists, removing.")
        try:
            os.remove(SOCKET_PATH)
        except OSError as e:
            logging.error(f"Failed to remove existing socket file {SOCKET_PATH}: {e}")
            return

    try:
        manager = ShardManager(base_path='shards')
        logging.info("ShardManager initialized successfully.")
    except Exception as e:
        logging.critical(f"Failed to initialize ShardManager: {e}", exc_info=True)
        return

    server_factory = lambda: asyncio.start_unix_server(
        lambda r, w: handle_client(r, w, manager),
        path=SOCKET_PATH
    )

    try:
        server = await server_factory()
        logging.info(f"UNIX socket server started at {SOCKET_PATH}")

        async with server:
            await server.serve_forever()

    except OSError as e:
        logging.critical(f"Failed to start server at {SOCKET_PATH}: {e}", exc_info=True)
    except Exception as e:
        logging.critical(f"An unexpected error occurred in the server loop: {e}", exc_info=True)


if __name__ == "__main__":
    try:
        asyncio.run(start_unix_socket_server())
    except KeyboardInterrupt:
        logging.info("Server stopping due to KeyboardInterrupt.")
    except Exception as e:
        logging.critical(f"Server failed to run: {e}", exc_info=True)
    finally:
        if os.path.exists(SOCKET_PATH):
            try:
                os.remove(SOCKET_PATH)
                logging.info(f"Removed socket file {SOCKET_PATH}.")
            except OSError as e:
                logging.error(f"Error removing socket file {SOCKET_PATH} on exit: {e}")
