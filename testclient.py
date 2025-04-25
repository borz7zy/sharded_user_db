import asyncio
import json
import os


async def send_command(command: str) -> str:
    reader, writer = await asyncio.open_unix_connection(str(os.getenv("USER_DB_PATH")))
    try:
        writer.write(f"{command}\n".encode())
        await writer.drain()
        data = await reader.readline()
        return data.decode().strip()
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    print(await send_command("CREATE alice hashed_pass123 192.168.1.1 2023-10-01_12:00:00 192.168.1.2"))

    print(await send_command("GET a0:0 username,ip_reg,password_hash"))

    print(await send_command("FIND username alice username,ip_reg"))

    print(await send_command("UPDATE a0:0 last_ip=192.168.1.3 last_logged=2023-10-02_12:00:00"))

    print(await send_command("DELETE a0:0"))


if __name__ == "__main__":
    asyncio.run(main())
