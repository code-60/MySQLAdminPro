from __future__ import annotations

import os
import socket
import threading
import webbrowser

from waitress import serve

from app import app, find_free_port


def is_port_available(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        return sock.connect_ex((host, port)) != 0


def main() -> None:
    host = "127.0.0.1"
    requested_port = int(os.getenv("APP_PORT", "5001"))
    port = requested_port if is_port_available(host, requested_port) else find_free_port()

    url = f"http://{host}:{port}/"

    # Open browser after server startup without blocking the process.
    threading.Timer(1.0, lambda: webbrowser.open(url)).start()
    serve(app, host=host, port=port, threads=8)


if __name__ == "__main__":
    main()
