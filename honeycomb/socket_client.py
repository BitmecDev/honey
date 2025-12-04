# services/socketio_client.py
from __future__ import annotations

import threading
from typing import Callable, Optional, Dict, Any, Tuple
from urllib.parse import urlparse

import socketio
from engineio.exceptions import ConnectionError as EIOConnectionError
from socketio.exceptions import ConnectionError as SIOConnectionError

DEFAULT_URL = "https://socketio.bitmec.com:2096"

JSON = Dict[str, Any]
Predicate = Callable[[JSON], bool]


class SocketBroker:
    def __init__(self, url: str = DEFAULT_URL):
        self.url = url.rstrip("/")
        self._sio: Optional[socketio.Client] = None
        self._lock = threading.Lock()

    def _connect(self, transports, socketio_path, headers, wait_timeout):
        self._sio = socketio.Client(
            reconnection=False,
            ssl_verify=False,
            logger=False,
            engineio_logger=False,
            request_timeout=max(1.0, float(wait_timeout) - 1.0),
        )
        self._sio.on("connect", lambda: None)
        self._sio.on("connect_error", lambda e: None)
        self._sio.connect(
            self.url,
            transports=transports,
            socketio_path=socketio_path,
            headers=headers,
            wait=True,
            wait_timeout=wait_timeout,
            namespaces=None,
        )

    def _ensure_client(self) -> socketio.Client:
        with self._lock:
            if self._sio and self._sio.connected:
                return self._sio
            parsed = urlparse(self.url)
            origin_default = f"{parsed.scheme}://{parsed.hostname}" + (f":{parsed.port}" if parsed.port else "")
            attempts = [
                (["websocket"], "socket.io", {"Origin": origin_default}, 10.0),
                (["polling", "websocket"], "socket.io", {"Origin": origin_default}, 12.0),
                (["polling", "websocket"], "socket.io", None, 12.0),
                (["websocket"], "socket.io", None, 10.0),
            ]
            last_exc: Optional[BaseException] = None
            for transports, path, headers, wtimeout in attempts:
                try:
                    self._connect(transports, path, headers, wtimeout)
                    return self._sio  # type: ignore[return-value]
                except (EIOConnectionError, SIOConnectionError, OSError) as exc:
                    last_exc = exc
                except Exception as exc:
                    last_exc = exc
            raise RuntimeError(f"Socket.IO connect failed: {type(last_exc).__name__}: {last_exc}")

    @staticmethod
    def _safe_unbind(sio: "socketio.Client", event_name: str, namespace: str = "/") -> None:
        off = getattr(sio, "off", None)
        if callable(off):
            try:
                off(event_name)
                return
            except Exception:
                pass
        handlers = getattr(sio, "handlers", None)
        if isinstance(handlers, dict):
            ns_handlers = handlers.get(namespace)
            if isinstance(ns_handlers, dict) and event_name in ns_handlers:
                ns_handlers.pop(event_name, None)

    @classmethod
    def _safe_bind(cls, sio: "socketio.Client", event_name: str, handler, namespace: str = "/") -> None:
        cls._safe_unbind(sio, event_name, namespace)
        sio.on(event_name, handler)

    @staticmethod
    def _normalize_payload(raw_payload: Any, assumed_channel: str) -> Tuple[str, JSON, JSON]:
        if isinstance(raw_payload, dict) and "message" in raw_payload and "channel" in raw_payload:
            channel = str(raw_payload.get("channel"))
            inner = raw_payload.get("message") or {}
            if not isinstance(inner, dict):
                inner = {"value": inner}
            normalized = {"channel": channel, "message": inner, "__raw__": raw_payload}
            return channel, inner, normalized
        if isinstance(raw_payload, dict):
            channel = assumed_channel
            inner = raw_payload
            normalized = {"channel": channel, "message": inner, "__raw__": raw_payload}
            return channel, inner, normalized
        channel = assumed_channel
        inner = {"value": raw_payload}
        normalized = {"channel": channel, "message": inner, "__raw__": raw_payload}
        return channel, inner, normalized

    def publish_and_wait(
        self,
        sub_channel: str,
        pub_channel: str,
        message: Dict[str, Any],
        predicate: Optional[Callable[[Dict[str, Any]], bool]] = None,
        timeout: int = 15,
    ) -> Optional[Dict[str, Any]]:
        sio = self._ensure_client()
        event = threading.Event()
        answer: Optional[Dict[str, Any]] = None

        def on_any(payload: Any) -> None:
            nonlocal answer
            try:
                channel, inner, normalized = self._normalize_payload(payload, sub_channel)
                if channel != sub_channel:
                    return
                if predicate is not None and not predicate(inner):
                    return
                answer = normalized
                event.set()
            except Exception:
                pass

        self._safe_bind(sio, "message", on_any, "/")
        self._safe_bind(sio, sub_channel, on_any, "/")

        try:
            sio.emit("subscribe", sub_channel)
        except Exception:
            sio.emit("subscribe", {"channel": sub_channel})

        sio.emit("publish", {"channel": pub_channel, "message": message})

        ok = event.wait(timeout)

        self._safe_unbind(sio, "message", "/")
        self._safe_unbind(sio, sub_channel, "/")

        return answer if ok else None

    def close(self):
        with self._lock:
            if self._sio:
                try:
                    if getattr(self._sio, "connected", False):
                        self._sio.disconnect()
                except Exception:
                    pass
                self._sio = None
                
    def publish(self, pub_channel: str, message: Dict[str, Any]) -> None:
        sio = self._ensure_client()
        sio.emit("publish", {"channel": pub_channel, "message": message})


broker = SocketBroker()
