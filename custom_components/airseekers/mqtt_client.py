"""
Airseekers MQTT client — AWS IoT Core via mTLS.

Design: this class manages a SINGLE connection lifetime. It does NOT reconnect.
Reconnection (with fresh credentials) is the coordinator's responsibility.
The coordinator fetches a new unique mqtt_client_id from the API each time,
avoiding session-takeover conflicts with the mobile app.

Broker:  AWS IoT Core
Auth:    mTLS — cert_key (client cert) + private_key, ca (Amazon Root CA)
Topics:
  Subscribe: as/{sn}/up    device → cloud (status, responses)
  Publish:   as/{sn}/down  cloud → device (commands)
"""
from __future__ import annotations

import asyncio
import logging
import os
import ssl
import tempfile
from typing import Any
from collections.abc import Callable

from .const import (
    MQTT_DEFAULT_PORT,
    MQTT_KEEPALIVE,
    MQTT_QOS_CMD,
    MQTT_QOS_STATUS,
    MQTT_TOPIC_DOWN_FMT,
    MQTT_TOPIC_UP_FMT,
)
from .proto import (
    BatteryStatusRsp,
    DeviceOnlineStatusRsp,
    MSG_TYPE_BATTERY_STATUS,
    MSG_TYPE_DEVICE_ONLINE,
    MSG_TYPE_NET_INFO,
    MSG_TYPE_RTK_INFO,
    MSG_TYPE_RTK_STATUS,
    MSG_TYPE_SENSOR_STATUS,
    MSG_TYPE_TASK_STATUS,
    MSG_TYPE_UPGRADE_STATUS,
    NetInfoRsp,
    RTKInfoRsp,
    SensorStatusRsp,
    TaskStatusRsp,
    UpgradeStatusRsp,
    build_go_dock_req,
    build_pause_task_req,
    build_resume_task_req,
    build_start_task_req,
    build_stop_task_req,
    decode_raw,
    parse_message,
)

_LOGGER = logging.getLogger(__name__)

try:
    import paho.mqtt.client as mqtt
    try:
        import paho.mqtt as _paho_pkg
        _PAHO_VERSION = tuple(int(x) for x in _paho_pkg.__version__.split(".")[:2])
    except Exception:
        _PAHO_VERSION = (1, 0)
    HAS_PAHO = True
    _PAHO_V2 = _PAHO_VERSION >= (2, 0)
    _LOGGER.debug("paho-mqtt version %s (v2 API: %s)", _PAHO_VERSION, _PAHO_V2)
except ImportError:
    HAS_PAHO = False
    _PAHO_V2 = False
    _LOGGER.warning("paho-mqtt not installed; MQTT unavailable")

MessageCallback    = Callable[[str, Any], None]   # (sn, parsed_message)
DisconnectCallback = Callable[[str, int], None]   # (sn, rc)


class AirseekersDeviceMQTT:
    """
    Single-lifetime MQTT connection for one Airseekers device.
    Does NOT reconnect internally. Coordinator handles reconnection with fresh creds.
    """

    def __init__(
        self,
        device_sn: str,
        iot_cert_info: dict[str, Any],
        on_message: MessageCallback,
        on_disconnect: DisconnectCallback,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._sn = device_sn
        self._cert_info = iot_cert_info
        self._on_message = on_message
        self._on_disconnect_cb = on_disconnect
        self._loop = loop or asyncio.get_event_loop()

        self._client: Any = None
        self._connected = False
        self._shutdown = False
        self._temp_cert_dir: str | None = None

        self._topic_up   = MQTT_TOPIC_UP_FMT.format(sn=device_sn)
        self._topic_down = MQTT_TOPIC_DOWN_FMT.format(sn=device_sn)

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    @property
    def connected(self) -> bool:
        return self._connected

    async def async_connect(self) -> bool:
        """Connect once. Returns True on successful TCP+TLS handshake."""
        if not HAS_PAHO:
            _LOGGER.error("paho-mqtt not installed")
            return False
        return await self._loop.run_in_executor(None, self._connect_sync)

    async def async_disconnect(self) -> None:
        """Disconnect and clean up. Safe to call multiple times."""
        self._shutdown = True
        if self._client:
            try:
                self._client.loop_stop()
                if self._connected:
                    self._client.disconnect()
            except Exception:
                pass
            self._client = None
        self._connected = False
        self._cleanup_certs()

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------

    async def async_start_task(self, task_id: str, map_id: str | None = None) -> None:
        await self._publish(build_start_task_req(task_id, map_id))

    async def async_stop_task(self) -> None:
        await self._publish(build_stop_task_req())

    async def async_pause_task(self) -> None:
        await self._publish(build_pause_task_req())

    async def async_resume_task(self) -> None:
        await self._publish(build_resume_task_req())

    async def async_return_to_dock(self) -> None:
        await self._publish(build_go_dock_req())

    # ------------------------------------------------------------------
    # Connection (sync, runs in executor)
    # ------------------------------------------------------------------

    def _connect_sync(self) -> bool:
        cert         = self._cert_info
        broker_raw   = cert.get("mqtt_broker", "")
        client_id    = cert.get("mqtt_client_id", f"ha_{self._sn}")
        ca_cert      = cert.get("ca", "")
        client_cert  = cert.get("cert_key", "") or cert.get("iot_certificate", "")
        private_key  = cert.get("private_key", "")

        if not broker_raw:
            _LOGGER.error("[%s] No mqtt_broker in iot-cert response", self._sn)
            return False

        host, port = self._parse_broker(broker_raw)
        _LOGGER.info(
            "[%s] Connecting AWS IoT Core %s:%s client_id=%s ca=%s cert=%s key=%s",
            self._sn, host, port, client_id,
            bool(ca_cert), bool(client_cert), bool(private_key),
        )

        client = self._make_client(client_id)
        client.on_connect    = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message    = self._on_mqtt_message

        try:
            ssl_ctx = ssl.create_default_context()
            if ca_cert:
                ca_path = self._write_temp("ca.crt", self._ensure_pem(ca_cert))
                if ca_path:
                    ssl_ctx.load_verify_locations(ca_path)
                    _LOGGER.debug("[%s] Loaded Amazon Root CA", self._sn)
            if client_cert and private_key:
                crt_path = self._write_temp("client.crt", self._ensure_pem(client_cert))
                key_path = self._write_temp("client.key", self._ensure_pem(private_key, is_key=True))
                if crt_path and key_path:
                    ssl_ctx.load_cert_chain(crt_path, key_path)
                    _LOGGER.debug("[%s] Loaded mTLS client cert + key", self._sn)
            else:
                _LOGGER.warning("[%s] Missing client cert or private key", self._sn)
            client.tls_set_context(ssl_ctx)
        except Exception as err:
            _LOGGER.error("[%s] TLS setup failed: %s", self._sn, err)
            return False

        try:
            client.connect(host, port, keepalive=MQTT_KEEPALIVE)
        except Exception as err:
            _LOGGER.error("[%s] connect() failed: %s", self._sn, err)
            return False

        self._client = client
        client.loop_start()
        return True

    def _make_client(self, client_id: str) -> Any:
        if _PAHO_V2:
            return mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION1,
                client_id=client_id,
                clean_session=True,
                protocol=mqtt.MQTTv311,
                reconnect_on_failure=False,  # coordinator handles reconnect
            )
        return mqtt.Client(
            client_id=client_id,
            clean_session=True,
            protocol=mqtt.MQTTv311,
        )

    # ------------------------------------------------------------------
    # Paho callbacks
    # ------------------------------------------------------------------

    def _on_connect(self, client: Any, userdata: Any, flags: Any, rc: int) -> None:
        if rc == 0:
            _LOGGER.info("[%s] MQTT connected (client_id=%s)",
                         self._sn, self._cert_info.get("mqtt_client_id", ""))
            self._connected = True
            client.subscribe(self._topic_up, qos=MQTT_QOS_STATUS)
            _LOGGER.debug("[%s] Subscribed to %s", self._sn, self._topic_up)
        else:
            _LOGGER.error("[%s] Connect refused rc=%s: %s", self._sn, rc, _rc_description(rc))
            self._connected = False
            # Notify coordinator so it can try fresh credentials
            self._loop.call_soon_threadsafe(self._on_disconnect_cb, self._sn, rc)

    def _on_disconnect(self, client: Any, userdata: Any, rc: int) -> None:
        if self._shutdown:
            return
        prev = self._connected
        self._connected = False
        if rc != 0:
            _LOGGER.warning("[%s] Disconnected rc=%s: %s", self._sn, rc, _rc_description(rc))
            if prev:
                # Notify coordinator to reconnect with fresh credentials
                self._loop.call_soon_threadsafe(self._on_disconnect_cb, self._sn, rc)
        else:
            _LOGGER.debug("[%s] Disconnected cleanly", self._sn)

    def _on_mqtt_message(self, client: Any, userdata: Any, msg: Any) -> None:
        payload_hex = (
            msg.payload.hex() if len(msg.payload) <= 64
            else msg.payload[:64].hex() + "..."
        )
        _LOGGER.debug(
            "[%s] Received %d bytes on %s: %s",
            self._sn, len(msg.payload), msg.topic, payload_hex,
        )
        try:
            parsed = self._parse_payload(msg.payload)
            if parsed is not None:
                self._loop.call_soon_threadsafe(self._on_message, self._sn, parsed)
        except Exception as err:
            _LOGGER.debug("[%s] Parse error: %s", self._sn, err)

    # ------------------------------------------------------------------
    # Publish
    # ------------------------------------------------------------------

    async def _publish(self, payload: bytes) -> None:
        if not self._connected or not self._client:
            _LOGGER.debug("[%s] Cannot publish — not connected", self._sn)
            return
        _LOGGER.debug(
            "[%s] Publishing %d bytes to %s: %s",
            self._sn, len(payload), self._topic_down, payload.hex(),
        )
        await self._loop.run_in_executor(
            None,
            lambda: self._client.publish(self._topic_down, payload, qos=MQTT_QOS_CMD),
        )

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_payload(self, data: bytes) -> Any:
        msg_type, payload = parse_message(data)
        decode_data = payload if isinstance(payload, bytes) and payload else data

        parsers = {
            MSG_TYPE_TASK_STATUS:    TaskStatusRsp.from_bytes,
            MSG_TYPE_BATTERY_STATUS: BatteryStatusRsp.from_bytes,
            MSG_TYPE_SENSOR_STATUS:  SensorStatusRsp.from_bytes,
            MSG_TYPE_RTK_INFO:       RTKInfoRsp.from_bytes,
            MSG_TYPE_RTK_STATUS:     RTKInfoRsp.from_bytes,
            MSG_TYPE_NET_INFO:       NetInfoRsp.from_bytes,
            MSG_TYPE_DEVICE_ONLINE:  DeviceOnlineStatusRsp.from_bytes,
            MSG_TYPE_UPGRADE_STATUS: UpgradeStatusRsp.from_bytes,
        }

        parser = parsers.get(msg_type)
        if parser:
            try:
                return parser(decode_data)
            except Exception as err:
                _LOGGER.debug("[%s] Parser msg_type=%s failed: %s", self._sn, msg_type, err)

        try:
            raw = decode_raw(data)
            if raw:
                return {"_raw_msg_type": msg_type, "_raw_fields": raw}
        except Exception:
            pass
        return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_broker(broker: str) -> tuple[str, int]:
        for scheme in ("ssl://", "tls://", "mqtts://", "mqtt://", "tcp://"):
            if broker.startswith(scheme):
                broker = broker[len(scheme):]
                break
        if ":" in broker:
            parts = broker.rsplit(":", 1)
            try:
                return parts[0], int(parts[1])
            except ValueError:
                pass
        return broker, MQTT_DEFAULT_PORT

    def _write_temp(self, filename: str, content: str) -> str | None:
        if not content:
            return None
        try:
            if self._temp_cert_dir is None:
                self._temp_cert_dir = tempfile.mkdtemp(prefix="airseekers_")
            path = os.path.join(self._temp_cert_dir, filename)
            with open(path, "w") as f:
                f.write(content)
            return path
        except Exception as err:
            _LOGGER.debug("[%s] Failed to write %s: %s", self._sn, filename, err)
            return None

    @staticmethod
    def _ensure_pem(content: str, is_key: bool = False) -> str:
        content = content.strip()
        if content.startswith("-----"):
            return content + "\n"
        label = "RSA PRIVATE KEY" if is_key else "CERTIFICATE"
        chunks = [content[i:i+64] for i in range(0, len(content), 64)]
        return f"-----BEGIN {label}-----\n" + "\n".join(chunks) + f"\n-----END {label}-----\n"

    def _cleanup_certs(self) -> None:
        if self._temp_cert_dir:
            import shutil
            shutil.rmtree(self._temp_cert_dir, ignore_errors=True)
            self._temp_cert_dir = None


def _rc_description(rc: int) -> str:
    return {
        0: "success",
        1: "incorrect protocol version",
        2: "client identifier rejected",
        3: "server unavailable",
        4: "bad username or password",
        5: "not authorised",
        7: "connection lost / session takeover by another client",
        8: "keepalive timeout",
    }.get(rc, f"unknown rc={rc}")
