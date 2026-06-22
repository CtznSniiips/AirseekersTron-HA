"""
Airseekers MQTT client.

Broker: AWS IoT Core (a26yx9tpysif9b-ats.iot.eu-central-1.amazonaws.com:8883)
Auth:   Mutual TLS (mTLS) — no username/password
Certs from /api/web/device/iot-cert response:
  'ca'          → Amazon Root CA cert  (verify server identity)
  'cert_key'    → Client certificate   (prove our identity to AWS IoT)
  'private_key' → Private key          (for the client cert)
  'mqtt_broker' → host:port
  'mqtt_client_id' → MQTT client identifier

AWS IoT Core policy note:
  The IoT policy attached to this certificate restricts which topics
  can be subscribed/published. Only subscribe to as/{sn}/up.
  Publishing to as/{sn}/down is for commands (once we know the format).

Topic structure (confirmed from logs):
  Subscribe: as/{sn}/up   — device → cloud (status, responses)
  Publish:   as/{sn}/down — cloud → device (commands)
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
    MQTT_RECONNECT_DELAY,
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
    HAS_PAHO = True
except ImportError:
    HAS_PAHO = False
    _LOGGER.warning("paho-mqtt not installed; MQTT unavailable")

StatusCallback = Callable[[str, Any], None]


class AirseekersDeviceMQTT:
    """MQTT connection for a single Airseekers device via AWS IoT Core."""

    def __init__(
        self,
        device_sn: str,
        iot_cert_info: dict[str, Any],
        on_message: StatusCallback,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._sn = device_sn
        self._cert_info = iot_cert_info
        self._on_message = on_message
        self._loop = loop or asyncio.get_event_loop()

        self._client: Any = None
        self._connected = False
        self._connect_lock = asyncio.Lock()
        self._reconnect_task: asyncio.Task | None = None
        self._shutdown = False
        self._temp_cert_dir: str | None = None

        self._topic_up   = MQTT_TOPIC_UP_FMT.format(sn=device_sn)
        self._topic_down = MQTT_TOPIC_DOWN_FMT.format(sn=device_sn)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def connected(self) -> bool:
        return self._connected

    async def async_connect(self) -> bool:
        if not HAS_PAHO:
            _LOGGER.error("paho-mqtt not installed")
            return False
        async with self._connect_lock:
            if self._connected or self._client is not None:
                return self._connected
            return await self._loop.run_in_executor(None, self._build_and_connect)

    async def async_disconnect(self) -> None:
        self._shutdown = True
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
        if self._client:
            try:
                self._client.loop_stop()
                if self._connected:
                    self._client.disconnect()
            except Exception:
                pass
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
    # Internal — connection
    # ------------------------------------------------------------------

    def _build_and_connect(self) -> bool:
        """Build paho client with AWS IoT mTLS and connect."""
        cert = self._cert_info

        broker_raw   = cert.get("mqtt_broker", "")
        client_id    = cert.get("mqtt_client_id", f"ha_{self._sn}")
        # AWS IoT cert fields (confirmed from API response + APK analysis):
        ca_cert      = cert.get("ca", "")           # Amazon Root CA → server verification
        client_cert  = cert.get("cert_key", "") or cert.get("iot_certificate", "")  # our client cert
        private_key  = cert.get("private_key", "")  # our private key

        if not broker_raw:
            _LOGGER.error("[%s] No mqtt_broker in iot-cert response", self._sn)
            return False

        host, port = self._parse_broker(broker_raw)
        _LOGGER.info(
            "[%s] Connecting to AWS IoT Core %s:%s client_id=%s "
            "ca=%s client_cert=%s key=%s",
            self._sn, host, port, client_id,
            bool(ca_cert), bool(client_cert), bool(private_key),
        )

        client = mqtt.Client(
            client_id=client_id,
            protocol=mqtt.MQTTv311,
            clean_session=True,
        )
        client.on_connect    = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message    = self._on_mqtt_message

        # AWS IoT Core uses mTLS — no username/password
        try:
            ssl_ctx = ssl.create_default_context()

            # Load Amazon Root CA for server cert verification
            if ca_cert:
                ca_path = self._write_temp("ca.crt", self._ensure_pem(ca_cert))
                if ca_path:
                    ssl_ctx.load_verify_locations(ca_path)
                    _LOGGER.debug("[%s] Loaded Amazon Root CA", self._sn)
                else:
                    _LOGGER.warning("[%s] Failed to write CA cert, using system trust", self._sn)
            else:
                _LOGGER.warning("[%s] No CA cert in response — using system trust store", self._sn)

            # Load client cert + key for mTLS
            if client_cert and private_key:
                cert_path = self._write_temp("client.crt", self._ensure_pem(client_cert))
                key_path  = self._write_temp("client.key", self._ensure_pem(private_key, is_key=True))
                if cert_path and key_path:
                    ssl_ctx.load_cert_chain(cert_path, key_path)
                    _LOGGER.debug("[%s] Loaded mTLS client cert + key", self._sn)
                else:
                    _LOGGER.warning("[%s] Failed to write client cert/key files", self._sn)
            else:
                _LOGGER.warning(
                    "[%s] Missing client cert (%s) or key (%s) — mTLS incomplete",
                    self._sn, bool(client_cert), bool(private_key),
                )

            client.tls_set_context(ssl_ctx)

        except Exception as err:
            _LOGGER.error("[%s] TLS setup failed: %s", self._sn, err)
            return False

        try:
            client.connect(host, port, keepalive=MQTT_KEEPALIVE)
        except Exception as err:
            _LOGGER.error("[%s] MQTT connect() failed: %s", self._sn, err)
            return False

        self._client = client
        client.loop_start()
        return True

    # ------------------------------------------------------------------
    # Paho callbacks
    # ------------------------------------------------------------------

    def _on_connect(self, client: Any, userdata: Any, flags: Any, rc: int) -> None:
        if rc == 0:
            _LOGGER.info("[%s] MQTT connected to AWS IoT Core", self._sn)
            self._connected = True
            # Only subscribe to up (device→cloud). AWS IoT policy denies subscribing to down.
            client.subscribe(self._topic_up, qos=MQTT_QOS_STATUS)
            _LOGGER.debug("[%s] Subscribed to %s", self._sn, self._topic_up)
        else:
            _LOGGER.error(
                "[%s] MQTT connect refused rc=%s: %s",
                self._sn, rc, _rc_description(rc),
            )
            self._connected = False
            self._maybe_schedule_reconnect()

    def _on_disconnect(self, client: Any, userdata: Any, rc: int) -> None:
        if self._shutdown:
            return
        was_connected = self._connected
        self._connected = False
        if rc != 0:
            _LOGGER.warning(
                "[%s] MQTT disconnected rc=%s: %s",
                self._sn, rc, _rc_description(rc),
            )
            if was_connected:
                self._maybe_schedule_reconnect()
        else:
            _LOGGER.debug("[%s] MQTT disconnected cleanly", self._sn)

    def _on_mqtt_message(self, client: Any, userdata: Any, msg: Any) -> None:
        _LOGGER.debug(
            "[%s] Received %d bytes on %s: %s",
            self._sn, len(msg.payload), msg.topic,
            msg.payload.hex() if len(msg.payload) <= 64 else msg.payload[:64].hex() + "...",
        )
        try:
            parsed = self._parse_payload(msg.payload)
            if parsed is not None:
                self._loop.call_soon_threadsafe(self._on_message, self._sn, parsed)
        except Exception as err:
            _LOGGER.debug("[%s] Parse error on %s: %s", self._sn, msg.topic, err)

    # ------------------------------------------------------------------
    # Reconnect
    # ------------------------------------------------------------------

    def _maybe_schedule_reconnect(self) -> None:
        if self._shutdown:
            return

        def _schedule() -> None:
            if self._reconnect_task and not self._reconnect_task.done():
                return
            self._reconnect_task = self._loop.create_task(self._reconnect_loop())

        self._loop.call_soon_threadsafe(_schedule)

    async def _reconnect_loop(self) -> None:
        delay = MQTT_RECONNECT_DELAY
        while not self._connected and not self._shutdown:
            _LOGGER.info("[%s] Reconnecting in %ss...", self._sn, delay)
            await asyncio.sleep(delay)
            if self._shutdown:
                return
            if not self._connected and self._client:
                try:
                    await self._loop.run_in_executor(None, self._client.reconnect)
                except Exception as err:
                    _LOGGER.debug("[%s] reconnect() failed: %s", self._sn, err)
            delay = min(delay * 2, 120)

    # ------------------------------------------------------------------
    # Publish
    # ------------------------------------------------------------------

    async def _publish(self, payload: bytes) -> None:
        if not self._connected or not self._client:
            _LOGGER.debug("[%s] Cannot publish — not connected", self._sn)
            return
        _LOGGER.debug("[%s] Publishing %d bytes to %s: %s",
                      self._sn, len(payload), self._topic_down, payload.hex())
        await self._loop.run_in_executor(
            None,
            lambda: self._client.publish(self._topic_down, payload, qos=MQTT_QOS_CMD),
        )

    # ------------------------------------------------------------------
    # Message parsing
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
                _LOGGER.debug("[%s] Parser for msg_type=%s failed: %s", self._sn, msg_type, err)

        try:
            raw = decode_raw(data)
            if raw:
                return {"_raw_msg_type": msg_type, "_raw_fields": raw}
        except Exception:
            pass
        return None

    # ------------------------------------------------------------------
    # Cert helpers
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
        2: "invalid client identifier",
        3: "server unavailable",
        4: "bad username or password",
        5: "not authorised",
        7: "connection lost / broker closed connection",
        8: "keepalive timeout",
    }.get(rc, f"unknown rc={rc}")
