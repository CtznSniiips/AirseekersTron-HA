"""
Airseekers MQTT client.

Connects to the eiotclub MQTT broker using credentials fetched from the REST API.

Auth model (confirmed from libapp.so static analysis):
  - Broker URL comes from iot-cert response: mqtt_broker field (host only, no scheme)
  - Port comes from mqtt_broker field or defaults (1883 plain / 8883 TLS)
  - Client ID: mqtt_client_id field
  - Username: mqtt_client_id (same as client ID — eiotclub convention)
  - Password: iot_cert_token field
  - TLS: server-side only (iot_certificate = CA/server cert for verification)
  - The private_key / cert_key fields appear to be for client-side TLS mutual auth;
    try token-only auth first, fall back to mTLS if broker rejects.

Topic structure (best-guess — confirm with traffic capture):
  Subscribe: as/{sn}/up
  Publish:   as/{sn}/down
"""
from __future__ import annotations

import asyncio
import logging
import os
import ssl
import tempfile
import threading
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
    build_get_network_req,
    build_get_rtk_req,
    build_get_status_req,
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

StatusCallback = Callable[[str, Any], None]  # (device_sn, parsed_message)


class AirseekersDeviceMQTT:
    """
    MQTT connection for a single Airseekers device.

    Usage:
        client = AirseekersDeviceMQTT(sn, cert_info, on_status)
        await client.async_connect()
        await client.async_start_task(task_id)
        await client.async_disconnect()
    """

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

        # Single paho client — never replaced after creation
        self._client: Any = None
        self._connected = False
        self._connect_lock = asyncio.Lock()
        self._reconnect_task: asyncio.Task | None = None
        self._shutdown = False
        self._temp_cert_dir: str | None = None

        # Topics
        self._topic_up   = MQTT_TOPIC_UP_FMT.format(sn=device_sn)
        self._topic_down = MQTT_TOPIC_DOWN_FMT.format(sn=device_sn)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def connected(self) -> bool:
        return self._connected

    async def async_connect(self) -> bool:
        """Connect to MQTT broker. Returns True on success."""
        if not HAS_PAHO:
            _LOGGER.error("paho-mqtt not installed; cannot connect to MQTT")
            return False

        async with self._connect_lock:
            if self._connected:
                return True
            if self._client is not None:
                # Already have a client — just ensure loop is running
                return self._connected

            return await self._loop.run_in_executor(None, self._build_and_connect)

    async def async_disconnect(self) -> None:
        """Disconnect cleanly and cancel reconnect."""
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
    # Commands — publish protobuf messages to device
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

    async def async_poll_status(self) -> None:
        await self._publish(build_get_status_req())

    async def async_poll_network(self) -> None:
        await self._publish(build_get_network_req())

    async def async_poll_rtk(self) -> None:
        await self._publish(build_get_rtk_req())

    # ------------------------------------------------------------------
    # Internal — connection build (runs in executor, called once)
    # ------------------------------------------------------------------

    def _build_and_connect(self) -> bool:
        """Build paho client and connect. Runs in executor thread."""
        cert = self._cert_info
        broker_raw = cert.get("mqtt_broker", "")
        client_id  = cert.get("mqtt_client_id", f"ha_{self._sn}")
        token      = cert.get("iot_cert_token", "")
        ca_cert    = cert.get("iot_certificate") or cert.get("cert_key")
        priv_key   = cert.get("private_key")

        if not broker_raw:
            _LOGGER.error("[%s] No mqtt_broker in iot-cert response", self._sn)
            return False

        # Parse host and port from broker string
        # Possible formats: "host", "host:port", "ssl://host:port"
        host, port = self._parse_broker(broker_raw)
        use_tls = port == 8883 or "ssl" in broker_raw or "tls" in broker_raw

        _LOGGER.info(
            "[%s] Connecting to MQTT broker raw=%r host=%s port=%s tls=%s client_id=%s token_set=%s",
            self._sn, broker_raw, host, port, use_tls, client_id, bool(token),
        )

        client = mqtt.Client(
            client_id=client_id,
            protocol=mqtt.MQTTv311,
            clean_session=True,
        )
        client.on_connect    = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message    = self._on_mqtt_message

        # Auth: username = client_id, password = iot_cert_token
        # (eiotclub token-based auth — confirmed by _clientToken field in ASMqttClient)
        if token:
            client.username_pw_set(username=client_id, password=token)
            _LOGGER.debug("[%s] Using token auth (username=%s)", self._sn, client_id)

        # TLS: server cert verification using iot_certificate as CA
        if use_tls:
            try:
                ssl_ctx = ssl.create_default_context()
                if ca_cert:
                    ca_path = self._write_temp_cert(ca_cert, "ca.crt")
                    if ca_path:
                        ssl_ctx.load_verify_locations(ca_path)
                        _LOGGER.debug("[%s] Loaded CA cert for server verification", self._sn)
                    else:
                        # CA cert write failed — use system CAs with relaxed verification
                        ssl_ctx.check_hostname = False
                        ssl_ctx.verify_mode = ssl.CERT_NONE
                        _LOGGER.warning(
                            "[%s] Could not write CA cert, disabling cert verification", self._sn
                        )
                else:
                    # No CA cert provided — trust system CAs
                    _LOGGER.debug("[%s] No CA cert, using system trust store", self._sn)

                # Client cert auth (mTLS) — only if both cert and key are provided
                # and cert looks like a PEM cert (not a token string)
                if priv_key and ca_cert and "BEGIN" in str(ca_cert):
                    cert_path = self._write_temp_cert(ca_cert, "client.crt")
                    key_path  = self._write_temp_cert(priv_key, "client.key", is_key=True)
                    if cert_path and key_path:
                        try:
                            ssl_ctx.load_cert_chain(cert_path, key_path)
                            _LOGGER.debug("[%s] Loaded mTLS client cert", self._sn)
                        except Exception as e:
                            _LOGGER.debug("[%s] mTLS cert load failed: %s", self._sn, e)

                client.tls_set_context(ssl_ctx)
            except Exception as err:
                _LOGGER.warning("[%s] TLS setup failed: %s — trying plain TCP", self._sn, err)
                use_tls = False
                port = 1883

        try:
            client.connect(host, port, keepalive=MQTT_KEEPALIVE)
        except Exception as err:
            _LOGGER.error("[%s] MQTT connect() failed: %s", self._sn, err)
            return False

        self._client = client
        # loop_start() starts a single background thread — only called once
        client.loop_start()
        return True

    # ------------------------------------------------------------------
    # Internal — paho callbacks (called on paho thread)
    # ------------------------------------------------------------------

    def _on_connect(self, client: Any, userdata: Any, flags: Any, rc: int) -> None:
        if rc == 0:
            _LOGGER.info("[%s] MQTT connected", self._sn)
            self._connected = True
            # Subscribe to both topic directions so we can sniff which one carries data.
            # Device → cloud: as/{sn}/up  (we expect status updates here)
            # Cloud → device: as/{sn}/down (commands go here, but also subscribe to confirm)
            client.subscribe(self._topic_up, qos=MQTT_QOS_STATUS)
            _LOGGER.debug("[%s] Subscribed to %s", self._sn, self._topic_up)
            client.subscribe(self._topic_down, qos=MQTT_QOS_STATUS)
            _LOGGER.debug("[%s] Subscribed to %s (diagnostic)", self._sn, self._topic_down)
            # NOTE: Do NOT publish anything here.
            # Sending an unverified protobuf payload causes the broker to
            # drop the connection (RC=7) within ~90ms. Status updates arrive
            # from the device automatically once subscribed.
        else:
            _LOGGER.error(
                "[%s] MQTT connection refused (rc=%s): %s",
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
                "[%s] MQTT disconnected (rc=%s): %s",
                self._sn, rc, _rc_description(rc),
            )
            if was_connected:
                # Only schedule reconnect once per disconnect event
                self._maybe_schedule_reconnect()
        else:
            _LOGGER.debug("[%s] MQTT disconnected cleanly", self._sn)

    def _on_mqtt_message(self, client: Any, userdata: Any, msg: Any) -> None:
        _LOGGER.debug(
            "[%s] Received %d bytes on topic %s qos=%s",
            self._sn, len(msg.payload), msg.topic, msg.qos,
        )
        try:
            parsed = self._parse_payload(msg.payload)
            if parsed is not None:
                # Deliver to coordinator on the HA event loop thread
                self._loop.call_soon_threadsafe(self._on_message, self._sn, parsed)
            else:
                _LOGGER.debug("[%s] Payload could not be parsed: %s", self._sn, msg.payload.hex())
        except Exception as err:
            _LOGGER.debug("[%s] Failed to parse MQTT message on %s: %s", self._sn, msg.topic, err)

    # ------------------------------------------------------------------
    # Internal — reconnect (single task, guarded)
    # ------------------------------------------------------------------

    def _maybe_schedule_reconnect(self) -> None:
        """Schedule reconnect only if none is already pending. Safe to call from paho thread."""
        if self._shutdown:
            return

        def _schedule() -> None:
            # Runs on the HA event loop thread — guarded against duplicate tasks
            if self._reconnect_task and not self._reconnect_task.done():
                return
            self._reconnect_task = self._loop.create_task(self._reconnect_loop())

        self._loop.call_soon_threadsafe(_schedule)

    async def _reconnect_loop(self) -> None:
        """Exponential backoff reconnect. Reuses the same paho client."""
        if self._shutdown:
            return
        delay = MQTT_RECONNECT_DELAY
        while not self._connected and not self._shutdown:
            _LOGGER.info("[%s] Reconnecting in %ss...", self._sn, delay)
            await asyncio.sleep(delay)
            if self._shutdown:
                return
            if not self._connected and self._client:
                try:
                    await self._loop.run_in_executor(
                        None, lambda: self._client.reconnect()
                    )
                except Exception as err:
                    _LOGGER.debug("[%s] reconnect() failed: %s", self._sn, err)
            delay = min(delay * 2, 120)

    # ------------------------------------------------------------------
    # Internal — publish
    # ------------------------------------------------------------------

    async def _publish(self, payload: bytes) -> None:
        if not self._connected or not self._client:
            _LOGGER.debug("[%s] Cannot publish — not connected", self._sn)
            return
        _LOGGER.debug("[%s] Publishing %d bytes to %s", self._sn, len(payload), self._topic_down)
        await self._loop.run_in_executor(
            None,
            lambda: self._client.publish(self._topic_down, payload, qos=MQTT_QOS_CMD),
        )

    # ------------------------------------------------------------------
    # Internal — message parsing
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

        # Unknown — return raw fields for debugging / calibration
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
        """Parse 'host', 'host:port', or 'ssl://host:port' → (host, port)."""
        # Strip scheme if present
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

    def _write_temp_cert(self, content: str, filename: str, is_key: bool = False) -> str | None:
        """Write a PEM string to a temp file. Returns path or None on failure."""
        if not content:
            return None
        try:
            if self._temp_cert_dir is None:
                self._temp_cert_dir = tempfile.mkdtemp(prefix="airseekers_")
            path = os.path.join(self._temp_cert_dir, filename)
            pem = self._ensure_pem(content, is_key)
            with open(path, "w") as f:
                f.write(pem)
            return path
        except Exception as err:
            _LOGGER.debug("[%s] Failed to write temp cert %s: %s", self._sn, filename, err)
            return None

    @staticmethod
    def _ensure_pem(content: str, is_key: bool = False) -> str:
        """Wrap bare base64 in PEM headers if needed."""
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


# ------------------------------------------------------------------
# RC code descriptions for better log messages
# ------------------------------------------------------------------

def _rc_description(rc: int) -> str:
    return {
        0: "success",
        1: "incorrect protocol version",
        2: "invalid client identifier",
        3: "server unavailable",
        4: "bad username or password",
        5: "not authorised",
        6: "reserved",
        7: "connection lost / broker closed connection",
        8: "keepalive timeout",
    }.get(rc, f"unknown rc={rc}")
