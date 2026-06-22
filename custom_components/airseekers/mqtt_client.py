"""
Airseekers MQTT client.

Connects to the eiotclub MQTT broker using per-device TLS client certificates
fetched from the REST API. Publishes/subscribes to device topics carrying
binary Protobuf payloads.

Topic structure (best-guess from static analysis — verify with traffic capture):
  Subscribe (device → cloud):  as/{sn}/up
  Publish   (cloud → device):  as/{sn}/down

The app calls publishGetOnlineStatus, publishGetTaskPath, etc. each pointing
to its own method that wraps a protobuf message and publishes it.
"""
from __future__ import annotations

import asyncio
import logging
import os
import ssl
import tempfile
import time
from collections.abc import Callable
from typing import Any

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
        self._client: Any = None  # paho Client
        self._connected = False
        self._connecting = False
        self._reconnect_task: asyncio.Task | None = None
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

        if self._connected or self._connecting:
            return self._connected

        self._connecting = True
        try:
            return await self._loop.run_in_executor(None, self._connect_sync)
        finally:
            self._connecting = False

    async def async_disconnect(self) -> None:
        """Disconnect cleanly."""
        if self._reconnect_task:
            self._reconnect_task.cancel()
        if self._client and self._connected:
            await self._loop.run_in_executor(None, self._client.disconnect)
        self._connected = False
        self._cleanup_certs()

    # ------------------------------------------------------------------
    # Commands — publish protobuf messages to device
    # ------------------------------------------------------------------

    async def async_start_task(self, task_id: str, map_id: str | None = None) -> None:
        """Start a mowing task."""
        await self._publish(build_start_task_req(task_id, map_id))

    async def async_stop_task(self) -> None:
        """Stop current task."""
        await self._publish(build_stop_task_req())

    async def async_pause_task(self) -> None:
        """Pause current task."""
        await self._publish(build_pause_task_req())

    async def async_resume_task(self) -> None:
        """Resume paused task."""
        await self._publish(build_resume_task_req())

    async def async_return_to_dock(self) -> None:
        """Send mower to charging dock."""
        await self._publish(build_go_dock_req())

    async def async_poll_status(self) -> None:
        """Request a status update from the device."""
        await self._publish(build_get_status_req())

    async def async_poll_network(self) -> None:
        """Request network info from the device."""
        await self._publish(build_get_network_req())

    async def async_poll_rtk(self) -> None:
        """Request RTK data from the device."""
        await self._publish(build_get_rtk_req())

    # ------------------------------------------------------------------
    # Internal — connection
    # ------------------------------------------------------------------

    def _connect_sync(self) -> bool:
        """Blocking connect; run in executor."""
        import paho.mqtt.client as mqtt

        broker = self._cert_info.get("mqtt_broker", "")
        client_id = self._cert_info.get("mqtt_client_id", f"ha_{self._sn}")
        certificate = self._cert_info.get("iot_certificate") or self._cert_info.get("cert_key")
        private_key = self._cert_info.get("private_key")
        cert_token = self._cert_info.get("iot_cert_token")

        if not broker:
            _LOGGER.error("[%s] No MQTT broker in iot-cert response", self._sn)
            return False

        # Parse host:port from broker string
        port = MQTT_DEFAULT_PORT
        host = broker
        if ":" in broker:
            parts = broker.rsplit(":", 1)
            host = parts[0].lstrip("mqtt://").lstrip("mqtts://")
            try:
                port = int(parts[1])
            except ValueError:
                pass

        _LOGGER.info("[%s] Connecting to MQTT broker %s:%s", self._sn, host, port)

        client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)
        client.on_connect    = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message    = self._on_mqtt_message

        # TLS client certificate auth
        tls_configured = False
        if certificate and private_key:
            try:
                self._temp_cert_dir = tempfile.mkdtemp(prefix="airseekers_")
                cert_path = os.path.join(self._temp_cert_dir, "client.crt")
                key_path  = os.path.join(self._temp_cert_dir, "client.key")
                with open(cert_path, "w") as f:
                    f.write(certificate if "\n" in certificate else self._pem_format(certificate))
                with open(key_path, "w") as f:
                    f.write(private_key if "\n" in private_key else self._pem_format(private_key, key=True))
                client.tls_set(
                    certfile=cert_path,
                    keyfile=key_path,
                    tls_version=ssl.PROTOCOL_TLS,
                )
                client.tls_insecure_set(False)
                tls_configured = True
                _LOGGER.debug("[%s] TLS client cert configured", self._sn)
            except Exception as err:
                _LOGGER.warning("[%s] Could not configure TLS cert: %s", self._sn, err)

        if not tls_configured:
            # Fallback: token-based auth (if eiotclub uses username/password)
            if cert_token:
                client.username_pw_set(self._sn, cert_token)
            try:
                client.tls_set(tls_version=ssl.PROTOCOL_TLS)
                client.tls_insecure_set(True)  # for testing; remove in production
            except Exception:
                pass

        try:
            client.connect(host, port, keepalive=MQTT_KEEPALIVE)
        except Exception as err:
            _LOGGER.error("[%s] MQTT connect failed: %s", self._sn, err)
            return False

        self._client = client
        client.loop_start()
        return True

    def _on_connect(self, client: Any, userdata: Any, flags: Any, rc: int) -> None:
        """Paho connect callback."""
        if rc == 0:
            _LOGGER.info("[%s] MQTT connected", self._sn)
            self._connected = True
            # Subscribe to device status topic
            client.subscribe(self._topic_up, qos=MQTT_QOS_STATUS)
            _LOGGER.debug("[%s] Subscribed to %s", self._sn, self._topic_up)
            # Request initial status
            self._loop.call_soon_threadsafe(
                self._loop.create_task,
                self.async_poll_status()
            )
        else:
            _LOGGER.error("[%s] MQTT connect failed with rc=%s", self._sn, rc)
            self._connected = False
            self._schedule_reconnect()

    def _on_disconnect(self, client: Any, userdata: Any, rc: int) -> None:
        """Paho disconnect callback."""
        _LOGGER.warning("[%s] MQTT disconnected (rc=%s)", self._sn, rc)
        self._connected = False
        if rc != 0:
            self._schedule_reconnect()

    def _on_mqtt_message(self, client: Any, userdata: Any, msg: Any) -> None:
        """Paho message callback — parse protobuf and dispatch."""
        try:
            parsed = self._parse_payload(msg.payload)
            if parsed is not None:
                self._loop.call_soon_threadsafe(
                    self._on_message, self._sn, parsed
                )
        except Exception as err:
            _LOGGER.debug("[%s] Failed to parse MQTT message: %s", self._sn, err)

    def _schedule_reconnect(self) -> None:
        """Schedule a reconnect attempt."""
        if self._reconnect_task and not self._reconnect_task.done():
            return
        self._reconnect_task = self._loop.create_task(self._reconnect_loop())

    async def _reconnect_loop(self) -> None:
        """Keep trying to reconnect with backoff."""
        delay = MQTT_RECONNECT_DELAY
        while not self._connected:
            _LOGGER.info("[%s] Reconnecting in %ss...", self._sn, delay)
            await asyncio.sleep(delay)
            connected = await self.async_connect()
            if connected:
                break
            delay = min(delay * 2, 120)

    # ------------------------------------------------------------------
    # Internal — publish
    # ------------------------------------------------------------------

    async def _publish(self, payload: bytes) -> None:
        """Publish a command to the device's down topic."""
        if not self._connected or not self._client:
            _LOGGER.warning("[%s] Cannot publish — not connected", self._sn)
            return
        topic = self._topic_down
        _LOGGER.debug("[%s] Publishing %d bytes to %s", self._sn, len(payload), topic)
        await self._loop.run_in_executor(
            None,
            lambda: self._client.publish(topic, payload, qos=MQTT_QOS_CMD),
        )

    # ------------------------------------------------------------------
    # Internal — message parsing
    # ------------------------------------------------------------------

    def _parse_payload(self, data: bytes) -> Any:
        """Parse incoming protobuf payload into a typed object."""
        msg_type, payload = parse_message(data)
        # If msg_type is 0, the outer envelope parse failed;
        # we still try common types on the raw data so captures work
        # even before field numbers are confirmed.
        decode_data = payload if isinstance(payload, bytes) and payload else data

        parsers = {
            MSG_TYPE_TASK_STATUS:   TaskStatusRsp.from_bytes,
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
                _LOGGER.debug("[%s] Parser %s failed: %s", self._sn, msg_type, err)

        # Unknown type — return raw dict so coordinator can log it
        from .proto import decode_raw
        try:
            return {"_raw_msg_type": msg_type, "_raw_fields": decode_raw(data)}
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Cert helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _pem_format(b64: str, key: bool = False) -> str:
        """Wrap a bare base64 string in PEM headers."""
        label = "PRIVATE KEY" if key else "CERTIFICATE"
        # Insert line breaks every 64 chars
        chunks = [b64[i:i+64] for i in range(0, len(b64), 64)]
        return f"-----BEGIN {label}-----\n" + "\n".join(chunks) + f"\n-----END {label}-----\n"

    def _cleanup_certs(self) -> None:
        """Remove temporary cert files."""
        if self._temp_cert_dir:
            import shutil
            try:
                shutil.rmtree(self._temp_cert_dir, ignore_errors=True)
            except Exception:
                pass
            self._temp_cert_dir = None
