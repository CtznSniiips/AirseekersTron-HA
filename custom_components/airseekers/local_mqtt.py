"""
Airseekers local MQTT client — connects directly to the mower on LAN.

The Airseekers Tron mower runs embedded Linux with SSH on port 22.
It likely runs a local MQTT broker (mosquitto) accessible on port 1883
which is how internal mower components communicate with each other.

This connector bypasses the AWS IoT Cloud entirely:
  - No session conflict with the cloud backend
  - Lower latency (LAN vs cloud round-trip)
  - Works even if cloud is down
  - Requires the mower to be on the same LAN as HA

Topics (to be confirmed via SSH access to the mower):
  - As/{sn}/up   — mower publishes status
  - As/{sn}/down — mower receives commands
  OR local variants like:
  - mower/status
  - device/{sn}/report
  - $SYS/... (mosquitto system topics)

Usage:
  If local MQTT works, set USE_LOCAL_MQTT = True in const.py and
  provide LOCAL_MQTT_PORT = 1883 and LOCAL_MQTT_HOST = device IP.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any
from collections.abc import Callable

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
except ImportError:
    HAS_PAHO = False
    _PAHO_V2 = False

MessageCallback    = Callable[[str, Any], None]
DisconnectCallback = Callable[[str, int], None]

LOCAL_MQTT_PORT = 1883
LOCAL_CLIENT_ID_FMT = "ha_{sn}_local"

# Topics to try — will subscribe to all and log what arrives
LOCAL_SUBSCRIBE_TOPICS = [
    "as/{sn}/#",        # Wildcard for cloud-style topics
    "{sn}/#",           # SN-prefixed
    "mower/#",          # Generic mower prefix
    "device/#",         # Generic device prefix
    "$SYS/#",           # Mosquitto system topics (reveals broker info)
    "#",                # ALL topics (use carefully)
]


class AirseekersLocalMQTT:
    """
    Direct local MQTT connection to the mower's onboard broker.
    No cloud involvement, no session conflicts.
    """

    def __init__(
        self,
        device_sn: str,
        device_ip: str,
        on_message: MessageCallback,
        on_disconnect: DisconnectCallback,
        loop: asyncio.AbstractEventLoop | None = None,
        port: int = LOCAL_MQTT_PORT,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        self._sn = device_sn
        self._ip = device_ip
        self._port = port
        self._username = username
        self._password = password
        self._on_message = on_message
        self._on_disconnect_cb = on_disconnect
        self._loop = loop or asyncio.get_event_loop()

        self._client: Any = None
        self._connected = False
        self._shutdown = False
        self._connack_event = asyncio.Event()
        self._connack_rc = -1

    @property
    def connected(self) -> bool:
        return self._connected

    async def async_connect(self) -> bool:
        """Connect to local MQTT broker. Returns True on CONNACK rc=0."""
        if not HAS_PAHO:
            _LOGGER.error("paho-mqtt not installed")
            return False

        self._connack_event.clear()
        ok = await self._loop.run_in_executor(None, self._connect_sync)
        if not ok:
            return False

        try:
            await asyncio.wait_for(self._connack_event.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            _LOGGER.warning("[%s] Local MQTT CONNACK timeout", self._sn)
            await self.async_disconnect()
            return False

        return self._connack_rc == 0

    async def async_disconnect(self) -> None:
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

    def _connect_sync(self) -> bool:
        client_id = LOCAL_CLIENT_ID_FMT.format(sn=self._sn)
        _LOGGER.info(
            "[%s] Connecting local MQTT %s:%s client_id=%s",
            self._sn, self._ip, self._port, client_id,
        )

        kwargs = dict(client_id=client_id, clean_session=True, protocol=mqtt.MQTTv311)
        if _PAHO_V2:
            client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION1,
                reconnect_on_failure=False,
                **kwargs,
            )
        else:
            client = mqtt.Client(**kwargs)

        if self._username:
            client.username_pw_set(self._username, self._password)

        client.on_connect    = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message    = self._on_mqtt_message

        try:
            client.connect(self._ip, self._port, keepalive=60)
        except Exception as err:
            _LOGGER.error("[%s] Local MQTT connect() failed: %s", self._sn, err)
            return False

        self._client = client
        client.loop_start()
        return True

    def _on_connect(self, client: Any, userdata: Any, flags: Any, rc: int) -> None:
        self._connack_rc = rc
        self._loop.call_soon_threadsafe(self._connack_event.set)

        if rc == 0:
            _LOGGER.info("[%s] Connected to local MQTT broker at %s", self._sn, self._ip)
            self._connected = True
            # Subscribe to all discovery topics
            for topic_fmt in LOCAL_SUBSCRIBE_TOPICS:
                topic = topic_fmt.format(sn=self._sn)
                client.subscribe(topic, qos=0)
                _LOGGER.debug("[%s] Subscribed local: %s", self._sn, topic)
        else:
            _LOGGER.warning("[%s] Local MQTT refused rc=%s", self._sn, rc)
            self._connected = False

    def _on_disconnect(self, client: Any, userdata: Any, rc: int) -> None:
        if self._shutdown:
            return
        was = self._connected
        self._connected = False
        if rc != 0:
            _LOGGER.warning("[%s] Local MQTT disconnected rc=%s", self._sn, rc)
            if was:
                self._loop.call_soon_threadsafe(self._on_disconnect_cb, self._sn, rc)

    def _on_mqtt_message(self, client: Any, userdata: Any, msg: Any) -> None:
        """Log ALL received messages — used for topic discovery."""
        payload_hex = msg.payload.hex() if len(msg.payload) <= 128 else msg.payload[:128].hex() + "..."
        payload_text = ""
        try:
            payload_text = msg.payload.decode("utf-8")[:200]
        except Exception:
            pass

        _LOGGER.info(
            "[%s] LOCAL MQTT topic=%s qos=%s retain=%s len=%d hex=%s text=%r",
            self._sn, msg.topic, msg.qos, msg.retain,
            len(msg.payload), payload_hex, payload_text,
        )
        # Deliver to coordinator for processing
        self._loop.call_soon_threadsafe(
            self._on_message, self._sn,
            {"_local_topic": msg.topic, "_payload_hex": payload_hex,
             "_payload_text": payload_text, "_payload_bytes": msg.payload},
        )

    async def async_publish(self, topic: str, payload: bytes, qos: int = 1) -> None:
        if not self._connected or not self._client:
            _LOGGER.debug("[%s] Cannot publish local — not connected", self._sn)
            return
        _LOGGER.debug("[%s] Local publish %s: %s", self._sn, topic, payload.hex())
        await self._loop.run_in_executor(
            None,
            lambda: self._client.publish(topic, payload, qos=qos),
        )
