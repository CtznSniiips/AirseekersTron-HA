"""Airseekers data coordinator — manages REST polling + MQTT push."""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import timedelta
from typing import Any

from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import AirseekersAPI, AirseekersAPIError, AirseekersAuthError
from .const import (
    DOMAIN,
    POLL_INTERVAL_SLOW,
)
from .mqtt_client import AirseekersDeviceMQTT
from .proto import (
    BatteryStatusRsp,
    DeviceOnlineStatusRsp,
    NetInfoRsp,
    RTKInfoRsp,
    SensorStatusRsp,
    TaskStatusRsp,
    UpgradeStatusRsp,
)

_LOGGER = logging.getLogger(__name__)


class AirseekersDeviceData:
    """All known state for one Airseekers device."""

    def __init__(self, device_info: dict[str, Any]) -> None:
        self.device_info = device_info
        # API returns "sn" key (not "device_sn") per static analysis of libapp.so
        self.sn: str = device_info.get("sn") or device_info.get("device_sn") or device_info.get("deviceSn", "")

        # REST-sourced
        self.maps: list[dict] = []
        self.tasks: list[dict] = []
        self.firmware_info: dict = {}
        self.latest_task_record: dict = {}

        # MQTT-sourced (real-time)
        self.task_status: TaskStatusRsp | None = None
        self.battery: BatteryStatusRsp | None = None
        self.sensors: SensorStatusRsp | None = None
        self.rtk: RTKInfoRsp | None = None
        self.network: NetInfoRsp | None = None
        self.upgrade: UpgradeStatusRsp | None = None
        self.online: bool | None = None

        # MQTT connection
        self.mqtt: AirseekersDeviceMQTT | None = None

    # --- Convenience properties ---

    @property
    def state_str(self) -> str:
        if self.task_status:
            return self.task_status.state_str
        if self.online is False:
            return "offline"
        return "unknown"

    @property
    def battery_percentage(self) -> int | None:
        return self.battery.percentage if self.battery else None

    @property
    def battery_temperature(self) -> float | None:
        return self.battery.temperature if self.battery else None

    @property
    def is_online(self) -> bool | None:
        return self.online

    @property
    def rtk_state(self) -> str | None:
        return self.rtk.state_str if self.rtk else None

    @property
    def rtk_satellites(self) -> int | None:
        return self.rtk.satellites if self.rtk else None

    @property
    def rtk_has_fix(self) -> bool | None:
        return self.rtk.has_fix if self.rtk else None

    @property
    def rain_detected(self) -> bool | None:
        return self.sensors.rain_detected if self.sensors else None

    @property
    def lift_triggered(self) -> bool | None:
        return self.sensors.lift_triggered if self.sensors else None

    @property
    def tilt_triggered(self) -> bool | None:
        return self.sensors.tilt_triggered if self.sensors else None

    @property
    def is_mowing(self) -> bool:
        from .const import TASK_STATE_MOWING
        return self.task_status is not None and self.task_status.state == TASK_STATE_MOWING

    @property
    def error_code(self) -> int | None:
        return self.task_status.error_code if self.task_status else None

    @property
    def ota_in_progress(self) -> bool:
        return self.upgrade is not None and self.upgrade.state != "OTA_IDLE"

    @property
    def ota_progress(self) -> int:
        return self.upgrade.progress if self.upgrade else 0

    @property
    def firmware_version(self) -> str:
        return self.device_info.get("firmware_ver", "")

    @property
    def model(self) -> str:
        return self.device_info.get("model", "Airseekers Tron")


class AirseekersCoordinator(DataUpdateCoordinator):
    """
    Coordinator for one Airseekers device.

    REST polls every POLL_INTERVAL_SLOW seconds.
    MQTT push updates arrive via _on_mqtt_message callback.

    Key design: fresh IoT credentials are fetched on every MQTT reconnect.
    The /api/web/device/iot-cert endpoint issues a new unique mqtt_client_id
    each call, preventing session-takeover conflicts with the mobile app.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        api: AirseekersAPI,
        device_info: dict[str, Any],
    ) -> None:
        self.api = api
        self.device = AirseekersDeviceData(device_info)
        self._mqtt_reconnect_task: asyncio.Task | None = None
        self._takeover_times: list[float] = []  # timestamps of recent rc=7 disconnects

        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}_{self.device.sn}",
            update_interval=timedelta(seconds=POLL_INTERVAL_SLOW),
        )

    async def _async_update_data(self) -> AirseekersDeviceData:
        """Fetch REST data. Called on schedule and on first refresh."""
        sn = self.device.sn
        try:
            # Fetch device list separately so we can log the FULL response
            devices = await self.api.async_get_devices()
            if devices:
                device_info = next(
                    (d for d in devices if d.get("sn") == sn or d.get("device_sn") == sn),
                    None,
                )
                if device_info:
                    self.device.device_info = device_info
                    # Log ALL fields — helps discover undocumented status fields
                    _LOGGER.debug(
                        "[%s] Full device REST response: %s",
                        sn, device_info,
                    )
                    # Update online status from REST
                    online = device_info.get("online_status", 0) == 1
                    if self.device.online != online:
                        self.device.online = online
                        _LOGGER.info("[%s] Online status from REST: %s", sn, online)

            maps_task   = asyncio.create_task(self.api.async_get_maps(sn))
            tasks_task  = asyncio.create_task(self.api.async_get_tasks(sn))
            fw_task     = asyncio.create_task(self.api.async_get_firmware_info(sn))
            record_task = asyncio.create_task(self.api.async_get_latest_task_record(sn))
            notify_task = asyncio.create_task(self.api.async_get_notifications(sn))

            results = await asyncio.gather(
                maps_task, tasks_task, fw_task, record_task, notify_task,
                return_exceptions=True,
            )

            if isinstance(results[0], list):
                self.device.maps = results[0]
            if isinstance(results[1], list):
                self.device.tasks = results[1]
                if results[1]:
                    _LOGGER.debug("[%s] Tasks: %s", sn, results[1])
            if isinstance(results[2], dict):
                self.device.firmware_info = results[2]
            if isinstance(results[3], dict):
                record = results[3]
                self.device.latest_task_record = record
                if record.get("task_id"):
                    _LOGGER.debug("[%s] Latest task record: %s", sn, record)
            if isinstance(results[4], list) and results[4]:
                _LOGGER.debug("[%s] Notifications: %s", sn, results[4][:3])

            for result, name in zip(results, ["maps", "tasks", "firmware", "task_record", "notify"]):
                if isinstance(result, Exception):
                    msg = str(result)
                    if "407" not in msg:
                        _LOGGER.warning("[%s] REST %s fetch failed: %s", sn, name, result)

        except (AirseekersAuthError, AirseekersAPIError) as err:
            raise UpdateFailed(f"Airseekers API error for {sn}: {err}") from err

        # Set up MQTT on first successful REST update (or if not connected)
        if not self.device.mqtt or not self.device.mqtt.connected:
            await self._async_connect_mqtt()

        return self.device

    # ------------------------------------------------------------------
    # MQTT — always fetch fresh credentials to avoid client_id conflicts
    # ------------------------------------------------------------------

    async def _async_connect_mqtt(self) -> None:
        """
        Fetch fresh IoT credentials and open a new MQTT connection.

        Each call to /api/web/device/iot-cert returns a unique mqtt_client_id,
        so there's no session-takeover conflict with the mobile app.
        We tear down any existing connection first.
        """
        sn = self.device.sn

        # Clean up old connection
        if self.device.mqtt:
            await self.device.mqtt.async_disconnect()
            self.device.mqtt = None

        try:
            cert_info = await self.api.async_get_iot_cert(sn)
            # Log all keys so we can see exactly what the iot-cert response contains
            _LOGGER.debug(
                "[%s] Got IoT cert: broker=%s client_id=%s "
                "keys=%s has_token=%s",
                sn,
                cert_info.get("mqtt_broker"),
                cert_info.get("mqtt_client_id"),
                sorted(cert_info.keys()),
                bool(cert_info.get("iot_cert_token")),
            )
        except Exception as err:
            _LOGGER.warning("[%s] Could not fetch IoT cert: %s — MQTT unavailable", sn, err)
            return

        mqtt_client = AirseekersDeviceMQTT(
            device_sn=sn,
            iot_cert_info=cert_info,
            on_message=self._on_mqtt_message,
            on_disconnect=self._on_mqtt_disconnect,
            loop=asyncio.get_event_loop(),
        )
        connected = await mqtt_client.async_connect()
        if connected:
            self.device.mqtt = mqtt_client
            _LOGGER.info("[%s] MQTT connected", sn)
        else:
            _LOGGER.warning("[%s] MQTT connection failed", sn)

    def _on_mqtt_disconnect(self, sn: str, rc: int) -> None:
        """
        Called when MQTT disconnects.

        Now that we connect as the app (WebSocket+JWT on port 443) rather than
        as the mower (mTLS on port 8883), we should no longer get RC=7 session
        takeovers from the mower's keepalive cycle. Any disconnect is unexpected
        and warrants a reconnect with backoff.
        """
        if rc == 0:
            _LOGGER.debug("[%s] MQTT disconnected cleanly", sn)
            return

        now = time.monotonic()
        self._takeover_times = [t for t in self._takeover_times if now - t < 60]
        self._takeover_times.append(now)
        rapid = len(self._takeover_times) >= 3

        if rapid:
            initial_delay = 60
            _LOGGER.warning(
                "[%s] Rapid MQTT disconnects (rc=%s, %sx in 60s) — backing off %ss",
                sn, rc, len(self._takeover_times), initial_delay,
            )
        else:
            initial_delay = 10
            _LOGGER.info(
                "[%s] MQTT disconnected (rc=%s) — reconnecting in %ss",
                sn, rc, initial_delay,
            )

        def _schedule(delay: int = initial_delay) -> None:
            if self._mqtt_reconnect_task and not self._mqtt_reconnect_task.done():
                return
            self._mqtt_reconnect_task = self.hass.loop.create_task(
                self._async_reconnect_with_backoff(initial_delay=delay)
            )

        self.hass.loop.call_soon_threadsafe(_schedule)

    async def _async_reconnect_with_backoff(self, initial_delay: int = 10) -> None:
        """Reconnect with exponential backoff, up to 5 minutes."""
        delay = initial_delay
        while True:
            await asyncio.sleep(delay)
            if self.device.mqtt and self.device.mqtt.connected:
                return
            _LOGGER.debug("[%s] Attempting MQTT reconnect", self.device.sn)
            try:
                await self._async_connect_mqtt()
                if self.device.mqtt and self.device.mqtt.connected:
                    _LOGGER.info("[%s] MQTT reconnected successfully", self.device.sn)
                    return
            except Exception as err:
                _LOGGER.warning("[%s] MQTT reconnect failed: %s", self.device.sn, err)
            delay = min(delay * 2, 300)

    # ------------------------------------------------------------------
    # MQTT message handler
    # ------------------------------------------------------------------

    @callback
    def _on_mqtt_message(self, sn: str, message: Any) -> None:
        """Handle incoming MQTT message and update device state."""
        if sn != self.device.sn:
            return

        updated = False

        if isinstance(message, TaskStatusRsp):
            self.device.task_status = message
            _LOGGER.debug("[%s] Task state: %s (err=%s)", sn, message.state_str, message.error_code)
            updated = True

        elif isinstance(message, BatteryStatusRsp):
            self.device.battery = message
            _LOGGER.debug("[%s] Battery: %s%% @ %s°C", sn, message.percentage, message.temperature)
            updated = True

        elif isinstance(message, SensorStatusRsp):
            self.device.sensors = message
            _LOGGER.debug("[%s] Sensors: lift=%s rain=%s tilt=%s",
                          sn, message.lift_triggered, message.rain_detected, message.tilt_triggered)
            updated = True

        elif isinstance(message, RTKInfoRsp):
            self.device.rtk = message
            _LOGGER.debug("[%s] RTK: state=%s sats=%s", sn, message.state_str, message.satellites)
            updated = True

        elif isinstance(message, NetInfoRsp):
            self.device.network = message
            updated = True

        elif isinstance(message, UpgradeStatusRsp):
            self.device.upgrade = message
            _LOGGER.debug("[%s] OTA: %s %s%%", sn, message.state, message.progress)
            updated = True

        elif isinstance(message, DeviceOnlineStatusRsp):
            self.device.online = message.online
            _LOGGER.debug("[%s] Online: %s", sn, message.online)
            updated = True

        elif isinstance(message, dict) and "_raw_msg_type" in message:
            _LOGGER.debug(
                "[%s] Unknown MQTT msg type=%s fields=%s",
                sn, message["_raw_msg_type"], list(message.get("_raw_fields", {}).keys()),
            )

        if updated:
            self.async_set_updated_data(self.device)

    # ------------------------------------------------------------------
    # Commands (delegated to MQTT)
    # ------------------------------------------------------------------

    async def async_start_task(self, task_id: str, map_id: str | None = None) -> None:
        await self._require_mqtt()
        await self.device.mqtt.async_start_task(task_id, map_id)

    async def async_stop_task(self) -> None:
        await self._require_mqtt()
        await self.device.mqtt.async_stop_task()

    async def async_pause_task(self) -> None:
        await self._require_mqtt()
        await self.device.mqtt.async_pause_task()

    async def async_resume_task(self) -> None:
        await self._require_mqtt()
        await self.device.mqtt.async_resume_task()

    async def async_return_to_dock(self) -> None:
        await self._require_mqtt()
        await self.device.mqtt.async_return_to_dock()

    async def _require_mqtt(self) -> None:
        if not self.device.mqtt or not self.device.mqtt.connected:
            await self._async_connect_mqtt()
            if not self.device.mqtt or not self.device.mqtt.connected:
                raise RuntimeError("MQTT not connected — cannot send command")

    async def async_unload(self) -> None:
        """Disconnect MQTT and cancel reconnect on unload."""
        if self._mqtt_reconnect_task and not self._mqtt_reconnect_task.done():
            self._mqtt_reconnect_task.cancel()
        if self.device.mqtt:
            await self.device.mqtt.async_disconnect()
            self.device.mqtt = None
