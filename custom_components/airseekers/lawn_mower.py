"""Lawn mower platform for Airseekers Tron."""
from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.lawn_mower import (
    LawnMowerActivity,
    LawnMowerEntity,
    LawnMowerEntityFeature,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import (
    DOMAIN,
    MANUFACTURER,
    TASK_STATE_CHARGING,
    TASK_STATE_DOCKING,
    TASK_STATE_ERROR,
    TASK_STATE_IDLE,
    TASK_STATE_MAPPING,
    TASK_STATE_MOWING,
    TASK_STATE_PAUSED,
    TASK_STATE_RETURNING,
)
from .coordinator import AirseekersCoordinator

_LOGGER = logging.getLogger(__name__)

# Map Airseekers state → HA LawnMowerActivity
_STATE_MAP: dict[int, LawnMowerActivity] = {
    TASK_STATE_IDLE:      LawnMowerActivity.DOCKED,
    TASK_STATE_MOWING:    LawnMowerActivity.MOWING,
    TASK_STATE_PAUSED:    LawnMowerActivity.PAUSED,
    TASK_STATE_RETURNING: LawnMowerActivity.RETURNING,
    TASK_STATE_CHARGING:  LawnMowerActivity.DOCKED,
    TASK_STATE_DOCKING:   LawnMowerActivity.RETURNING,
    TASK_STATE_MAPPING:   LawnMowerActivity.MOWING,   # closest analogue
    TASK_STATE_ERROR:     LawnMowerActivity.ERROR,
}


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    coordinator: AirseekersCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([AirseekersLawnMower(coordinator)])


class AirseekersLawnMower(CoordinatorEntity[AirseekersCoordinator], LawnMowerEntity):
    """Airseekers Tron lawn mower entity."""

    _attr_supported_features = (
        LawnMowerEntityFeature.START_MOWING
        | LawnMowerEntityFeature.PAUSE
        | LawnMowerEntityFeature.DOCK
    )

    def __init__(self, coordinator: AirseekersCoordinator) -> None:
        super().__init__(coordinator)
        self._attr_unique_id = f"{coordinator.device.sn}_mower"
        self._attr_name = coordinator.device.model
        self._attr_device_info = _device_info(coordinator)

    @property
    def activity(self) -> LawnMowerActivity | None:
        device = self.coordinator.device
        if device.online is False:
            return LawnMowerActivity.ERROR
        ts = device.task_status
        if ts is None:
            return None
        return _STATE_MAP.get(ts.state, LawnMowerActivity.ERROR)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        device = self.coordinator.device
        attrs: dict[str, Any] = {
            "device_sn": device.sn,
            "firmware_version": device.firmware_version,
            "state_raw": device.state_str,
        }
        if device.task_status:
            attrs["error_code"] = device.task_status.error_code
            attrs["elapsed_time_s"] = device.task_status.elapsed_time
            attrs["total_area_m2"] = device.task_status.total_area
            attrs["remaining_area_m2"] = device.task_status.remaining_area
            attrs["task_id"] = device.task_status.task_id
        if device.network:
            attrs["wifi_ssid"] = device.network.wifi_ssid
            attrs["ip_address"] = device.network.ip_address
        return attrs

    # --- Service handlers ---

    async def async_start_mowing(self) -> None:
        """Start mowing — uses first available task, or prompts user via select."""
        tasks = self.coordinator.device.tasks
        if not tasks:
            _LOGGER.warning("No tasks defined for device %s — create a task in the app first", self.coordinator.device.sn)
            return
        # Use the first task by default; the select entity lets user pick
        task = tasks[0]
        task_id = task.get("task_id", "")
        map_id = task.get("map_id")
        if not task_id:
            _LOGGER.error("Task has no task_id: %s", task)
            return
        await self.coordinator.async_start_task(task_id, map_id)

    async def async_pause(self) -> None:
        await self.coordinator.async_pause_task()

    async def async_dock(self) -> None:
        await self.coordinator.async_return_to_dock()


def _device_info(coordinator: AirseekersCoordinator) -> dict:
    device = coordinator.device
    return {
        "identifiers": {(DOMAIN, device.sn)},
        "name": device.model,
        "manufacturer": MANUFACTURER,
        "model": device.model,
        "sw_version": device.firmware_version,
        "serial_number": device.sn,
    }
