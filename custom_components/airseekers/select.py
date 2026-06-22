"""Select platform for Airseekers Tron — choose active task or map."""
from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.select import SelectEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import AirseekersCoordinator
from .lawn_mower import _device_info

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    coordinator: AirseekersCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([
        AirseekersTaskSelect(coordinator),
        AirseekersMapSelect(coordinator),
    ])


class AirseekersTaskSelect(CoordinatorEntity[AirseekersCoordinator], SelectEntity):
    """
    Select the active task to start.
    Calling start_mowing on the lawn_mower entity will use the selected task.
    """

    _attr_icon = "mdi:format-list-checks"

    def __init__(self, coordinator: AirseekersCoordinator) -> None:
        super().__init__(coordinator)
        self._attr_unique_id = f"{coordinator.device.sn}_selected_task"
        self._attr_name = "Selected Task"
        self._attr_device_info = _device_info(coordinator)
        self._selected: str | None = None

    @property
    def options(self) -> list[str]:
        tasks = self.coordinator.device.tasks
        if not tasks:
            return []
        return [self._task_label(t) for t in tasks]

    @property
    def current_option(self) -> str | None:
        if self._selected:
            return self._selected
        opts = self.options
        return opts[0] if opts else None

    def _task_label(self, task: dict) -> str:
        name = task.get("task_name") or task.get("name") or task.get("task_id", "")
        return str(name)

    def _task_by_label(self, label: str) -> dict | None:
        for t in self.coordinator.device.tasks:
            if self._task_label(t) == label:
                return t
        return None

    async def async_select_option(self, option: str) -> None:
        self._selected = option
        self.async_write_ha_state()

    def selected_task_id(self) -> str | None:
        """Return the task_id of the currently selected task."""
        label = self.current_option
        if not label:
            return None
        task = self._task_by_label(label)
        return task.get("task_id") if task else None

    def selected_map_id(self) -> str | None:
        """Return the map_id of the currently selected task."""
        label = self.current_option
        if not label:
            return None
        task = self._task_by_label(label)
        return task.get("map_id") if task else None


class AirseekersMapSelect(CoordinatorEntity[AirseekersCoordinator], SelectEntity):
    """Select the active map (informational — shown on device card)."""

    _attr_icon = "mdi:map-outline"
    _attr_entity_registry_enabled_default = False

    def __init__(self, coordinator: AirseekersCoordinator) -> None:
        super().__init__(coordinator)
        self._attr_unique_id = f"{coordinator.device.sn}_selected_map"
        self._attr_name = "Selected Map"
        self._attr_device_info = _device_info(coordinator)
        self._selected: str | None = None

    @property
    def options(self) -> list[str]:
        maps = self.coordinator.device.maps
        if not maps:
            return []
        return [self._map_label(m) for m in maps]

    @property
    def current_option(self) -> str | None:
        if self._selected:
            return self._selected
        opts = self.options
        return opts[0] if opts else None

    def _map_label(self, m: dict) -> str:
        return m.get("map_name") or m.get("nick_name") or m.get("map_id", "")

    async def async_select_option(self, option: str) -> None:
        self._selected = option
        self.async_write_ha_state()
