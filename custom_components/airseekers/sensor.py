"""Sensor platform for Airseekers Tron."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    PERCENTAGE,
    UnitOfTemperature,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import AirseekersCoordinator, AirseekersDeviceData
from .lawn_mower import _device_info


@dataclass(frozen=True, kw_only=True)
class AirseekerseSensorDescription(SensorEntityDescription):
    value_fn: Callable[[AirseekersDeviceData], Any] = lambda d: None
    available_fn: Callable[[AirseekersDeviceData], bool] = lambda d: True


SENSOR_DESCRIPTIONS: tuple[AirseekerseSensorDescription, ...] = (
    AirseekerseSensorDescription(
        key="last_notification",
        name="Last Notification",
        value_fn=lambda d: (
            d.device_info.get("_last_notification_content", "")
            if d.device_info else ""
        ),
        icon="mdi:bell-outline",
        entity_registry_enabled_default=False,
    ),
    AirseekerseSensorDescription(
        key="battery",
        name="Battery",
        device_class=SensorDeviceClass.BATTERY,
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=PERCENTAGE,
        value_fn=lambda d: d.battery_percentage,
        available_fn=lambda d: d.battery is not None,
    ),
    AirseekerseSensorDescription(
        key="battery_temperature",
        name="Battery Temperature",
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        value_fn=lambda d: d.battery_temperature,
        available_fn=lambda d: d.battery is not None,
    ),
    AirseekerseSensorDescription(
        key="mower_state",
        name="Mower State",
        value_fn=lambda d: d.state_str,
        icon="mdi:robot-mower",
    ),
    AirseekerseSensorDescription(
        key="error_code",
        name="Error Code",
        value_fn=lambda d: d.error_code,
        available_fn=lambda d: d.task_status is not None,
        icon="mdi:alert-circle-outline",
        entity_registry_enabled_default=False,
    ),
    AirseekerseSensorDescription(
        key="rtk_state",
        name="RTK State",
        value_fn=lambda d: d.rtk_state,
        available_fn=lambda d: d.rtk is not None,
        icon="mdi:crosshairs-gps",
    ),
    AirseekerseSensorDescription(
        key="rtk_satellites",
        name="RTK Satellites",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.rtk_satellites,
        available_fn=lambda d: d.rtk is not None,
        icon="mdi:satellite-variant",
    ),
    AirseekerseSensorDescription(
        key="rtk_signal",
        name="RTK Signal Strength",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.rtk.rssi if d.rtk else None,
        available_fn=lambda d: d.rtk is not None,
        icon="mdi:signal",
    ),
    AirseekerseSensorDescription(
        key="wifi_ssid",
        name="WiFi SSID",
        value_fn=lambda d: d.network.wifi_ssid if d.network else None,
        available_fn=lambda d: d.network is not None,
        icon="mdi:wifi",
        entity_registry_enabled_default=False,
    ),
    AirseekerseSensorDescription(
        key="task_elapsed_time",
        name="Task Elapsed Time",
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement="s",
        device_class=SensorDeviceClass.DURATION,
        value_fn=lambda d: d.task_status.elapsed_time if d.task_status else None,
        available_fn=lambda d: d.task_status is not None and d.task_status.elapsed_time > 0,
        icon="mdi:timer-outline",
    ),
    AirseekerseSensorDescription(
        key="task_total_area",
        name="Task Total Area",
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement="m²",
        value_fn=lambda d: d.task_status.total_area if d.task_status else None,
        available_fn=lambda d: d.task_status is not None,
        icon="mdi:texture-box",
    ),
    AirseekerseSensorDescription(
        key="firmware_version",
        name="Firmware Version",
        value_fn=lambda d: d.firmware_version,
        icon="mdi:chip",
        entity_registry_enabled_default=False,
    ),
    AirseekerseSensorDescription(
        key="ota_progress",
        name="OTA Progress",
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=PERCENTAGE,
        value_fn=lambda d: d.ota_progress,
        available_fn=lambda d: d.upgrade is not None and d.ota_in_progress,
        icon="mdi:update",
        entity_registry_enabled_default=False,
    ),
    AirseekerseSensorDescription(
        key="ota_state",
        name="OTA State",
        value_fn=lambda d: d.upgrade.state if d.upgrade else None,
        available_fn=lambda d: d.upgrade is not None,
        icon="mdi:update",
        entity_registry_enabled_default=False,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    coordinator: AirseekersCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        AirseekerseSensor(coordinator, desc) for desc in SENSOR_DESCRIPTIONS
    )


class AirseekerseSensor(CoordinatorEntity[AirseekersCoordinator], SensorEntity):
    """A sensor for the Airseekers Tron device."""

    entity_description: AirseekerseSensorDescription

    def __init__(
        self,
        coordinator: AirseekersCoordinator,
        description: AirseekerseSensorDescription,
    ) -> None:
        super().__init__(coordinator)
        self.entity_description = description
        self._attr_unique_id = f"{coordinator.device.sn}_{description.key}"
        self._attr_device_info = _device_info(coordinator)

    @property
    def native_value(self) -> Any:
        return self.entity_description.value_fn(self.coordinator.device)

    @property
    def available(self) -> bool:
        return (
            super().available
            and self.entity_description.available_fn(self.coordinator.device)
        )
