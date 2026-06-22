"""Binary sensor platform for Airseekers Tron."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
    BinarySensorEntityDescription,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import AirseekersCoordinator, AirseekersDeviceData
from .lawn_mower import _device_info


@dataclass(frozen=True, kw_only=True)
class AirseekerseBinarySensorDescription(BinarySensorEntityDescription):
    value_fn: Callable[[AirseekersDeviceData], bool | None] = lambda d: None
    available_fn: Callable[[AirseekersDeviceData], bool] = lambda d: True


BINARY_SENSOR_DESCRIPTIONS: tuple[AirseekerseBinarySensorDescription, ...] = (
    AirseekerseBinarySensorDescription(
        key="online",
        name="Online",
        device_class=BinarySensorDeviceClass.CONNECTIVITY,
        value_fn=lambda d: d.is_online,
        available_fn=lambda d: d.is_online is not None,
    ),
    AirseekerseBinarySensorDescription(
        key="rain_detected",
        name="Rain Detected",
        device_class=BinarySensorDeviceClass.MOISTURE,
        value_fn=lambda d: d.rain_detected,
        available_fn=lambda d: d.sensors is not None,
        icon="mdi:weather-rainy",
    ),
    AirseekerseBinarySensorDescription(
        key="lift_triggered",
        name="Blade Lifted",
        device_class=BinarySensorDeviceClass.SAFETY,
        value_fn=lambda d: d.lift_triggered,
        available_fn=lambda d: d.sensors is not None,
        icon="mdi:elevator-up",
    ),
    AirseekerseBinarySensorDescription(
        key="tilt_triggered",
        name="Tilt Triggered",
        device_class=BinarySensorDeviceClass.SAFETY,
        value_fn=lambda d: d.tilt_triggered,
        available_fn=lambda d: d.sensors is not None,
        icon="mdi:angle-acute",
    ),
    AirseekerseBinarySensorDescription(
        key="battery_anomaly",
        name="Battery Anomaly",
        device_class=BinarySensorDeviceClass.PROBLEM,
        value_fn=lambda d: d.battery.anomaly if d.battery else None,
        available_fn=lambda d: d.battery is not None,
        icon="mdi:battery-alert",
        entity_registry_enabled_default=False,
    ),
    AirseekerseBinarySensorDescription(
        key="low_battery",
        name="Low Battery",
        device_class=BinarySensorDeviceClass.BATTERY,
        value_fn=lambda d: d.battery.low_battery if d.battery else None,
        available_fn=lambda d: d.battery is not None,
    ),
    AirseekerseBinarySensorDescription(
        key="rtk_fixed",
        name="RTK Fixed",
        device_class=BinarySensorDeviceClass.LOCK,  # "locked in" to signal
        value_fn=lambda d: d.rtk_has_fix,
        available_fn=lambda d: d.rtk is not None,
        icon="mdi:crosshairs-gps",
    ),
    AirseekerseBinarySensorDescription(
        key="ota_in_progress",
        name="OTA Update In Progress",
        device_class=BinarySensorDeviceClass.UPDATE,
        value_fn=lambda d: d.ota_in_progress,
        available_fn=lambda d: d.upgrade is not None,
        entity_registry_enabled_default=False,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    coordinator: AirseekersCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        AirseekerseBinarySensor(coordinator, desc)
        for desc in BINARY_SENSOR_DESCRIPTIONS
    )


class AirseekerseBinarySensor(
    CoordinatorEntity[AirseekersCoordinator], BinarySensorEntity
):
    """A binary sensor for the Airseekers Tron device."""

    entity_description: AirseekerseBinarySensorDescription

    def __init__(
        self,
        coordinator: AirseekersCoordinator,
        description: AirseekerseBinarySensorDescription,
    ) -> None:
        super().__init__(coordinator)
        self.entity_description = description
        self._attr_unique_id = f"{coordinator.device.sn}_{description.key}"
        self._attr_device_info = _device_info(coordinator)

    @property
    def is_on(self) -> bool | None:
        return self.entity_description.value_fn(self.coordinator.device)

    @property
    def available(self) -> bool:
        return (
            super().available
            and self.entity_description.available_fn(self.coordinator.device)
        )
