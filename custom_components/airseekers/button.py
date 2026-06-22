"""Button platform for Airseekers Tron."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Coroutine, Any

from homeassistant.components.button import ButtonEntity, ButtonEntityDescription
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import AirseekersCoordinator
from .lawn_mower import _device_info


@dataclass(frozen=True, kw_only=True)
class AirseekersButtonDescription(ButtonEntityDescription):
    press_fn: Callable[[AirseekersCoordinator], Coroutine[Any, Any, None]] | None = None


BUTTON_DESCRIPTIONS: tuple[AirseekersButtonDescription, ...] = (
    AirseekersButtonDescription(
        key="stop",
        name="Stop",
        icon="mdi:stop",
        press_fn=lambda c: c.async_stop_task(),
    ),
    AirseekersButtonDescription(
        key="resume",
        name="Resume",
        icon="mdi:play",
        press_fn=lambda c: c.async_resume_task(),
    ),
    AirseekersButtonDescription(
        key="return_to_dock",
        name="Return to Dock",
        icon="mdi:home-import-outline",
        press_fn=lambda c: c.async_return_to_dock(),
    ),
    AirseekersButtonDescription(
        key="poll_status",
        name="Refresh Status",
        icon="mdi:refresh",
        press_fn=lambda c: c.device.mqtt.async_poll_status() if c.device.mqtt else None,
        entity_registry_enabled_default=False,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    coordinator: AirseekersCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        AirseekersButton(coordinator, desc) for desc in BUTTON_DESCRIPTIONS
    )


class AirseekersButton(CoordinatorEntity[AirseekersCoordinator], ButtonEntity):
    """A command button for the Airseekers Tron device."""

    entity_description: AirseekersButtonDescription

    def __init__(
        self,
        coordinator: AirseekersCoordinator,
        description: AirseekersButtonDescription,
    ) -> None:
        super().__init__(coordinator)
        self.entity_description = description
        self._attr_unique_id = f"{coordinator.device.sn}_btn_{description.key}"
        self._attr_device_info = _device_info(coordinator)

    async def async_press(self) -> None:
        if self.entity_description.press_fn:
            result = self.entity_description.press_fn(self.coordinator)
            if result is not None:
                await result
