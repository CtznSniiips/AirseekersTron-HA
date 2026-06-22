"""Airseekers Tron Mower integration."""
from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers import aiohttp_client

from .api import AirseekersAPI
from .const import CONF_API_BASE, CONF_DEVICE_SN, CONF_EMAIL, CONF_PASSWORD, DOMAIN
from .coordinator import AirseekersCoordinator

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [
    Platform.LAWN_MOWER,
    Platform.SENSOR,
    Platform.BINARY_SENSOR,
    Platform.BUTTON,
    Platform.SELECT,
]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Airseekers from a config entry."""
    email    = entry.data[CONF_EMAIL]
    password = entry.data[CONF_PASSWORD]
    api_base = entry.data.get(CONF_API_BASE, "https://eu.airseekers-robotics.com")
    sn       = entry.data[CONF_DEVICE_SN]

    session = aiohttp_client.async_get_clientsession(hass)
    api = AirseekersAPI(session, email, password, api_base)

    # Fetch device info to seed the coordinator
    try:
        await api.async_login()
        devices = await api.async_get_devices()
        device_info = next(
            (d for d in devices if d.get("device_sn") == sn or d.get("sn") == sn),
            {"device_sn": sn},
        )
    except Exception as err:
        _LOGGER.error("Airseekers failed to fetch device info for %s: %s", sn, err)
        device_info = {"device_sn": sn}

    coordinator = AirseekersCoordinator(hass, api, device_info)

    # First refresh — populates REST data and triggers MQTT setup
    await coordinator.async_config_entry_first_refresh()

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    coordinator: AirseekersCoordinator = hass.data[DOMAIN].get(entry.entry_id)
    if coordinator:
        await coordinator.async_unload()

    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id, None)
    return unload_ok
