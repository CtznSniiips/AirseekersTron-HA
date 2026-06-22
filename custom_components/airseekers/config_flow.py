"""Config flow for Airseekers Tron integration."""
from __future__ import annotations

import logging
from typing import Any

import aiohttp
import voluptuous as vol

from homeassistant import config_entries
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers import aiohttp_client

from .api import AirseekersAPI, AirseekersAPIError, AirseekersAuthError
from .const import API_BASE_EU, API_BASE_EU_CLOUD, CONF_API_BASE, CONF_DEVICE_SN, DOMAIN

_LOGGER = logging.getLogger(__name__)

REGIONS = {
    "EU": API_BASE_EU,
    "EU (Cloud)": API_BASE_EU_CLOUD,
}

STEP_USER_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_EMAIL): str,
        vol.Required(CONF_PASSWORD): str,
        vol.Optional("region", default="EU"): vol.In(list(REGIONS.keys())),
    }
)

STEP_DEVICE_SCHEMA_FACTORY = lambda device_options: vol.Schema(
    {vol.Required(CONF_DEVICE_SN): vol.In(device_options)}
)


class AirseekersConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Airseekers."""

    VERSION = 1

    def __init__(self) -> None:
        self._email: str = ""
        self._password: str = ""
        self._api_base: str = API_BASE_EU
        self._devices: list[dict] = []
        self._api: AirseekersAPI | None = None

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial step — credentials."""
        errors: dict[str, str] = {}

        if user_input is not None:
            self._email = user_input[CONF_EMAIL]
            self._password = user_input[CONF_PASSWORD]
            self._api_base = REGIONS.get(user_input.get("region", "EU"), API_BASE_EU)

            session = aiohttp_client.async_get_clientsession(self.hass)
            self._api = AirseekersAPI(session, self._email, self._password, self._api_base)

            try:
                await self._api.async_login()
                self._devices = await self._api.async_get_devices()
            except AirseekersAuthError:
                errors["base"] = "invalid_auth"
            except AirseekersAPIError as err:
                _LOGGER.warning("Airseekers API error during setup: %s", err)
                errors["base"] = "cannot_connect"
            except Exception as err:
                _LOGGER.exception("Unexpected error during Airseekers setup: %s", err)
                errors["base"] = "unknown"
            else:
                if not self._devices:
                    errors["base"] = "no_devices"
                elif len(self._devices) == 1:
                    # Skip device selection step if only one device
                    return await self._create_entry(self._devices[0])
                else:
                    return await self.async_step_device()

        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_SCHEMA,
            errors=errors,
        )

    async def async_step_device(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Select which device to integrate."""
        errors: dict[str, str] = {}

        device_options = {
            d.get("device_sn", d.get("sn", "")): (
                f"{d.get('model', 'Airseekers Tron')} — {d.get('device_sn', d.get('sn', ''))}"
            )
            for d in self._devices
        }

        if user_input is not None:
            sn = user_input[CONF_DEVICE_SN]
            device = next(
                (d for d in self._devices if d.get("device_sn") == sn or d.get("sn") == sn),
                None,
            )
            if device:
                return await self._create_entry(device)
            errors["base"] = "unknown"

        return self.async_show_form(
            step_id="device",
            data_schema=STEP_DEVICE_SCHEMA_FACTORY(device_options),
            errors=errors,
        )

    async def _create_entry(self, device: dict) -> FlowResult:
        """Create the config entry for a device."""
        sn = device.get("device_sn") or device.get("sn", "")
        await self.async_set_unique_id(sn)
        self._abort_if_unique_id_configured()

        return self.async_create_entry(
            title=f"{device.get('model', 'Airseekers Tron')} ({sn})",
            data={
                CONF_EMAIL: self._email,
                CONF_PASSWORD: self._password,
                CONF_API_BASE: self._api_base,
                CONF_DEVICE_SN: sn,
            },
        )

    async def async_step_reauth(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle re-authentication."""
        return await self.async_step_user(user_input)
