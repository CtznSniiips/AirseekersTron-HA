"""Airseekers REST API client."""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import aiohttp

from .const import (
    API_BASE_EU,
    API_BASE_EU_CLOUD,
    API_DEVICE_IOT_CERT,
    API_DEVICE_LIST,
    API_FIRMWARE_LATEST,
    API_LOGIN,
    API_MAP_LIST,
    API_REFRESH_TOKEN,
    API_SERVER_HOST,
    API_TASK_LIST,
    API_TASK_RECORD_LATEST,
    TOKEN_REFRESH_MARGIN,
    API_DEVICE_NOTIFY_LIST,
)

_LOGGER = logging.getLogger(__name__)

DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=30)


class AirseekersAuthError(Exception):
    """Raised when authentication fails."""


class AirseekersAPIError(Exception):
    """Raised on non-auth API errors."""


class AirseekersAPI:
    """Async REST client for the Airseekers cloud API."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        email: str,
        password: str,
        base_url: str = API_BASE_EU,
    ) -> None:
        self._session = session
        self._email = email
        self._password = password
        self._base_url = base_url.rstrip("/")
        self._access_token: str | None = None
        self._refresh_token: str | None = None
        self._token_expires_at: float = 0.0

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    async def async_login(self) -> dict[str, Any]:
        """Log in and cache tokens. Returns user info dict."""
        payload = {
            "email": self._email,
            "password": self._password,
        }
        data = await self._request("POST", API_LOGIN, json=payload, auth=False)
        inner = data.get("data", data)
        self._access_token  = inner.get("access_token")
        self._refresh_token = inner.get("refresh_token")
        expires_in = inner.get("expires_in", 3600)
        self._token_expires_at = time.monotonic() + int(expires_in)

        # The login response includes the regional API host to use for all
        # subsequent calls. Switch to it immediately.
        host = inner.get("host", "").rstrip("/")
        if host and host != self._base_url:
            _LOGGER.debug(
                "Airseekers: switching API base from %s to %s (from login response)",
                self._base_url, host,
            )
            self._base_url = host

        _LOGGER.debug("Airseekers login successful; token expires in %s s", expires_in)
        return data

    async def async_refresh_token(self) -> None:
        """Refresh the access token using the refresh token."""
        if not self._refresh_token:
            await self.async_login()
            return
        payload = {"refresh_token": self._refresh_token}
        try:
            data = await self._request("POST", API_REFRESH_TOKEN, json=payload, auth=False)
            self._access_token = data.get("access_token") or data.get("data", {}).get("access_token")
            new_refresh = data.get("refresh_token") or data.get("data", {}).get("refresh_token")
            if new_refresh:
                self._refresh_token = new_refresh
            expires_in = data.get("expires_in") or data.get("data", {}).get("expires_in", 3600)
            self._token_expires_at = time.monotonic() + int(expires_in)
            _LOGGER.debug("Airseekers token refreshed")
        except AirseekersAuthError:
            _LOGGER.warning("Refresh token invalid; re-logging in")
            await self.async_login()

    async def _ensure_token(self) -> None:
        """Ensure we have a valid token, refreshing if needed."""
        if self._access_token is None:
            await self.async_login()
        elif time.monotonic() >= self._token_expires_at - TOKEN_REFRESH_MARGIN:
            await self.async_refresh_token()

    # ------------------------------------------------------------------
    # Device
    # ------------------------------------------------------------------

    async def async_get_devices(self) -> list[dict[str, Any]]:
        """Return list of devices bound to this account."""
        data = await self._request("GET", API_DEVICE_LIST)
        # Response: { "code": 0, "data": [ { "device_sn": ..., ... }, ... ] }
        return _unwrap_list(data)

    async def async_get_iot_cert(self, device_sn: str) -> dict[str, Any]:
        """Fetch MQTT IoT certificate for a device.

        Returns dict with: mqtt_broker, mqtt_client_id, iot_certificate,
        iot_cert_token, cert_key, private_key.

        Static analysis of libapp.so confirms the request is a POST with body
        {"sn": "<device_sn>"}. Falls back to the cloud-eu host if the primary
        base URL returns 404, since routing differs between eu and cloud-eu.
        """
        payload = {"sn": device_sn}
        try:
            data = await self._request("POST", API_DEVICE_IOT_CERT, json=payload)
            result = _unwrap_data(data)
            if result:
                return result
        except AirseekersAPIError as err:
            if "404" not in str(err):
                raise
            _LOGGER.debug(
                "POST %s returned 404 on primary host; trying cloud-eu base URL",
                API_DEVICE_IOT_CERT,
            )

        # Retry on the cloud-eu host if primary returned 404
        alt_base = API_BASE_EU_CLOUD
        if self._base_url.rstrip("/") == alt_base.rstrip("/"):
            raise AirseekersAPIError(
                f"iot-cert 404 on both hosts for sn={device_sn}"
            )

        _LOGGER.info("Retrying iot-cert on %s", alt_base)
        alt_url = alt_base.rstrip("/") + API_DEVICE_IOT_CERT
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self._access_token}",
        }
        async with self._session.post(
            alt_url, json=payload, headers=headers, timeout=DEFAULT_TIMEOUT
        ) as resp:
            if resp.status >= 400:
                body = await resp.text()
                raise AirseekersAPIError(
                    f"iot-cert alt host HTTP {resp.status}: {body[:200]}"
                )
            body = await resp.json(content_type=None)
            return _unwrap_data(body)

    async def async_get_server_host(self) -> dict[str, Any]:
        """Get the regional server host (for region selection)."""
        data = await self._request("GET", API_SERVER_HOST, auth=False)
        return _unwrap_data(data)

    # ------------------------------------------------------------------
    # Maps
    # ------------------------------------------------------------------

    async def async_get_maps(self, device_sn: str) -> list[dict[str, Any]]:
        """Return list of maps for the device."""
        params = {"sn": device_sn}
        data = await self._request("GET", API_MAP_LIST, params=params)
        return _unwrap_list(data)

    # ------------------------------------------------------------------
    # Tasks
    # ------------------------------------------------------------------

    async def async_get_tasks(self, device_sn: str) -> list[dict[str, Any]]:
        """Return list of tasks for the device."""
        params = {"sn": device_sn}
        data = await self._request("GET", API_TASK_LIST, params=params)
        return _unwrap_list(data)

    async def async_get_latest_task_record(self, device_sn: str) -> dict[str, Any]:
        """Return the most recent completed task record."""
        params = {"sn": device_sn}
        data = await self._request("GET", API_TASK_RECORD_LATEST, params=params)
        return _unwrap_data(data)

    # ------------------------------------------------------------------
    # Firmware
    # ------------------------------------------------------------------

    async def async_get_firmware_info(self, device_sn: str) -> dict[str, Any]:
        """Check for firmware updates."""
        params = {"sn": device_sn}
        data = await self._request("GET", API_FIRMWARE_LATEST, params=params)
        return _unwrap_data(data)

    async def async_get_notifications(self, device_sn: str) -> list[dict[str, Any]]:
        """Get device notifications/alerts."""
        try:
            params = {"sn": device_sn}
            data = await self._request("GET", API_DEVICE_NOTIFY_LIST, params=params)
            return _unwrap_list(data)
        except AirseekersAPIError:
            return []

    # ------------------------------------------------------------------
    # Internal HTTP machinery
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        auth: bool = True,
        json: dict | None = None,
        params: dict | None = None,
    ) -> dict[str, Any]:
        if auth:
            await self._ensure_token()

        url = self._base_url + path
        headers: dict[str, str] = {"Content-Type": "application/json", "Accept": "application/json"}
        if auth and self._access_token:
            headers["Authorization"] = f"Bearer {self._access_token}"

        _LOGGER.debug("Airseekers API %s %s params=%s", method, path, params)
        try:
            async with self._session.request(
                method,
                url,
                headers=headers,
                json=json,
                params=params,
                timeout=DEFAULT_TIMEOUT,
            ) as resp:
                if resp.status == 401:
                    raise AirseekersAuthError(f"Unauthorized: {resp.status}")
                if resp.status >= 400:
                    body = await resp.text()
                    raise AirseekersAPIError(f"HTTP {resp.status}: {body[:200]}")
                body = await resp.json(content_type=None)
                _LOGGER.debug("Airseekers API response: %s", str(body)[:300])
                # Check app-level error code
                code = body.get("code")
                if code is not None and code != 0:
                    if code in (401, 403):
                        raise AirseekersAuthError(f"API auth error code {code}")
                    raise AirseekersAPIError(f"API error code {code}: {body.get('msg', '')}")
                return body
        except aiohttp.ClientError as err:
            raise AirseekersAPIError(f"Network error: {err}") from err


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _unwrap_data(body: dict) -> dict:
    """Unwrap standard { code, data } envelope."""
    return body.get("data") or body


def _unwrap_list(body: dict) -> list:
    """Unwrap standard { code, data: [...] } envelope."""
    data = body.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # Sometimes wrapped as { "list": [...] }
        for key in ("list", "items", "records", "devices", "maps", "tasks"):
            if key in data and isinstance(data[key], list):
                return data[key]
    return []
