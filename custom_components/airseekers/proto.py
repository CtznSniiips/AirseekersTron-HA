"""
Airseekers protobuf message stubs.

The mower communicates over MQTT using binary Protobuf payloads.
The proto schemas were reconstructed from static analysis of libapp.so
(Dart AOT binary) from app v1.1.13.

Proto package: mower_proto
Files: common, status, task, config, map, network, rtk, teleop, upgrade, msg, init

⚠️  Field numbers are best-guess based on field ordering observed in the
    Dart generated code symbol names. Verify & correct against live traffic
    using: protoc --decode_raw < captured_payload.bin

To generate real .proto files, capture MQTT traffic with Wireshark on port 8883,
extract binary payloads, and run protoc --decode_raw on them.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Wire-format helpers (minimal protobuf parser for unknown schemas)
# ---------------------------------------------------------------------------

def decode_varint(data: bytes, pos: int) -> tuple[int, int]:
    """Decode a varint starting at pos. Returns (value, new_pos)."""
    result = 0
    shift = 0
    while True:
        b = data[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        shift += 7
        if not (b & 0x80):
            break
    return result, pos


def decode_raw(data: bytes) -> dict[int, list]:
    """
    Minimal protobuf --decode_raw equivalent.
    Returns {field_number: [value, ...]} where value is int, bytes, or nested dict.
    """
    pos = 0
    fields: dict[int, list] = {}
    while pos < len(data):
        tag, pos = decode_varint(data, pos)
        field_num = tag >> 3
        wire_type = tag & 0x7

        if wire_type == 0:  # varint
            value, pos = decode_varint(data, pos)
        elif wire_type == 1:  # 64-bit
            value = struct.unpack_from("<Q", data, pos)[0]
            pos += 8
        elif wire_type == 2:  # length-delimited
            length, pos = decode_varint(data, pos)
            value = data[pos:pos + length]
            pos += length
            # Try to parse nested message
            try:
                nested = decode_raw(value)
                if nested:
                    value = nested
            except Exception:
                pass  # keep as bytes
        elif wire_type == 5:  # 32-bit
            value = struct.unpack_from("<I", data, pos)[0]
            pos += 4
        else:
            break  # unknown wire type, stop parsing

        if field_num not in fields:
            fields[field_num] = []
        fields[field_num].append(value)
    return fields


# ---------------------------------------------------------------------------
# Message envelope (msg.proto)
# Based on the MsgType discriminator pattern observed in code:
# publishGetOnlineStatus, publishGetTaskPath, publishGoDockTask etc
# each produce a different message type tagged in a wrapper.
# ---------------------------------------------------------------------------

MSG_TYPE_TASK_STATUS      = 1   # TBD — verify with traffic capture
MSG_TYPE_BATTERY_STATUS   = 2
MSG_TYPE_SENSOR_STATUS    = 3
MSG_TYPE_RTK_INFO         = 4
MSG_TYPE_RTK_STATUS       = 5
MSG_TYPE_NET_INFO         = 6
MSG_TYPE_DEVICE_ONLINE    = 7
MSG_TYPE_UPGRADE_STATUS   = 8
MSG_TYPE_GO_DOCKING_RSP   = 9
MSG_TYPE_START_TASK_RSP   = 10
MSG_TYPE_STOP_TASK_RSP    = 11
MSG_TYPE_RESUME_TASK_RSP  = 12
MSG_TYPE_GET_CONFIG_RSP   = 13
MSG_TYPE_GET_TRACK_RSP    = 14

MSG_TYPE_START_TASK_REQ   = 100
MSG_TYPE_STOP_TASK_REQ    = 101
MSG_TYPE_PAUSE_TASK_REQ   = 102
MSG_TYPE_RESUME_TASK_REQ  = 103
MSG_TYPE_GO_DOCK_REQ      = 104
MSG_TYPE_GET_STATUS_REQ   = 105
MSG_TYPE_GET_NETWORK_REQ  = 106
MSG_TYPE_GET_RTK_REQ      = 107
MSG_TYPE_GET_TRACK_REQ    = 108
MSG_TYPE_GET_CONFIG_REQ   = 109
MSG_TYPE_SET_CONFIG_REQ   = 110
MSG_TYPE_SET_MOW_PARAMS   = 111


def encode_varint(value: int) -> bytes:
    """Encode integer as protobuf varint."""
    bits = []
    while value > 0x7F:
        bits.append((value & 0x7F) | 0x80)
        value >>= 7
    bits.append(value & 0x7F)
    return bytes(bits)


def encode_field_varint(field_num: int, value: int) -> bytes:
    tag = (field_num << 3) | 0  # wire type 0
    return encode_varint(tag) + encode_varint(value)


def encode_field_bytes(field_num: int, value: bytes) -> bytes:
    tag = (field_num << 3) | 2  # wire type 2
    return encode_varint(tag) + encode_varint(len(value)) + value


def encode_field_string(field_num: int, value: str) -> bytes:
    return encode_field_bytes(field_num, value.encode("utf-8"))


# ---------------------------------------------------------------------------
# Status response parsers
# Field numbers are best-guess — update after traffic capture.
# ---------------------------------------------------------------------------

@dataclass
class TaskStatusRsp:
    """Parsed TaskStatusRsp message."""
    state: int = 0            # field 1 — TaskStatusRsp_State enum
    error_code: int = 0       # field 2
    task_id: str = ""         # field 3
    task_type: int = 0        # field 4
    elapsed_time: int = 0     # field 5 — seconds
    total_area: float = 0.0   # field 6 — m²
    remaining_area: float = 0.0  # field 7 — m²
    raw: dict = field(default_factory=dict)

    @classmethod
    def from_bytes(cls, data: bytes) -> "TaskStatusRsp":
        raw = decode_raw(data)
        obj = cls(raw=raw)
        obj.state        = _int(raw, 1)
        obj.error_code   = _int(raw, 2)
        obj.task_id      = _str(raw, 3)
        obj.task_type    = _int(raw, 4)
        obj.elapsed_time = _int(raw, 5)
        obj.total_area   = _int(raw, 6) / 100.0   # assume centimetres²
        obj.remaining_area = _int(raw, 7) / 100.0
        return obj

    @property
    def state_str(self) -> str:
        from .const import TASK_STATE_MAP
        return TASK_STATE_MAP.get(self.state, f"unknown({self.state})")


@dataclass
class BatteryStatusRsp:
    """Parsed BatteryStatusRsp message."""
    percentage: int = 0     # field 1 — 0..100
    temperature: float = 0.0  # field 2 — °C (may be x10)
    anomaly: bool = False   # field 3
    low_battery: bool = False  # field 4
    rated_capacity: int = 0   # field 5 — mAh
    raw: dict = field(default_factory=dict)

    @classmethod
    def from_bytes(cls, data: bytes) -> "BatteryStatusRsp":
        raw = decode_raw(data)
        obj = cls(raw=raw)
        obj.percentage     = _int(raw, 1)
        raw_temp           = _int(raw, 2)
        obj.temperature    = raw_temp / 10.0 if raw_temp > 500 else float(raw_temp)
        obj.anomaly        = bool(_int(raw, 3))
        obj.low_battery    = bool(_int(raw, 4))
        obj.rated_capacity = _int(raw, 5)
        return obj


@dataclass
class SensorStatusRsp:
    """Parsed SensorStatusRsp message."""
    lift_triggered: bool = False   # field 1
    tilt_triggered: bool = False   # field 2
    rain_detected: bool = False    # field 3
    blade_motor_state: int = 0     # field 4
    drive_motor_state: int = 0     # field 5
    emergency_stop: bool = False   # field 6
    raw: dict = field(default_factory=dict)

    @classmethod
    def from_bytes(cls, data: bytes) -> "SensorStatusRsp":
        raw = decode_raw(data)
        obj = cls(raw=raw)
        obj.lift_triggered   = bool(_int(raw, 1))
        obj.tilt_triggered   = bool(_int(raw, 2))
        obj.rain_detected    = bool(_int(raw, 3))
        obj.blade_motor_state = _int(raw, 4)
        obj.drive_motor_state = _int(raw, 5)
        obj.emergency_stop   = bool(_int(raw, 6))
        return obj


@dataclass
class RTKInfoRsp:
    """Parsed RTKinfoRsp / RtkStatusRsp message."""
    state: int = 0         # field 1 — RTK fix state
    satellites: int = 0    # field 2
    rssi: int = 0          # field 3
    version: str = ""      # field 4
    latitude: float = 0.0  # field 5 (if present)
    longitude: float = 0.0 # field 6 (if present)
    raw: dict = field(default_factory=dict)

    RTK_STATE_IDLE        = 0
    RTK_STATE_SEARCHING   = 1
    RTK_STATE_FLOAT       = 2
    RTK_STATE_FIXED       = 3  # "differential mode" = RTK fixed

    @classmethod
    def from_bytes(cls, data: bytes) -> "RTKInfoRsp":
        raw = decode_raw(data)
        obj = cls(raw=raw)
        obj.state      = _int(raw, 1)
        obj.satellites = _int(raw, 2)
        obj.rssi       = _int(raw, 3)
        obj.version    = _str(raw, 4)
        obj.latitude   = _int(raw, 5) / 1e7
        obj.longitude  = _int(raw, 6) / 1e7
        return obj

    @property
    def state_str(self) -> str:
        return {0: "idle", 1: "searching", 2: "float", 3: "fixed"}.get(self.state, f"unknown({self.state})")

    @property
    def has_fix(self) -> bool:
        return self.state >= self.RTK_STATE_FLOAT


@dataclass
class NetInfoRsp:
    """Parsed NetInfoRsp message."""
    wifi_status: int = 0      # field 1
    ip_address: str = ""      # field 2
    wifi_ssid: str = ""       # field 3
    signal_strength: int = 0  # field 4
    use_4g: bool = False      # field 5
    raw: dict = field(default_factory=dict)

    @classmethod
    def from_bytes(cls, data: bytes) -> "NetInfoRsp":
        raw = decode_raw(data)
        obj = cls(raw=raw)
        obj.wifi_status      = _int(raw, 1)
        obj.ip_address       = _str(raw, 2)
        obj.wifi_ssid        = _str(raw, 3)
        obj.signal_strength  = _int(raw, 4)
        obj.use_4g           = bool(_int(raw, 5))
        return obj


@dataclass
class UpgradeStatusRsp:
    """Parsed UpgradeStatusRsp message."""
    state: str = "OTA_IDLE"   # field 1 — OTA_IDLE/OTA_DOWNLOAD/OTA_EXTRACT/OTA_INSTALL
    step: int = 0             # field 2
    progress: int = 0         # field 3 — 0..100
    voice_state: str = "OTA_IDLE"  # field 4
    voice_progress: int = 0   # field 5
    raw: dict = field(default_factory=dict)

    OTA_STATES = {0: "OTA_IDLE", 1: "OTA_DOWNLOAD", 2: "OTA_EXTRACT", 3: "OTA_INSTALL"}

    @classmethod
    def from_bytes(cls, data: bytes) -> "UpgradeStatusRsp":
        raw = decode_raw(data)
        obj = cls(raw=raw)
        obj.state         = cls.OTA_STATES.get(_int(raw, 1), "OTA_IDLE")
        obj.step          = _int(raw, 2)
        obj.progress      = _int(raw, 3)
        obj.voice_state   = cls.OTA_STATES.get(_int(raw, 4), "OTA_IDLE")
        obj.voice_progress = _int(raw, 5)
        return obj


@dataclass
class DeviceOnlineStatusRsp:
    """Parsed DeviceOnlineStatusRsp message."""
    online: bool = False  # field 1

    @classmethod
    def from_bytes(cls, data: bytes) -> "DeviceOnlineStatusRsp":
        raw = decode_raw(data)
        return cls(online=bool(_int(raw, 1)))


# ---------------------------------------------------------------------------
# Command encoders (App → Device)
# Field numbers best-guess — verify against traffic capture.
# ---------------------------------------------------------------------------

def build_start_task_req(task_id: str, map_id: str | None = None) -> bytes:
    """Encode StartTaskReq protobuf message."""
    msg = b""
    msg += encode_field_string(1, task_id)
    if map_id:
        msg += encode_field_string(2, map_id)
    return _wrap_message(MSG_TYPE_START_TASK_REQ, msg)


def build_stop_task_req() -> bytes:
    """Encode StopTaskReq protobuf message."""
    return _wrap_message(MSG_TYPE_STOP_TASK_REQ, b"")


def build_pause_task_req() -> bytes:
    """Encode PauseTaskReq protobuf message."""
    return _wrap_message(MSG_TYPE_PAUSE_TASK_REQ, b"")


def build_resume_task_req() -> bytes:
    """Encode ResumeTaskReq protobuf message."""
    return _wrap_message(MSG_TYPE_RESUME_TASK_REQ, b"")


def build_go_dock_req() -> bytes:
    """Encode GoDockReq protobuf message."""
    return _wrap_message(MSG_TYPE_GO_DOCK_REQ, b"")


def build_get_status_req() -> bytes:
    """Encode GetStatusReq — polls device for current state."""
    return _wrap_message(MSG_TYPE_GET_STATUS_REQ, b"")


def build_get_network_req() -> bytes:
    """Encode GetNetworkReq."""
    return _wrap_message(MSG_TYPE_GET_NETWORK_REQ, b"")


def build_get_rtk_req() -> bytes:
    """Encode GetRTKDataReq."""
    return _wrap_message(MSG_TYPE_GET_RTK_REQ, b"")


def build_set_mow_params_req(
    cut_mode: str = "BIG_CUT",
    cut_speed: int = 1,
    cutter_height: int = 50,
    path_angle: int = 0,
    rain_wait_minutes: int = 30,
    night_mode: bool = False,
    edge_cutting: bool = True,
) -> bytes:
    """Encode SetMowTaskParamsReq protobuf message."""
    msg = b""
    cut_mode_int = 0 if cut_mode == "BIG_CUT" else 1
    msg += encode_field_varint(1, cut_mode_int)
    msg += encode_field_varint(2, cut_speed)
    msg += encode_field_varint(3, cutter_height)
    msg += encode_field_varint(4, path_angle)
    msg += encode_field_varint(5, rain_wait_minutes)
    msg += encode_field_varint(6, 1 if night_mode else 0)
    msg += encode_field_varint(7, 1 if edge_cutting else 0)
    return _wrap_message(MSG_TYPE_SET_MOW_PARAMS, msg)


# ---------------------------------------------------------------------------
# Message envelope wrapper
# The app uses a MsgType discriminator field to identify message type.
# Outer structure (msg.proto): { msg_type: int, payload: bytes }
# Field numbers speculative — adjust based on traffic capture.
# ---------------------------------------------------------------------------

def _wrap_message(msg_type: int, payload: bytes) -> bytes:
    """Wrap payload in the outer message envelope."""
    outer = b""
    outer += encode_field_varint(1, msg_type)   # field 1: msg_type
    if payload:
        outer += encode_field_bytes(2, payload)  # field 2: payload bytes
    return outer


def parse_message(data: bytes) -> tuple[int, bytes]:
    """
    Parse the outer message envelope.
    Returns (msg_type, payload_bytes).
    Returns (0, data) if parsing fails — caller can still try decode_raw(data).
    """
    try:
        raw = decode_raw(data)
        msg_type = _int(raw, 1)
        payload_list = raw.get(2, [])
        payload = payload_list[0] if payload_list else b""
        if isinstance(payload, dict):
            # Already decoded as nested; re-encode isn't useful — return raw data
            payload = data
        return msg_type, payload
    except Exception:
        return 0, data


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _int(raw: dict, field_num: int) -> int:
    vals = raw.get(field_num, [])
    if not vals:
        return 0
    v = vals[0]
    return v if isinstance(v, int) else 0


def _str(raw: dict, field_num: int) -> str:
    vals = raw.get(field_num, [])
    if not vals:
        return ""
    v = vals[0]
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return str(v) if v else ""
