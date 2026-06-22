"""Constants for the Airseekers Tron integration."""

DOMAIN = "airseekers"
MANUFACTURER = "Airseekers (Changyao Innovation)"

# --- REST API ---
# Regional base URLs — fetched dynamically via /api/web/server-host
# Fallback defaults:
API_BASE_EU = "https://eu.airseekers-robotics.com"
API_BASE_EU_CLOUD = "https://cloud-eu.airseekers-robotics.com"

# API paths — extracted from libapp.so
API_LOGIN = "/user/login"
API_REGISTER = "/user/register"
API_GET_USER_INFO = "/user/getUserInfo"
API_SET_SELF_INFO = "/user/setSelfInfo"
API_CHANGE_EMAIL = "/user/changeEmail"
API_EMAIL_SEND = "/user/email/send"

API_REFRESH_TOKEN = "/api/web/user/refresh-token"
API_USER_SETTING = "/api/web/user/setting"
API_UPDATE_DEVICE_TOKEN = "/api/web/user/update-device-token"
API_IS_AUTHORIZED = "/api/web/user/is-authorized"
API_FIND_PASSWORD = "/api/web/find-password"

API_DEVICE_LIST = "/api/web/device"
API_DEVICE_BIND = "/api/web/device/bind"
API_DEVICE_BIND_RES = "/api/web/device/bind-res"
API_DEVICE_UNBIND = "/api/web/device/unbind"
API_DEVICE_LOCK = "/api/web/device/lock"
API_DEVICE_UNLOCK = "/api/web/device/unlock"
API_DEVICE_FIND_LOCK_PASSWORD = "/api/web/device/find-lock-password"
API_DEVICE_FACTORY_RESET = "/api/web/device/factory-reset"
API_DEVICE_IOT_CERT = "/api/web/device/iot-cert"
API_DEVICE_NOTIFY_LIST = "/api/web/device/notify/list"
API_DEVICE_RTK_ADDRESS = "/api/web/device/rtk/address-info"
API_DEVICE_WARRANTY = "/api/web/device/warranty/info"
API_DEVICE_EXTENDED_WARRANTY_ACTIVE = "/api/web/device/extended-warranty/active"
API_DEVICE_EXTENDED_WARRANTY_INFO = "/api/web/device/extended-warranty/info"

API_MAP_LIST = "/api/web/device/map"
API_MAP_NICK_NAME = "/api/web/device/map/nick-name"

API_TASK_LIST = "/api/web/device/task"  # ?sn=<sn>
API_TASK_RECORD_LATEST = "/api/web/device/task-record/latest"

API_FIRMWARE_LATEST = "/api/web/firmware/latest"
API_FIRMWARE_UPGRADE = "/api/web/firmware/upgrade"
API_APP_VERSION_LATEST = "/api/web/app-version/latest"
API_VOICE_VERSION_LATEST = "/api/web/voice-version/latest"

API_SERVER_HOST = "/api/web/server-host"
API_UPLOAD = "/api/web/upload"
API_FEEDBACK = "/api/web/feedback/ticket"

# --- MQTT ---
# Broker details come from /api/web/device/iot-cert per device.
# Topic structure: assembled at runtime from device SN.
# Based on eiotclub platform conventions and source analysis,
# likely patterns (to be confirmed via traffic capture):
MQTT_TOPIC_UP_FMT   = "as/{sn}/up"    # device → cloud (status/reports)
MQTT_TOPIC_DOWN_FMT = "as/{sn}/down"  # cloud → device (commands)

# eiotclub IoT platform
EIOTCLUB_API = "https://sim.eiotclub.com/1/"

# MQTT connection
MQTT_DEFAULT_PORT = 8883  # TLS
MQTT_QOS_CMD = 1      # atLeastOnce for commands
MQTT_QOS_STATUS = 0   # atMostOnce for status

MQTT_KEEPALIVE = 60
MQTT_RECONNECT_DELAY = 5  # seconds

# --- PROTOBUF message type tags ---
# These are the Dart class names observed; wire field numbers TBD from traffic capture.
# Used here as logical identifiers for message routing.
PROTO_TASK_STATUS_RSP      = "TaskStatusRsp"
PROTO_BATTERY_STATUS_RSP   = "BatteryStatusRsp"
PROTO_SENSOR_STATUS_RSP    = "SensorStatusRsp"
PROTO_RTK_INFO_RSP         = "RTKinfoRsp"
PROTO_RTK_STATUS_RSP       = "RtkStatusRsp"
PROTO_NET_INFO_RSP         = "NetInfoRsp"
PROTO_DEVICE_ONLINE_RSP    = "DeviceOnlineStatusRsp"
PROTO_UPGRADE_STATUS_RSP   = "UpgradeStatusRsp"
PROTO_GO_DOCKING_RSP       = "GoDockingRsp"
PROTO_START_TASK_RSP       = "StartTaskRsp"
PROTO_STOP_TASK_RSP        = "StopTaskRsp"
PROTO_RESUME_TASK_RSP      = "ResumeTaskRsp"
PROTO_GET_CONFIG_RSP       = "GetConfigRsp"
PROTO_GET_TRACK_RSP        = "GetTrackRsp"
PROTO_MOW_TASK_INFO        = "MowTaskInfo"

# --- Task state enum values (TaskStatusRsp_State) ---
TASK_STATE_IDLE       = 0
TASK_STATE_MOWING     = 1
TASK_STATE_PAUSED     = 2
TASK_STATE_RETURNING  = 3
TASK_STATE_CHARGING   = 4
TASK_STATE_DOCKING    = 5
TASK_STATE_MAPPING    = 6
TASK_STATE_ERROR      = 7

TASK_STATE_MAP = {
    TASK_STATE_IDLE:      "idle",
    TASK_STATE_MOWING:    "mowing",
    TASK_STATE_PAUSED:    "paused",
    TASK_STATE_RETURNING: "returning",
    TASK_STATE_CHARGING:  "charging",
    TASK_STATE_DOCKING:   "docking",
    TASK_STATE_MAPPING:   "mapping",
    TASK_STATE_ERROR:     "error",
}

# --- OTA state ---
OTA_IDLE     = "OTA_IDLE"
OTA_DOWNLOAD = "OTA_DOWNLOAD"
OTA_EXTRACT  = "OTA_EXTRACT"
OTA_INSTALL  = "OTA_INSTALL"

# --- Cut modes ---
CUT_MODE_BIG_CUT   = "BIG_CUT"
CUT_MODE_SMALL_CUT = "SMALL_CUT"

# --- Map area types ---
AREA_TYPE_WORK         = "TYPE_WORK"
AREA_TYPE_CHARGE_AREA  = "TYPE_CHARGE_AREA"
AREA_TYPE_RESTRICTED   = "TYPE_RESTRICTED"
AREA_TYPE_RTK_POINT    = "TYPE_RTK_POINT"
AREA_TYPE_UNDOCK_POINT = "TYPE_UNDOCK_POINT"
AREA_TYPE_CONNECTION   = "TYPE_CONNECTION"

# --- Config/Storage keys ---
CONF_API_BASE    = "api_base"
CONF_EMAIL       = "email"
CONF_PASSWORD    = "password"
CONF_REGION      = "region"
CONF_ACCESS_TOKEN  = "access_token"
CONF_REFRESH_TOKEN = "refresh_token"
CONF_DEVICE_SN   = "device_sn"

# --- HA entity update intervals ---
POLL_INTERVAL_FAST = 10   # seconds — used for task status polling
POLL_INTERVAL_SLOW = 300  # seconds — used for device info / firmware checks

# --- Token ---
TOKEN_REFRESH_MARGIN = 300  # refresh if within 5 min of expiry

# --- BLE (provisioning only — not used in HA integration) ---
BLE_SERVICE_SETUP    = "c1e88b20-03d7-11f0-8a15-c53e45d1044f"
BLE_CHAR_WRITE       = "5205daa0-03d8-11f0-8a15-c53e45d1044f"
BLE_CHAR_NOTIFY      = "5205daa1-03d8-11f0-8a15-c53e45d1044f"
BLE_SERVICE_MAPPING  = "24b1da90-6157-11f0-8a3b-d5b541d8e2cf"
BLE_CHAR_MAP_WRITE   = "ab7c1ee0-6158-11f0-8a3b-d5b541d8e2cf"
BLE_CHAR_MAP_NOTIFY  = "ab7c1ee1-6158-11f0-8a3b-d5b541d8e2cf"
