# Airseekers Tron — Home Assistant Custom Integration

A custom Home Assistant integration for the **Airseekers Tron** robot mower,
reverse-engineered from the official Android app (v1.1.13).

> **Status: PRELIMINARY / EXPERIMENTAL**  
> The REST API is well-understood. MQTT protobuf field numbers are best-guess
> from static analysis — they need to be confirmed against live traffic before
> full real-time control works reliably. See [Calibration](#calibration).

---

## Features

| Entity | Platform | Notes |
|--------|----------|-------|
| Mower | `lawn_mower` | Start, pause, dock; reports mowing/docked/paused/returning/error |
| Battery | `sensor` | % |
| Battery Temperature | `sensor` | °C |
| Mower State | `sensor` | Human-readable state string |
| Error Code | `sensor` | Numeric error from device |
| RTK State | `sensor` | idle / searching / float / fixed |
| RTK Satellites | `sensor` | Count |
| RTK Signal | `sensor` | RSSI |
| WiFi SSID | `sensor` | Connected network |
| Task Elapsed Time | `sensor` | Seconds |
| Task Total Area | `sensor` | m² |
| Firmware Version | `sensor` | |
| OTA Progress | `sensor` | % (hidden by default) |
| OTA State | `sensor` | (hidden by default) |
| Online | `binary_sensor` | Cloud connectivity |
| Rain Detected | `binary_sensor` | |
| Blade Lifted | `binary_sensor` | Lift safety switch |
| Tilt Triggered | `binary_sensor` | Tilt safety switch |
| Battery Anomaly | `binary_sensor` | |
| Low Battery | `binary_sensor` | |
| RTK Fixed | `binary_sensor` | True when fix ≥ float |
| OTA In Progress | `binary_sensor` | |
| Stop | `button` | |
| Resume | `button` | |
| Return to Dock | `button` | |
| Refresh Status | `button` | Manual MQTT poll |
| Selected Task | `select` | Choose which task start_mowing uses |
| Selected Map | `select` | Informational |

---

## Installation

1. Copy the `airseekers/` folder into `<config>/custom_components/`
2. Install Python dependencies:
   ```bash
   pip install paho-mqtt aiohttp protobuf
   ```
3. Restart Home Assistant
4. Go to **Settings → Devices & Services → Add Integration → Airseekers Tron**
5. Enter your Airseekers app email and password

---

## Architecture

```
HA Integration
  │
  ├── REST API  (https://eu.airseekers-robotics.com)
  │     • Login / token refresh
  │     • Enumerate devices
  │     • Fetch maps, tasks, firmware info
  │     • Fetch per-device IoT cert (for MQTT)
  │
  └── MQTT  (eiotclub broker, TLS client cert)
        • Subscribe to as/{sn}/up — device status (Protobuf binary)
        • Publish to as/{sn}/down — commands (Protobuf binary)
```

The app uses **Protocol Buffers** (not JSON) over MQTT. This integration includes
a minimal protobuf parser that decodes incoming messages without a compiled `.proto`
schema, and hand-crafted encoders for outgoing commands.

---

## Calibration

### MQTT topic confirmation

The exact topic strings need confirming for your device. They are built at runtime
from the device serial number. The current best-guess is:

```
Subscribe: as/{sn}/up
Publish:   as/{sn}/down
```

To confirm, capture MQTT traffic while using the official app:

**Option A — Network-level (easiest):**
```bash
# On the same WiFi as the mower, in Wireshark:
# Filter: tcp.port == 8883
# Or use tcpdump on your router
```
Note: traffic is TLS-encrypted. You'll need the client cert to decrypt it,
or use the Frida approach below.

**Option B — Frida on rooted Android:**
```bash
frida -U -f com.changyao.app.airseekers \
  --codeshare sowdust/universal-android-ssl-pinning-bypass-with-frida

# Then mitmproxy:
mitmproxy --mode transparent
```

Once you have topic strings, update `MQTT_TOPIC_UP_FMT` and `MQTT_TOPIC_DOWN_FMT`
in `const.py`.

### Protobuf field number calibration

Protobuf field numbers in `proto.py` are ordered guesses. To verify:

```bash
# Capture a raw MQTT payload (e.g. with mitmproxy or Wireshark)
# Save the binary payload to a file, then:
protoc --decode_raw < payload.bin
```

Compare the field numbers against the dataclass fields in `proto.py` and correct them.
The field names (battery_percentage, task state, etc.) are confirmed from the Dart
symbol table — only their wire numbers need calibration.

### Login endpoint

The login API path was confirmed as `/user/login` from the Dart source symbols.
If this returns 404, try `/api/web/user/login` and update `API_LOGIN` in `const.py`.

---

## Known Gaps

| Gap | Impact | How to resolve |
|-----|--------|----------------|
| Exact MQTT topic strings | MQTT may not connect to the right topic | Traffic capture (see above) |
| Protobuf field numbers | Status parsing may return zeros | `protoc --decode_raw` on captured payloads |
| Outer message envelope format | Message type routing may fail | Capture + decode_raw of full packet |
| Multiple regional servers | Non-EU users need different base URL | Fetch from `/api/web/server-host` at setup |
| Login request body format | Login may fail | Check if body uses `email`/`password` or `username`/`password` |

---

## Services

Beyond standard `lawn_mower` services, you can call MQTT commands directly:

```yaml
# Start a specific task
service: airseekers.start_task   # (planned — not yet a service, use lawn_mower.start_mowing)

# The lawn_mower entity supports:
service: lawn_mower.start_mowing
  entity_id: lawn_mower.airseekers_tron_<sn>

service: lawn_mower.pause
  entity_id: lawn_mower.airseekers_tron_<sn>

service: lawn_mower.dock
  entity_id: lawn_mower.airseekers_tron_<sn>
```

---

## Debugging

Enable debug logging in `configuration.yaml`:

```yaml
logger:
  default: warning
  logs:
    custom_components.airseekers: debug
```

This will log:
- All REST API calls and responses
- MQTT connection events
- Parsed MQTT message contents
- Unknown MQTT message types (with raw field numbers — useful for calibration)

---

## API Reference

See `AIRSEEKERS_API_REFERENCE.md` (generated from APK analysis) for the full
REST API endpoint list, MQTT message type inventory, and BLE characteristic UUIDs.

---

## Credits

Reverse-engineered from `Airseekers_1_1_13_APKPure.apk` via static analysis
of the Flutter/Dart AOT binary (`libapp.so`). No proprietary code is included.

Manufacturer: **Changyao Innovation (昌耀创新)** — `changyaoinno.com`
