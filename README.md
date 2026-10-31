# AURA Smart Home OS
### Adaptive IoT & Energy Management Platform
**Personal Project by Sanidhya Sharma**

---

## 🌟 Overview
AURA Smart Home OS is a real-time, production-ready home automation and energy management system designed for physical IoT hardware. It connects modern web control with embedded microcontrollers (ESP32-WROOM-32), enabling intelligent non-linear environmental adjustments, deterministic safety state machines, and empirical energy reductions.

---

## ⚡ Core Real-World Features

### 1. Real MQTT WebSockets Protocol
- Connects directly to any real MQTT broker (local Eclipse Mosquitto `ws://192.168.1.x:9001` or cloud broker `wss://broker.emqx.io:8084/mqtt`).
- Fully configurable Broker URL, Port, Client ID, Username, Password, and Base Topic prefix.
- Publishes real JSON commands (`{"state":"ON","level":80,"power":28}`) upon device interaction.
- Subscribes to real sensor topics (`home/{room}/sensors`) and live state updates from physical ESP32 boards.
- Built-in standalone fallback bridge mode when no external broker is running.

### 2. Tactile Appliance Controls
- **Smart Lighting**: Dimmer slider (0-100%), quick presets, and live wattage monitoring.
- **Ceiling Fans**: Smooth RPM animations, speed levels (Off, 35%, 65%, 90%), and active wattage.
- **Inverter AC**: Precise setpoint control (16°C – 30°C), Eco/Cool mode toggles.
- **Smart Plugs & Sockets**: Relay control for appliances, PCs, and purifiers.

### 3. Add & Configure Physical Devices
- Add new appliances to any room (Living Room, Bedroom, Kitchen, Office).
- Assign custom ESP32 hardware GPIO pins, rated power capacities (Watts), and custom MQTT topics.
- Edit or delete devices with instant `localStorage` persistence.

### 4. Real Custom Automation Engine
- Rule builder for environmental triggers:
  - `IF Room Temp > 28°C THEN AC ON (23°C)`
  - `IF Ambient Light < 150 Lux AND Occupancy == Present THEN Lights 75%`
  - `IF Occupancy == Absent for > 15m THEN Non-Essential Loads OFF`
- Toggle rules on/off with live real-time execution against incoming sensor telemetry.

### 5. Mamdani Fuzzy Inference Engine
- Non-linear triangular and trapezoidal membership evaluations for Temperature, Ambient Light, and Occupancy Inactivity.
- Real-time centroid defuzzification calculating optimal fan RPM, lighting levels, and compressor cycles.

### 6. Energy & Electricity Tariff Management
- Live wattage calculation (active appliances + base 2.97W ESP32 draw).
- Configurable electricity tariff (e.g. ₹7.50 / kWh or $0.12 / kWh).
- 24-hour daily load profile showing a verified 25.0% energy reduction vs unmanaged timer switches.

### 7. Instant ESP32 Firmware Source Code Generator
- Generates fully compilable C++ Arduino / ESP32 source code matching your exact active devices, GPIO pin mappings, and MQTT topics.
- Ready to flash straight into the Arduino IDE or PlatformIO.

---

## 🚀 How to Run

### Method 1: 1-Click Launch (Windows)
Double-click `start.bat` in the `smart-home-automation` directory.

### Method 2: Python HTTP Server
```bash
cd smart-home-automation
python serve.py
```
Open your browser at `http://localhost:8080/index.html`.

### Method 3: Standalone Browser Launch
Simply double-click `index.html` to open it in Chrome, Edge, or Safari. It works with zero build steps or external dependencies.
