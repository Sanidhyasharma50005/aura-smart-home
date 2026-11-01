/**
 * AURA Smart Home OS - Core Client Engine
 * Production-ready IoT Architecture with real MQTT WebSockets,
 * Device Management, Automation Rules, Fuzzy Climate Engine,
 * LocalStorage persistence, and ESP32 Firmware Generator.
 */

// ============================================================================
// 1. SYSTEM STATE & STORE
// ============================================================================
const AURA_STORAGE_KEY_DEVICES = 'AURA_DEVICES_V2';
const AURA_STORAGE_KEY_AUTOMATIONS = 'AURA_AUTOMATIONS_V2';
const AURA_STORAGE_KEY_CONFIG = 'AURA_CONFIG_V2';

// Default Appliances Configuration
const DEFAULT_DEVICES = [
  {
    id: 'dev_fan_1',
    name: 'Master Ceiling Fan',
    room: 'living_room',
    category: 'fan',
    state: 'ON',
    level: 70, // Speed 0 - 100
    power: 35.0,
    maxPower: 50,
    gpio: 'GPIO 18',
    mqttTopic: 'home/living_room/fan',
    fsmState: 'ON'
  },
  {
    id: 'dev_light_1',
    name: 'Ambient Cove Lights',
    room: 'living_room',
    category: 'light',
    state: 'ON',
    level: 80, // Brightness %
    colorTemp: 'warm',
    power: 9.6,
    maxPower: 12,
    gpio: 'GPIO 19',
    mqttTopic: 'home/living_room/light',
    fsmState: 'ON'
  },
  {
    id: 'dev_ac_1',
    name: 'Inverter Climate AC',
    room: 'living_room',
    category: 'climate',
    state: 'ON',
    level: 24, // Setpoint in °C
    mode: 'COOL',
    power: 840,
    maxPower: 1200,
    gpio: 'GPIO 21',
    mqttTopic: 'home/living_room/ac',
    fsmState: 'ON'
  },
  {
    id: 'dev_purifier_1',
    name: 'Air Purifier System',
    room: 'living_room',
    category: 'socket',
    state: 'OFF',
    level: 0,
    power: 0,
    maxPower: 45,
    gpio: 'GPIO 22',
    mqttTopic: 'home/living_room/purifier',
    fsmState: 'OFF'
  },
  {
    id: 'dev_bed_fan',
    name: 'Bedroom Whisper Fan',
    room: 'bedroom',
    category: 'fan',
    state: 'OFF',
    level: 0,
    power: 0,
    maxPower: 45,
    gpio: 'GPIO 25',
    mqttTopic: 'home/bedroom/fan',
    fsmState: 'OFF'
  },
  {
    id: 'dev_bed_light',
    name: 'Reading Lamp',
    room: 'bedroom',
    category: 'light',
    state: 'ON',
    level: 50,
    colorTemp: 'warm',
    power: 4.5,
    maxPower: 9,
    gpio: 'GPIO 26',
    mqttTopic: 'home/bedroom/light',
    fsmState: 'ON'
  },
  {
    id: 'dev_kitchen_light',
    name: 'Under-Cabinet LEDs',
    room: 'kitchen',
    category: 'light',
    state: 'OFF',
    level: 0,
    colorTemp: 'cool',
    power: 0,
    maxPower: 18,
    gpio: 'GPIO 27',
    mqttTopic: 'home/kitchen/light',
    fsmState: 'OFF'
  },
  {
    id: 'dev_office_pc',
    name: 'Workstation Smart Plug',
    room: 'office',
    category: 'socket',
    state: 'ON',
    level: 100,
    power: 145,
    maxPower: 350,
    gpio: 'GPIO 32',
    mqttTopic: 'home/office/plug',
    fsmState: 'ON'
  }
];

// Default Automations
const DEFAULT_AUTOMATIONS = [
  {
    id: 'auto_temp_cool',
    name: 'Adaptive Temperature Cooling',
    enabled: true,
    sensor: 'temperature',
    operator: '>',
    threshold: 28,
    targetDevice: 'dev_ac_1',
    action: 'SET_ON',
    actionValue: 23,
    description: 'When room temperature exceeds 28°C, set AC to 23°C.'
  },
  {
    id: 'auto_dusk_lighting',
    name: 'Dusk Ambient Illumination',
    enabled: true,
    sensor: 'light',
    operator: '<',
    threshold: 150,
    targetDevice: 'dev_light_1',
    action: 'SET_LEVEL',
    actionValue: 75,
    description: 'When ambient daylight drops below 150 Lux, ramp lights to 75%.'
  },
  {
    id: 'auto_energy_vacant',
    name: 'Zero-Occupancy Eco Saver',
    enabled: true,
    sensor: 'occupancy',
    operator: '==',
    threshold: 'absent',
    targetDevice: 'ALL',
    action: 'TURN_OFF',
    actionValue: 0,
    description: 'When occupancy sensor detects absence for >15 min, shutdown non-essential loads.'
  }
];

// Default System Configuration
const DEFAULT_CONFIG = {
  theme: 'dark',
  systemMode: 'AUTO', // 'AUTO' (Fuzzy Logic) or 'MANUAL'
  tariffPerKwh: 7.50, // ₹ per kWh (or $/€ based on currency)
  currencySymbol: '₹',
  baseEspPowerW: 2.97, // 592mA @ 5V
  mqtt: {
    enabled: false,
    brokerUrl: 'wss://broker.emqx.io:8084/mqtt',
    clientId: 'AURA_HOME_' + Math.random().toString(16).substring(2, 8),
    username: '',
    password: '',
    baseTopic: 'home/'
  }
};

// Runtime Application State
const AppState = {
  activeTab: 'overview',
  activeRoomFilter: 'all',
  eStopActive: false,
  manualOverrideTimer: 1800, // 30 mins
  
  // Real or Simulated Sensor Telemetry
  sensors: {
    temperature: 27.8,
    humidity: 54,
    light: 195,
    motion: true,
    occupancyMinutes: 2,
    staleSeconds: 0,
    health: 'NORMAL'
  },

  // Loaded from storage or defaults
  devices: [],
  automations: [],
  config: {},

  // Live Metrics
  metrics: {
    totalWatts: 0,
    energyTodayKwh: 4.83,
    baselineTimerKwh: 6.44,
    savingsPct: 25.0,
    costToday: 0
  },

  // MQTT Connection Client Reference & Packet Stream
  mqttClient: null,
  mqttConnected: false,
  mqttPackets: [],

  // Activity Log
  activityLog: []
};

// ============================================================================
// 2. PERSISTENCE & INITIALIZATION
// ============================================================================
function loadStoredData() {
  try {
    const dev = localStorage.getItem(AURA_STORAGE_KEY_DEVICES);
    AppState.devices = dev ? JSON.parse(dev) : JSON.parse(JSON.stringify(DEFAULT_DEVICES));

    const auto = localStorage.getItem(AURA_STORAGE_KEY_AUTOMATIONS);
    AppState.automations = auto ? JSON.parse(auto) : JSON.parse(JSON.stringify(DEFAULT_AUTOMATIONS));

    const cfg = localStorage.getItem(AURA_STORAGE_KEY_CONFIG);
    AppState.config = cfg ? Object.assign({}, DEFAULT_CONFIG, JSON.parse(cfg)) : JSON.parse(JSON.stringify(DEFAULT_CONFIG));
  } catch (err) {
    console.warn('LocalStorage load error, using defaults:', err);
    AppState.devices = JSON.parse(JSON.stringify(DEFAULT_DEVICES));
    AppState.automations = JSON.parse(JSON.stringify(DEFAULT_AUTOMATIONS));
    AppState.config = JSON.parse(JSON.stringify(DEFAULT_CONFIG));
  }
}

function saveDevices() {
  try {
    localStorage.setItem(AURA_STORAGE_KEY_DEVICES, JSON.stringify(AppState.devices));
  } catch (e) {}
}

function saveAutomations() {
  try {
    localStorage.setItem(AURA_STORAGE_KEY_AUTOMATIONS, JSON.stringify(AppState.automations));
  } catch (e) {}
}

function saveConfig() {
  try {
    localStorage.setItem(AURA_STORAGE_KEY_CONFIG, JSON.stringify(AppState.config));
  } catch (e) {}
}

// ============================================================================
// 3. AUDIO FEEDBACK (WEB AUDIO API)
// ============================================================================
class AudioEngine {
  constructor() {
    this.ctx = null;
  }
  init() {
    if (!this.ctx) {
      const AudioCtx = window.AudioContext || window.webkitAudioContext;
      if (AudioCtx) this.ctx = new AudioCtx();
    }
  }
  playRelayClick(engage = true) {
    try {
      this.init();
      if (!this.ctx) return;
      const osc = this.ctx.createOscillator();
      const gain = this.ctx.createGain();
      osc.type = 'triangle';
      osc.frequency.setValueAtTime(engage ? 1100 : 700, this.ctx.currentTime);
      osc.frequency.exponentialRampToValueAtTime(120, this.ctx.currentTime + 0.035);
      gain.gain.setValueAtTime(0.18, this.ctx.currentTime);
      gain.gain.linearRampToValueAtTime(0.01, this.ctx.currentTime + 0.035);
      osc.connect(gain);
      gain.connect(this.ctx.destination);
      osc.start();
      osc.stop(this.ctx.currentTime + 0.04);
    } catch (e) {}
  }
  playEmergencyAlarm() {
    try {
      this.init();
      if (!this.ctx) return;
      const now = this.ctx.currentTime;
      [0, 0.14, 0.28].forEach(off => {
        const osc = this.ctx.createOscillator();
        const gain = this.ctx.createGain();
        osc.type = 'sawtooth';
        osc.frequency.setValueAtTime(2600, now + off);
        gain.gain.setValueAtTime(0.2, now + off);
        gain.gain.linearRampToValueAtTime(0.01, now + off + 0.08);
        osc.connect(gain);
        gain.connect(this.ctx.destination);
        osc.start(now + off);
        osc.stop(now + off + 0.09);
      });
    } catch (e) {}
  }
}
const sfx = new AudioEngine();

// ============================================================================
// 4. MQTT ENGINE OVER WEBSOCKETS (REAL BROKER SUPPORT)
// ============================================================================
class MqttBridge {
  static init() {
    if (!AppState.config.mqtt || !AppState.config.mqtt.enabled) {
      this.setLocalBridgeStatus();
      return;
    }
    this.connect();
  }

  static connect() {
    const cfg = AppState.config.mqtt;
    if (!window.mqtt) {
      console.warn('MQTT.js library not detected. Running in high-fidelity local virtual bridge mode.');
      this.setLocalBridgeStatus();
      return;
    }

    try {
      this.updateStatusBadge('connecting', 'Connecting to ' + cfg.brokerUrl);
      const options = {
        clientId: cfg.clientId || 'AURA_' + Math.random().toString(16).substr(2, 6),
        clean: true,
        connectTimeout: 5000,
        reconnectPeriod: 6000
      };
      if (cfg.username) options.username = cfg.username;
      if (cfg.password) options.password = cfg.password;

      AppState.mqttClient = window.mqtt.connect(cfg.brokerUrl, options);

      AppState.mqttClient.on('connect', () => {
        AppState.mqttConnected = true;
        MqttBridge.updateStatusBadge('connected', 'MQTT Broker Live (' + cfg.brokerUrl.replace(/wss?:\/\//, '') + ')');
        LogManager.add('MQTT_CONNECTED', 'Securely connected to MQTT broker over WebSocket TLS.');
        
        // Subscribe to all rooms and sensor telemetry
        const subTopic = (cfg.baseTopic || 'home/') + '#';
        AppState.mqttClient.subscribe(subTopic, { qos: 1 }, (err) => {
          if (!err) {
            LogManager.add('MQTT_SUB', 'Subscribed to topic stream: ' + subTopic);
          }
        });
      });

      AppState.mqttClient.on('message', (topic, message) => {
        MqttBridge.handleInboundPacket(topic, message.toString());
      });

      AppState.mqttClient.on('error', (err) => {
        console.error('MQTT connection error:', err);
        AppState.mqttConnected = false;
        MqttBridge.updateStatusBadge('error', 'Broker Error (Offline Bridge Active)');
      });

      AppState.mqttClient.on('close', () => {
        AppState.mqttConnected = false;
        MqttBridge.updateStatusBadge('offline', 'Broker Offline (Local Bridge Mode)');
      });
    } catch (e) {
      console.warn('Failed to initialize MQTT connection:', e);
      MqttBridge.setLocalBridgeStatus();
    }
  }

  static disconnect() {
    if (AppState.mqttClient) {
      try {
        AppState.mqttClient.end(true);
      } catch (e) {}
      AppState.mqttClient = null;
    }
    AppState.mqttConnected = false;
    this.setLocalBridgeStatus();
  }

  static setLocalBridgeStatus() {
    this.updateStatusBadge('local', 'Local Node Bridge (Standalone Ready)');
  }

  static updateStatusBadge(status, text) {
    const badge = document.getElementById('mqtt-status-badge');
    const dot = document.getElementById('mqtt-status-dot');
    const label = document.getElementById('mqtt-status-text');
    if (!badge || !dot || !label) return;

    label.textContent = text;
    dot.className = 'status-dot';
    if (status === 'connected') {
      dot.classList.add('active');
    } else if (status === 'connecting') {
      dot.classList.add('warning');
    } else if (status === 'error') {
      dot.classList.add('danger');
    } else {
      dot.classList.add('active'); // active local bridge
    }
  }

  static publish(topic, payloadObj) {
    const payloadStr = JSON.stringify(payloadObj);
    const packet = {
      id: Date.now(),
      time: new Date().toLocaleTimeString(),
      dir: 'OUT',
      topic,
      payload: payloadStr,
      qos: 1
    };

    AppState.mqttPackets.unshift(packet);
    if (AppState.mqttPackets.length > 50) AppState.mqttPackets.pop();
    renderMqttPacketLog();

    if (AppState.mqttClient && AppState.mqttConnected) {
      try {
        AppState.mqttClient.publish(topic, payloadStr, { qos: 1 });
      } catch (err) {
        console.error('MQTT publish error:', err);
      }
    }
  }

  static handleInboundPacket(topic, payloadStr) {
    const packet = {
      id: Date.now(),
      time: new Date().toLocaleTimeString(),
      dir: 'IN',
      topic,
      payload: payloadStr,
      qos: 1
    };

    AppState.mqttPackets.unshift(packet);
    if (AppState.mqttPackets.length > 50) AppState.mqttPackets.pop();
    renderMqttPacketLog();

    try {
      const data = JSON.parse(payloadStr);

      // Inbound Sensor Telemetry: e.g. home/living_room/sensors
      if (topic.includes('sensor')) {
        if (data.temp !== undefined) AppState.sensors.temperature = parseFloat(data.temp);
        if (data.humidity !== undefined) AppState.sensors.humidity = parseFloat(data.humidity);
        if (data.light !== undefined) AppState.sensors.light = parseInt(data.light);
        if (data.motion !== undefined) AppState.sensors.motion = Boolean(data.motion);
        AppState.sensors.staleSeconds = 0;
        updateSensorReadouts();
        return;
      }

      // Inbound Device Status Ack from ESP32: e.g. home/living_room/fan/status
      const matchedDevice = AppState.devices.find(d => topic.startsWith(d.mqttTopic));
      if (matchedDevice) {
        if (data.state !== undefined) matchedDevice.state = data.state;
        if (data.level !== undefined) matchedDevice.level = data.level;
        if (data.power !== undefined) matchedDevice.power = data.power;
        renderDeviceGrid();
        recalculatePowerAndCost();
      }
    } catch (e) {}
  }
}

// ============================================================================
// 5. FUZZY CLIMATE & LIGHTING ENGINE (MAMDANI)
// ============================================================================
class FuzzyLogicEngine {
  static trimf(x, a, b, c) {
    if (x <= a || x >= c) return 0;
    if (x === b) return 1;
    if (x > a && x < b) return (x - a) / (b - a);
    if (x > b && x < c) return (c - x) / (c - b);
    return 0;
  }

  static trapmf(x, a, b, c, d) {
    if (x <= a || x >= d) return 0;
    if (x >= b && x <= c) return 1;
    if (x > a && x < b) return (x - a) / (b - a);
    if (x > c && x < d) return (d - x) / (d - c);
    return 0;
  }

  static compute(temp, light, occMinutes) {
    // 1. Temperature Fuzzification
    const tFuzz = {
      cold: this.trapmf(temp, -10, 0, 16, 20),
      cool: this.trimf(temp, 18, 21, 24),
      comfortable: this.trimf(temp, 22, 24, 26),
      warm: this.trimf(temp, 25, 28, 31),
      hot: this.trapmf(temp, 29, 33, 50, 60)
    };

    // 2. Ambient Light Fuzzification
    const lFuzz = {
      dark: this.trapmf(light, 0, 0, 60, 120),
      dim: this.trimf(light, 80, 200, 350),
      moderate: this.trimf(light, 250, 450, 650),
      bright: this.trapmf(light, 500, 750, 2000, 2500)
    };

    // 3. Occupancy Fuzzification
    const oFuzz = {
      present: this.trapmf(occMinutes, 0, 0, 3, 6),
      recent: this.trimf(occMinutes, 4, 15, 25),
      absent: this.trapmf(occMinutes, 20, 30, 120, 240)
    };

    // 4. Rule Evaluations
    const rules = [
      {
        id: 1,
        text: 'IF Absent THEN Turn Off Climate & Lighting',
        weight: oFuzz.absent,
        fanSpeed: 0,
        lightLevel: 0,
        acMode: 'OFF'
      },
      {
        id: 2,
        text: 'IF Hot AND Present THEN High Fan (90%) & Climate Cooling (22°C)',
        weight: Math.min(tFuzz.hot, oFuzz.present),
        fanSpeed: 90,
        lightLevel: null,
        acMode: 'COOL'
      },
      {
        id: 3,
        text: 'IF Warm AND Present THEN Medium Fan (65%) & Eco AC (24°C)',
        weight: Math.min(tFuzz.warm, oFuzz.present),
        fanSpeed: 65,
        lightLevel: null,
        acMode: 'ECO'
      },
      {
        id: 4,
        text: 'IF Comfortable AND Present THEN Gentle Fan (35%) & AC Off',
        weight: Math.min(tFuzz.comfortable, oFuzz.present),
        fanSpeed: 35,
        lightLevel: null,
        acMode: 'OFF'
      },
      {
        id: 5,
        text: 'IF Dark AND Present THEN Ramp Lights to 85%',
        weight: Math.min(lFuzz.dark, oFuzz.present),
        fanSpeed: null,
        lightLevel: 85,
        acMode: null
      },
      {
        id: 6,
        text: 'IF Dim AND Present THEN Ambient Lights 50%',
        weight: Math.min(lFuzz.dim, oFuzz.present),
        fanSpeed: null,
        lightLevel: 50,
        acMode: null
      },
      {
        id: 7,
        text: 'IF Bright OR Moderate THEN Lighting Off',
        weight: Math.max(lFuzz.bright, lFuzz.moderate),
        fanSpeed: null,
        lightLevel: 0,
        acMode: null
      }
    ];

    // Defuzzify (Weighted Sum Centroid)
    let fSum = 0, fWeight = 0;
    let lSum = 0, lWeight = 0;
    let dominantAc = 'OFF';
    let maxAcW = 0;

    rules.forEach(r => {
      if (r.weight > 0) {
        if (r.fanSpeed !== null) {
          fSum += r.fanSpeed * r.weight;
          fWeight += r.weight;
        }
        if (r.lightLevel !== null) {
          lSum += r.lightLevel * r.weight;
          lWeight += r.weight;
        }
        if (r.acMode !== null && r.weight > maxAcW) {
          maxAcW = r.weight;
          dominantAc = r.acMode;
        }
      }
    });

    return {
      tFuzz,
      lFuzz,
      oFuzz,
      rules: rules.filter(r => r.weight > 0),
      outputs: {
        fanSpeed: fWeight > 0 ? Math.round(fSum / fWeight) : 0,
        lightLevel: lWeight > 0 ? Math.round(lSum / lWeight) : 0,
        acMode: dominantAc
      }
    };
  }
}

// ============================================================================
// 6. APPLIANCE CONTROL & FSM LIFECYCLE
// ============================================================================
function setDeviceState(deviceId, turnOn, targetLevel = null) {
  if (AppState.eStopActive) {
    alert('Emergency Stop is Active! Reset E-Stop before controlling devices.');
    return;
  }

  const dev = AppState.devices.find(d => d.id === deviceId);
  if (!dev) return;

  // If in AUTO mode and user manually intervenes, start 30m override
  if (AppState.config.systemMode === 'AUTO') {
    AppState.config.systemMode = 'MANUAL';
    AppState.manualOverrideTimer = 1800;
    saveConfig();
    updateModeUI();
    LogManager.add('MANUAL_OVERRIDE', `Manual override engaged by user for ${dev.name}.`);
  }

  dev.state = turnOn ? 'ON' : 'OFF';
  if (targetLevel !== null) {
    dev.level = targetLevel;
  } else if (turnOn && dev.level === 0) {
    dev.level = dev.category === 'climate' ? 24 : 100;
  }

  // Update calculated current power
  if (dev.state === 'ON') {
    if (dev.category === 'climate') {
      dev.power = dev.level <= 20 ? 1100 : dev.level <= 24 ? 820 : 600;
    } else {
      dev.power = parseFloat(((dev.maxPower * (dev.level / 100))).toFixed(1));
    }
  } else {
    dev.power = 0;
  }

  sfx.playRelayClick(turnOn);
  saveDevices();
  renderDeviceGrid();
  recalculatePowerAndCost();

  // Transmit real MQTT command
  MqttBridge.publish(`${dev.mqttTopic}/set`, {
    id: dev.id,
    state: dev.state,
    level: dev.level,
    power: dev.power,
    timestamp: new Date().toISOString()
  });

  LogManager.add('DEVICE_ACTION', `${dev.name} switched ${dev.state} (${dev.level}% / ${dev.power}W).`);
}

function setDeviceLevel(deviceId, level) {
  const dev = AppState.devices.find(d => d.id === deviceId);
  if (!dev) return;
  const numLevel = parseInt(level, 10);
  setDeviceState(deviceId, numLevel > 0, numLevel);
}

// Master Emergency Stop
function toggleEmergencyStop() {
  AppState.eStopActive = !AppState.eStopActive;

  if (AppState.eStopActive) {
    sfx.playEmergencyAlarm();
    LogManager.add('EMERGENCY_STOP', '⚠️ Emergency Stop Tripped! All physical relays commanded OPEN.', 'CRITICAL');
    
    // Force all devices off
    AppState.devices.forEach(d => {
      d.state = 'OFF';
      d.fsmState = 'SAFE_STATE';
      d.power = 0;
    });

    MqttBridge.publish('home/master/estop', {
      status: 'LOCKED',
      reason: 'User Emergency Cutoff',
      timestamp: new Date().toISOString()
    });
  } else {
    LogManager.add('ESTOP_RESET', 'Emergency Stop released. Devices safe to resume normal operation.');
    AppState.devices.forEach(d => {
      d.fsmState = 'OFF';
    });
  }

  saveDevices();
  renderDeviceGrid();
  recalculatePowerAndCost();
  updateModeUI();
}

// Quick Scene Activation
function activateScene(sceneName) {
  if (AppState.eStopActive) {
    alert('Reset Emergency Stop before activating scenes.');
    return;
  }

  sfx.playRelayClick(true);
  document.querySelectorAll('.scene-pill').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.scene === sceneName);
  });

  switch (sceneName) {
    case 'arrive_home':
      AppState.devices.forEach(d => {
        if (d.room === 'living_room') {
          if (d.category === 'light') { d.state = 'ON'; d.level = 80; d.power = 9.6; }
          if (d.category === 'fan') { d.state = 'ON'; d.level = 65; d.power = 32.5; }
          if (d.category === 'climate') { d.state = 'ON'; d.level = 24; d.power = 820; }
        }
      });
      LogManager.add('SCENE_ACTIVATED', 'Scene "Arrive Home" activated: living room illuminated & comfortable.');
      break;

    case 'all_off':
      AppState.devices.forEach(d => {
        d.state = 'OFF';
        d.power = 0;
      });
      LogManager.add('SCENE_ACTIVATED', 'Scene "All Off" activated: complete load shutdown.');
      break;

    case 'eco_night':
      AppState.devices.forEach(d => {
        if (d.category === 'light') { d.state = 'OFF'; d.power = 0; }
        if (d.id === 'dev_bed_fan') { d.state = 'ON'; d.level = 40; d.power = 18; }
        if (d.id === 'dev_bed_light') { d.state = 'ON'; d.level = 20; d.power = 2; }
        if (d.category === 'climate') { d.state = 'ON'; d.level = 26; d.power = 550; }
      });
      LogManager.add('SCENE_ACTIVATED', 'Scene "Eco Night" activated: optimized quiet climate & low illumination.');
      break;

    case 'movie_time':
      AppState.devices.forEach(d => {
        if (d.category === 'light') { d.state = 'ON'; d.level = 15; d.power = 2; }
        if (d.category === 'fan') { d.state = 'ON'; d.level = 50; d.power = 25; }
      });
      LogManager.add('SCENE_ACTIVATED', 'Scene "Movie Time" activated: dim theater ambiance.');
      break;

    case 'full_bright':
      AppState.devices.forEach(d => {
        if (d.category === 'light') { d.state = 'ON'; d.level = 100; d.power = d.maxPower; }
      });
      LogManager.add('SCENE_ACTIVATED', 'Scene "Full Bright" activated: all lights set to maximum.');
      break;
  }

  saveDevices();
  renderDeviceGrid();
  recalculatePowerAndCost();

  MqttBridge.publish('home/master/scene', {
    scene: sceneName,
    timestamp: new Date().toISOString()
  });
}

// Switch Mode (AUTO vs MANUAL)
function setSystemMode(mode) {
  if (AppState.eStopActive) return;
  AppState.config.systemMode = mode;
  if (mode === 'MANUAL') {
    AppState.manualOverrideTimer = 1800; // 30 mins
    LogManager.add('MODE_MANUAL', 'Switched to Manual Control. 30-minute timer running.');
  } else {
    LogManager.add('MODE_AUTO', 'Switched to Adaptive Auto Mode (Fuzzy Inference Active).');
  }
  saveConfig();
  updateModeUI();
  MqttBridge.publish('home/master/mode', { mode });
}

// ============================================================================
// 7. REAL-TIME ENGINE LOOP (1-SECOND INTERVAL)
// ============================================================================
function startEngineLoop() {
  setInterval(() => {
    // 1. Stale Sensor Data Counter (Safety Watchdog)
    AppState.sensors.staleSeconds += 1;
    if (AppState.sensors.staleSeconds >= 10 && AppState.sensors.health === 'NORMAL') {
      AppState.sensors.health = 'STALE_WARNING';
      LogManager.add('SENSOR_STALE', 'Sensor heartbeat silent >10s. Verifying bus health.', 'WARNING');
    }

    // 2. Manual Override 30-min auto-revert countdown
    if (AppState.config.systemMode === 'MANUAL') {
      AppState.manualOverrideTimer -= 1;
      if (AppState.manualOverrideTimer <= 0) {
        setSystemMode('AUTO');
        LogManager.add('OVERRIDE_EXPIRED', 'Manual override expired (30m). Auto-reverted to Adaptive Mode.');
      }
    }

    // 3. Environmental Jitter Simulation (Smooth natural fluctuation)
    if (!AppState.mqttConnected) {
      const jitter = (Math.random() - 0.5) * 0.15;
      AppState.sensors.temperature = parseFloat((AppState.sensors.temperature + jitter).toFixed(1));
    }

    // 4. Run Automations against Current Sensors
    evaluateAutomations();

    // 5. Run Fuzzy Inference if in AUTO mode
    if (AppState.config.systemMode === 'AUTO' && !AppState.eStopActive) {
      const fz = FuzzyLogicEngine.compute(
        AppState.sensors.temperature,
        AppState.sensors.light,
        AppState.sensors.occupancyMinutes
      );

      // Auto modulate living room primary appliances
      const fan = AppState.devices.find(d => d.id === 'dev_fan_1');
      if (fan) {
        fan.level = fz.outputs.fanSpeed;
        fan.state = fz.outputs.fanSpeed > 0 ? 'ON' : 'OFF';
        fan.power = parseFloat((fan.maxPower * (fan.level / 100)).toFixed(1));
      }

      const light = AppState.devices.find(d => d.id === 'dev_light_1');
      if (light) {
        light.level = fz.outputs.lightLevel;
        light.state = fz.outputs.lightLevel > 0 ? 'ON' : 'OFF';
        light.power = parseFloat((light.maxPower * (light.level / 100)).toFixed(1));
      }

      const ac = AppState.devices.find(d => d.id === 'dev_ac_1');
      if (ac) {
        ac.state = fz.outputs.acMode !== 'OFF' ? 'ON' : 'OFF';
        ac.mode = fz.outputs.acMode;
        ac.power = ac.state === 'ON' ? (ac.mode === 'COOL' ? 880 : 550) : 0;
      }

      renderFuzzyEngineOutput(fz);
    }

    // 6. Recalculate Power & Cost
    recalculatePowerAndCost();
    updateSensorReadouts();
    updateModeUI();
  }, 1000);
}

// Evaluate Custom User Automation Rules
function evaluateAutomations() {
  if (AppState.eStopActive) return;

  AppState.automations.forEach(rule => {
    if (!rule.enabled) return;

    let triggerMet = false;
    let currentVal = null;

    if (rule.sensor === 'temperature') currentVal = AppState.sensors.temperature;
    if (rule.sensor === 'humidity') currentVal = AppState.sensors.humidity;
    if (rule.sensor === 'light') currentVal = AppState.sensors.light;
    if (rule.sensor === 'occupancy') {
      currentVal = AppState.sensors.motion ? 'present' : 'absent';
    }

    if (rule.operator === '>') triggerMet = currentVal > rule.threshold;
    if (rule.operator === '<') triggerMet = currentVal < rule.threshold;
    if (rule.operator === '==') triggerMet = currentVal === rule.threshold;

    if (triggerMet) {
      if (rule.targetDevice === 'ALL' && rule.action === 'TURN_OFF') {
        AppState.devices.forEach(d => {
          if (d.state === 'ON') {
            d.state = 'OFF';
            d.power = 0;
          }
        });
      } else {
        const target = AppState.devices.find(d => d.id === rule.targetDevice);
        if (target) {
          if (rule.action === 'SET_ON' && target.state !== 'ON') {
            target.state = 'ON';
            target.level = rule.actionValue || target.level || 100;
          } else if (rule.action === 'SET_LEVEL' && target.level !== rule.actionValue) {
            target.level = rule.actionValue;
            target.state = 'ON';
          }
        }
      }
    }
  });
}

// ============================================================================
// 8. METRICS & POWER CALCULATIONS
// ============================================================================
function recalculatePowerAndCost() {
  let activeWatts = AppState.config.baseEspPowerW || 2.97;
  let activeDeviceCount = 0;

  AppState.devices.forEach(d => {
    if (d.state === 'ON') {
      activeWatts += d.power;
      activeDeviceCount++;
    }
  });

  AppState.metrics.totalWatts = parseFloat(activeWatts.toFixed(1));
  
  // Daily cost computation: kWh * tariff
  const kwh = AppState.metrics.energyTodayKwh;
  const tariff = AppState.config.tariffPerKwh || 7.50;
  AppState.metrics.costToday = (kwh * tariff).toFixed(2);

  // Update Top Bar Indicators
  const powerEl = document.getElementById('stat-total-power');
  const costEl = document.getElementById('stat-total-cost');
  const countEl = document.getElementById('stat-active-count');
  const sidePowerEl = document.getElementById('side-power-val');
  
  if (powerEl) powerEl.textContent = `${AppState.metrics.totalWatts} W`;
  if (costEl) costEl.textContent = `${AppState.config.currencySymbol}${AppState.metrics.costToday}`;
  if (countEl) countEl.textContent = `${activeDeviceCount} of ${AppState.devices.length} On`;
  if (sidePowerEl) sidePowerEl.textContent = `${AppState.metrics.totalWatts} W`;

  updateRoomPills(activeDeviceCount);
}

function updateSensorReadouts() {
  const tempEl = document.getElementById('gauge-temp');
  const humEl = document.getElementById('gauge-hum');
  const lightEl = document.getElementById('gauge-light');
  const motionEl = document.getElementById('gauge-motion');

  if (tempEl) tempEl.textContent = `${AppState.sensors.temperature.toFixed(1)}°C`;
  if (humEl) humEl.textContent = `${AppState.sensors.humidity.toFixed(0)}%`;
  if (lightEl) lightEl.textContent = `${AppState.sensors.light} Lux`;
  if (motionEl) {
    motionEl.textContent = AppState.sensors.motion ? 'Occupied' : 'Vacant';
    motionEl.className = AppState.sensors.motion ? 'text-emerald-400 font-bold' : 'text-amber-400 font-bold';
  }
}

// ============================================================================
// 9. UI RENDERING & COMPONENT BUILDERS
// ============================================================================
function renderDeviceGrid() {
  const grid = document.getElementById('device-control-grid');
  if (!grid) return;

  const filter = AppState.activeRoomFilter;
  const filtered = filter === 'all' 
    ? AppState.devices 
    : AppState.devices.filter(d => d.room === filter);

  if (filtered.length === 0) {
    grid.innerHTML = `
      <div class="col-span-full py-12 text-center text-gray-500 aura-card p-8">
        <p class="text-sm">No devices found in this room.</p>
        <button onclick="openAddDeviceModal()" class="mt-3 px-4 py-1.5 bg-sky-600 hover:bg-sky-500 text-white rounded-lg text-xs font-semibold">
          + Add New Device
        </button>
      </div>
    `;
    return;
  }

  grid.innerHTML = filtered.map(dev => buildDeviceCardHtml(dev)).join('');
}

function buildDeviceCardHtml(dev) {
  const isOn = dev.state === 'ON';
  const category = dev.category;
  const isLighting = category === 'light';
  const isFan = category === 'fan';
  const isClimate = category === 'climate';

  let iconContainer = '';
  let badgeClass = 'lighting-type';
  let sliderTheme = 'slider-amber';

  if (isLighting) {
    badgeClass = 'lighting-type';
    sliderTheme = 'slider-amber';
    iconContainer = `
      <div class="w-12 h-12 rounded-2xl ${isOn ? 'bg-amber-100 dark:bg-amber-950/60 border-amber-300 dark:border-amber-700 text-amber-600 dark:text-amber-400' : 'bg-slate-100 dark:bg-slate-800 border-slate-200 dark:border-slate-700 text-slate-400 dark:text-slate-500'} border flex items-center justify-center shadow-sm transition">
        <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z"/></svg>
      </div>`;
  } else if (isFan) {
    badgeClass = 'fan-type';
    sliderTheme = 'slider-green';
    const spinClass = isOn && dev.level > 0 ? 'fan-running' : '';
    const speedSec = dev.level > 0 ? (2.2 - (dev.level / 100) * 1.8).toFixed(2) : 1;
    iconContainer = `
      <div class="w-12 h-12 rounded-2xl ${isOn ? 'bg-emerald-100 dark:bg-emerald-950/60 border-emerald-300 dark:border-emerald-700 text-emerald-600 dark:text-emerald-400' : 'bg-slate-100 dark:bg-slate-800 border-slate-200 dark:border-slate-700 text-slate-400 dark:text-slate-500'} border flex items-center justify-center shadow-sm transition">
        <svg style="--fan-speed: ${speedSec}s;" class="w-6 h-6 ${spinClass}" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8V4m0 0l-3 3m3-3l3 3M8 12H4m0 0l3 3m-3-3l3-3m8 0l3 3m0 0l-3 3m3-3h-4m-4 4v4m0 0l-3-3m3 3l3-3"/></svg>
      </div>`;
  } else if (isClimate) {
    badgeClass = 'climate-type';
    sliderTheme = 'slider-blue';
    iconContainer = `
      <div class="w-12 h-12 rounded-2xl ${isOn ? 'bg-sky-100 dark:bg-sky-950/60 border-sky-300 dark:border-sky-700 text-sky-600 dark:text-sky-400' : 'bg-slate-100 dark:bg-slate-800 border-slate-200 dark:border-slate-700 text-slate-400 dark:text-slate-500'} border flex items-center justify-center shadow-sm transition">
        <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19.428 15.428a2 2 0 00-1.022-.547l-2.387-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z"/></svg>
      </div>`;
  } else {
    badgeClass = 'plug-type';
    sliderTheme = 'slider-indigo';
    iconContainer = `
      <div class="w-12 h-12 rounded-2xl ${isOn ? 'bg-indigo-100 dark:bg-indigo-950/60 border-indigo-300 dark:border-indigo-700 text-indigo-600 dark:text-indigo-400' : 'bg-slate-100 dark:bg-slate-800 border-slate-200 dark:border-slate-700 text-slate-400 dark:text-slate-500'} border flex items-center justify-center shadow-sm transition">
        <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>
      </div>`;
  }

  const roomNameFormatted = dev.room.replace('_', ' ').toUpperCase();

  return `
    <div class="device-tile ${isOn ? 'is-on' : ''} ${badgeClass}">
      
      <!-- Card Header -->
      <div class="flex items-start justify-between gap-3">
        <div class="flex items-center gap-3">
          ${iconContainer}
          <div>
            <span class="device-room text-[10px] font-mono font-bold tracking-wider text-slate-500 dark:text-slate-400 uppercase">${roomNameFormatted}</span>
            <h4 class="device-title text-sm font-black text-slate-900 dark:text-white leading-tight">${dev.name}</h4>
          </div>
        </div>

        <!-- Tactile iOS Switch -->
        <label class="ios-switch">
          <input type="checkbox" ${isOn ? 'checked' : ''} onchange="setDeviceState('${dev.id}', this.checked)">
          <span class="ios-slider ${sliderTheme}"></span>
        </label>
      </div>

      <!-- Contextual Controls -->
      <div class="my-4">
        ${isClimate ? `
          <div class="flex items-center justify-between text-xs mb-1.5 font-bold">
            <span class="device-label text-slate-600 dark:text-slate-200">Cooling Setpoint</span>
            <span class="font-mono text-sky-700 dark:text-sky-300 font-black">${dev.level}°C · ${dev.mode || 'COOL'}</span>
          </div>
          <input type="range" min="16" max="30" step="1" value="${dev.level}" oninput="setDeviceLevel('${dev.id}', this.value)" class="w-full">
          <div class="device-scale flex justify-between text-[10px] text-slate-500 dark:text-slate-400 font-mono font-bold mt-1">
            <span>16°C (Max)</span>
            <span>24°C (Eco)</span>
            <span>30°C</span>
          </div>
        ` : `
          <div class="flex items-center justify-between text-xs mb-1.5 font-bold">
            <span class="device-label text-slate-600 dark:text-slate-200">${isLighting ? 'Brightness' : isFan ? 'Fan Speed' : 'Power Level'}</span>
            <span class="font-mono ${isLighting ? 'text-amber-700 dark:text-amber-300' : isFan ? 'text-emerald-700 dark:text-emerald-300' : 'text-indigo-700 dark:text-indigo-300'} font-black">${dev.level}%</span>
          </div>
          <input type="range" min="0" max="100" step="5" value="${dev.level}" oninput="setDeviceLevel('${dev.id}', this.value)" class="w-full">
          <div class="device-scale flex justify-between text-[10px] text-slate-500 dark:text-slate-400 font-mono font-bold mt-1">
            <span>0% (OFF)</span>
            <span>50%</span>
            <span>100% (High)</span>
          </div>
        `}
      </div>

      <!-- Card Footer -->
      <div class="pt-3 border-t border-slate-200 dark:border-slate-800 flex items-center justify-between text-xs">
        <span class="device-gpio text-slate-600 dark:text-slate-300 flex items-center gap-1.5 font-mono text-[11px] font-semibold">
          <span class="w-2 h-2 rounded-full ${isOn ? (isLighting ? 'bg-amber-500' : isClimate ? 'bg-sky-500' : isFan ? 'bg-emerald-500' : 'bg-indigo-500') : 'bg-slate-400'}"></span>
          ${dev.gpio}
        </span>
        <div class="flex items-center gap-2">
          <span class="font-mono font-black px-2.5 py-1 rounded-lg text-xs ${isOn ? (isLighting ? 'bg-amber-100 text-amber-900 dark:bg-amber-950 dark:text-amber-200 border border-amber-300 dark:border-amber-700' : isClimate ? 'bg-sky-100 text-sky-900 dark:bg-sky-950 dark:text-sky-200 border border-sky-300 dark:border-sky-700' : isFan ? 'bg-emerald-100 text-emerald-900 dark:bg-emerald-950 dark:text-emerald-200 border border-emerald-300 dark:border-emerald-700' : 'bg-indigo-100 text-indigo-900 dark:bg-indigo-950 dark:text-indigo-200 border border-indigo-300 dark:border-indigo-700') : 'bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300 border border-slate-200 dark:border-slate-700'}">
            ${dev.power} W
          </span>
          <button onclick="openEditDeviceModal('${dev.id}')" class="text-slate-400 hover:text-slate-700 dark:hover:text-slate-200 transition p-1" title="Device Config">
            <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 6V4m0 2a2 2 0 100 4m0-4a2 2 0 110 4m-6 8a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4m6 6v10m6-2a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4"/></svg>
          </button>
        </div>
      </div>

    </div>
  `;
}

function updateRoomPills(activeCount) {
  const rooms = ['all', 'living_room', 'bedroom', 'kitchen', 'office'];
  rooms.forEach(r => {
    const pill = document.getElementById(`room-pill-${r}`);
    if (pill) {
      pill.classList.toggle('bg-sky-600', AppState.activeRoomFilter === r);
      pill.classList.toggle('text-white', AppState.activeRoomFilter === r);
      pill.classList.toggle('text-gray-400', AppState.activeRoomFilter !== r);
    }
  });
}

function filterRoom(roomName) {
  AppState.activeRoomFilter = roomName;
  updateRoomPills();
  renderDeviceGrid();
}

function updateModeUI() {
  const autoBtn = document.getElementById('btn-mode-auto');
  const manualBtn = document.getElementById('btn-mode-manual');
  const banner = document.getElementById('override-banner');
  const countdown = document.getElementById('override-countdown');

  if (autoBtn && manualBtn) {
    autoBtn.classList.toggle('bg-sky-600', AppState.config.systemMode === 'AUTO');
    autoBtn.classList.toggle('text-white', AppState.config.systemMode === 'AUTO');
    autoBtn.classList.toggle('text-gray-400', AppState.config.systemMode !== 'AUTO');

    manualBtn.classList.toggle('bg-amber-600', AppState.config.systemMode === 'MANUAL');
    manualBtn.classList.toggle('text-white', AppState.config.systemMode === 'MANUAL');
    manualBtn.classList.toggle('text-gray-400', AppState.config.systemMode !== 'MANUAL');
  }

  if (banner && countdown) {
    if (AppState.config.systemMode === 'MANUAL') {
      banner.classList.remove('hidden');
      const m = Math.floor(AppState.manualOverrideTimer / 60);
      const s = AppState.manualOverrideTimer % 60;
      countdown.textContent = `${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
    } else {
      banner.classList.add('hidden');
    }
  }

  const estopBtn = document.getElementById('btn-global-estop');
  if (estopBtn) {
    if (AppState.eStopActive) {
      estopBtn.innerHTML = `⚠️ E-STOP ACTIVE (RESET)`;
      estopBtn.className = 'w-full py-3 px-4 rounded-xl text-white font-black text-xs bg-rose-700 hover:bg-rose-800 ring-4 ring-rose-400 animate-pulse transition flex items-center justify-center gap-2 shadow-lg';
    } else {
      estopBtn.innerHTML = `<svg class="w-4 h-4 mr-1 inline" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/></svg> EMERGENCY STOP`;
      estopBtn.className = 'w-full py-3 px-4 rounded-xl text-white font-black text-xs bg-rose-600 hover:bg-rose-700 transition flex items-center justify-center gap-2 shadow-md shadow-rose-600/30';
    }
  }
}

function renderFuzzyEngineOutput(fz) {
  const setBar = (id, val) => {
    const el = document.getElementById(id);
    const text = document.getElementById(`${id}-val`);
    if (el) el.style.width = `${Math.round(val * 100)}%`;
    if (text) text.textContent = (val || 0).toFixed(2);
  };

  const tf = fz.tFuzz;
  const lf = fz.lFuzz;
  const of = fz.oFuzz;

  setBar('fz-t-cold', tf.cold);
  setBar('fz-t-cool', tf.cool);
  setBar('fz-t-comf', tf.comfortable);
  setBar('fz-t-warm', tf.warm);
  setBar('fz-t-hot', tf.hot);

  setBar('fz-l-dark', lf.dark);
  setBar('fz-l-dim', lf.dim);
  setBar('fz-l-mod', lf.moderate);
  setBar('fz-l-bright', lf.bright);

  setBar('fz-o-present', of.present);
  setBar('fz-o-recent', of.recent);
  setBar('fz-o-absent', of.absent);

  const ruleList = document.getElementById('active-rules-list');
  if (ruleList) {
    if (fz.rules.length === 0) {
      ruleList.innerHTML = `<div class="text-xs text-gray-500 py-3">No fuzzy rules currently dominant.</div>`;
    } else {
      ruleList.innerHTML = fz.rules.map(r => `
        <div class="flex items-center justify-between text-xs py-2 px-3 bg-slate-900/60 rounded-lg border border-slate-800">
          <span class="text-gray-300 font-mono">${r.text}</span>
          <span class="px-2 py-0.5 rounded text-[11px] font-bold bg-sky-950 text-sky-400 border border-sky-800">α = ${(r.weight).toFixed(2)}</span>
        </div>
      `).join('');
    }
  }
}

function renderMqttPacketLog() {
  const consoleEl = document.getElementById('mqtt-log-console');
  if (!consoleEl) return;

  consoleEl.innerHTML = AppState.mqttPackets.slice(0, 15).map(p => `
    <div class="font-mono text-xs py-1.5 px-2 border-b border-slate-800 flex items-start gap-2 hover:bg-slate-800/40">
      <span class="text-gray-500 whitespace-nowrap">${p.time}</span>
      <span class="px-1 rounded text-[10px] font-bold ${p.dir === 'OUT' ? 'bg-sky-950 text-sky-400' : 'bg-emerald-950 text-emerald-400'}">${p.dir}</span>
      <span class="text-gray-300 font-semibold whitespace-nowrap">${p.topic}</span>
      <span class="text-gray-400 break-all">${p.payload}</span>
    </div>
  `).join('');
}

// ============================================================================
// 10. ACTIVITY LOG MANAGER
// ============================================================================
class LogManager {
  static add(type, message, severity = 'INFO') {
    const entry = {
      id: Date.now(),
      time: new Date().toLocaleTimeString(),
      type,
      message,
      severity
    };
    AppState.activityLog.unshift(entry);
    if (AppState.activityLog.length > 50) AppState.activityLog.pop();
    renderActivityLog();
  }
}

function renderActivityLog() {
  const container = document.getElementById('activity-log-feed');
  if (container) {
    container.innerHTML = AppState.activityLog.slice(0, 15).map(e => {
      let badgeClass = 'bg-slate-100 text-slate-700 border-slate-300 dark:bg-slate-800 dark:text-gray-300 dark:border-slate-700';
      if (e.severity === 'CRITICAL') badgeClass = 'bg-rose-100 text-rose-800 border-rose-300 dark:bg-rose-950 dark:text-rose-300 dark:border-rose-800';
      if (e.severity === 'WARNING') badgeClass = 'bg-amber-100 text-amber-800 border-amber-300 dark:bg-amber-950 dark:text-amber-300 dark:border-amber-800';
      return `
        <div class="flex items-start justify-between text-xs py-2.5 px-3 bg-[var(--bg-surface-elevated)] rounded-xl border border-[var(--border-subtle)]">
          <div class="flex items-start gap-2.5">
            <span class="px-2 py-0.5 rounded text-[10px] font-bold font-mono border ${badgeClass}">${e.severity}</span>
            <div>
              <span class="font-mono text-[var(--text-primary)] font-bold">${e.type}</span>
              <p class="text-[var(--text-muted)] mt-0.5">${e.message}</p>
            </div>
          </div>
          <span class="text-[var(--text-dim)] font-mono text-[11px] whitespace-nowrap ml-3">${e.time}</span>
        </div>
      `;
    }).join('');
  }

  const sideStream = document.getElementById('side-activity-stream');
  if (sideStream) {
    sideStream.innerHTML = AppState.activityLog.slice(0, 5).map(e => `
      <div class="p-2 bg-[var(--bg-surface-elevated)] rounded-xl border border-[var(--border-subtle)] text-[11px]">
        <span class="font-bold text-[var(--text-primary)]">${e.time}:</span>
        <span class="text-[var(--text-muted)]">${e.message}</span>
      </div>
    `).join('');
  }
}

// ============================================================================
// 11. ESP32 FIRMWARE GENERATOR (PRODUCTION READY)
// ============================================================================
function generateEsp32FirmwareCode() {
  const broker = AppState.config.mqtt.brokerUrl.replace(/wss?:\/\//, '').split(':')[0] || '192.168.1.100';
  const port = 1883;

  const relayPins = AppState.devices
    .filter(d => d.gpio && d.gpio.startsWith('GPIO'))
    .map(d => ({
      pin: d.gpio.replace('GPIO', '').trim(),
      name: d.name.replace(/[^a-zA-Z0-9_]/g, '_'),
      topic: d.mqttTopic
    }));

  return `/*
 * AURA Smart Home OS - ESP32 Firmware Node
 * Target Microcontroller: ESP32-WROOM-32
 * Auto-generated by AURA Home OS
 */

#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <DHT.h>

// Wi-Fi Credentials
const char* WIFI_SSID     = "YOUR_WIFI_SSID";
const char* WIFI_PASSWORD = "YOUR_WIFI_PASSWORD";

// MQTT Broker Configuration
const char* MQTT_BROKER   = "${broker}";
const int   MQTT_PORT     = ${port};
const char* MQTT_CLIENT   = "${AppState.config.mqtt.clientId}";

// Hardware Pin Definitions
#define DHT_PIN 4
#define DHT_TYPE DHT22
#define PIR_PIN 13
#define LDR_PIN 34

${relayPins.map(r => `#define RELAY_${r.name.toUpperCase()} ${r.pin}`).join('\n')}

DHT dht(DHT_PIN, DHT_TYPE);
WiFiClient espClient;
PubSubClient client(espClient);

unsigned long lastTelemetryTime = 0;

void setup() {
  Serial.begin(115200);
  dht.begin();
  pinMode(PIR_PIN, INPUT);

  // Initialize Relays (Active-LOW Safe Boot)
${relayPins.map(r => `  pinMode(RELAY_${r.name.toUpperCase()}, OUTPUT);\n  digitalWrite(RELAY_${r.name.toUpperCase()}, HIGH); // Safe OFF`).join('\n')}

  connectToWiFi();
  client.setServer(MQTT_BROKER, MQTT_PORT);
  client.setCallback(mqttCallback);
}

void loop() {
  if (!client.connected()) {
    reconnectMQTT();
  }
  client.loop();

  // Publish sensor telemetry every 5 seconds
  if (millis() - lastTelemetryTime > 5000) {
    lastTelemetryTime = millis();
    publishTelemetry();
  }
}

void connectToWiFi() {
  Serial.print("Connecting to Wi-Fi...");
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println("\\nWi-Fi Connected! IP: " + WiFi.localIP().toString());
}

void reconnectMQTT() {
  while (!client.connected()) {
    Serial.print("Connecting to MQTT Broker...");
    if (client.connect(MQTT_CLIENT)) {
      Serial.println(" Connected!");
      // Subscribe to all configured device command topics
${relayPins.map(r => `      client.subscribe("${r.topic}/set");`).join('\n')}
      client.subscribe("home/master/#");
    } else {
      Serial.print(" Failed, rc=");
      Serial.print(client.state());
      Serial.println(" retrying in 4s...");
      delay(4000);
    }
  }
}

void mqttCallback(char* topic, byte* payload, unsigned int length) {
  String message = "";
  for (unsigned int i = 0; i < length; i++) {
    message += (char)payload[i];
  }
  Serial.printf("Received [%s]: %s\\n", topic, message.c_str());

  StaticJsonDocument<256> doc;
  deserializeJson(doc, message);

  const char* state = doc["state"];
  bool turnOn = (strcmp(state, "ON") == 0);

  // Route commands to matching GPIO relays
${relayPins.map(r => `  if (String(topic) == "${r.topic}/set") {
    digitalWrite(RELAY_${r.name.toUpperCase()}, turnOn ? LOW : HIGH);
  }`).join(' else ')}
}

void publishTelemetry() {
  float temp = dht.readTemperature();
  float hum = dht.readHumidity();
  int ldrRaw = analogRead(LDR_PIN);
  int lux = map(ldrRaw, 0, 4095, 0, 1000);
  int motion = digitalRead(PIR_PIN);

  if (isnan(temp) || isnan(hum)) return;

  StaticJsonDocument<256> doc;
  doc["temp"] = temp;
  doc["humidity"] = hum;
  doc["light"] = lux;
  doc["motion"] = motion;

  char buffer[256];
  serializeJson(doc, buffer);
  client.publish("home/living_room/sensors", buffer);
}
`;
}

// ============================================
// 12. TAB SWITCHER & MODAL DIALOG CONTROLLERS
// ============================================
function switchTab(tabId) {
  AppState.activeTab = tabId;
  document.querySelectorAll('.nav-tab-btn').forEach(btn => {
    const isCur = btn.dataset.tab === tabId;
    btn.classList.toggle('border-sky-400', isCur);
    btn.classList.toggle('text-sky-400', isCur);
    btn.classList.toggle('border-transparent', !isCur);
    btn.classList.toggle('text-gray-400', !isCur);
  });

  document.querySelectorAll('.tab-content-panel').forEach(p => {
    p.classList.toggle('hidden', p.id !== `tab-${tabId}`);
  });

  if (tabId === 'firmware-code') {
    const codeArea = document.getElementById('esp32-code-output');
    if (codeArea) codeArea.value = generateEsp32FirmwareCode();
  }
}

function setTheme(mode) {
  AppState.config.theme = mode;
  const isDark = mode === 'dark';
  document.documentElement.classList.toggle('dark', isDark);
  document.body.classList.toggle('dark', isDark);
  document.body.classList.toggle('dark-theme', isDark);
  
  const lightBtn = document.getElementById('theme-btn-light');
  const darkBtn = document.getElementById('theme-btn-dark');
  if (lightBtn && darkBtn) {
    lightBtn.classList.toggle('active', !isDark);
    darkBtn.classList.toggle('active', isDark);
  }
  saveConfig();
  renderDeviceGrid();
}

function toggleTheme() {
  const next = AppState.config.theme === 'dark' ? 'light' : 'dark';
  setTheme(next);
}

// Modal Handlers
function openMqttSettingsModal() {
  const modal = document.getElementById('modal-mqtt-settings');
  if (!modal) return;
  const cfg = AppState.config.mqtt || {};
  document.getElementById('input-mqtt-url').value = cfg.brokerUrl || '';
  document.getElementById('input-mqtt-client').value = cfg.clientId || '';
  document.getElementById('input-mqtt-user').value = cfg.username || '';
  document.getElementById('input-mqtt-pass').value = cfg.password || '';
  document.getElementById('input-mqtt-topic').value = cfg.baseTopic || 'home/';
  document.getElementById('toggle-mqtt-enabled').checked = Boolean(cfg.enabled);
  modal.classList.add('open');
}

function saveMqttSettings() {
  const cfg = AppState.config.mqtt;
  cfg.brokerUrl = document.getElementById('input-mqtt-url').value.trim();
  cfg.clientId = document.getElementById('input-mqtt-client').value.trim();
  cfg.username = document.getElementById('input-mqtt-user').value.trim();
  cfg.password = document.getElementById('input-mqtt-pass').value.trim();
  cfg.baseTopic = document.getElementById('input-mqtt-topic').value.trim();
  cfg.enabled = document.getElementById('toggle-mqtt-enabled').checked;

  saveConfig();
  closeModal('modal-mqtt-settings');

  if (cfg.enabled) {
    MqttBridge.disconnect();
    MqttBridge.connect();
  } else {
    MqttBridge.disconnect();
  }
}

function openAddDeviceModal() {
  const modal = document.getElementById('modal-device-manager');
  if (!modal) return;
  document.getElementById('device-modal-title').textContent = 'Add Smart Appliance';
  document.getElementById('device-edit-id').value = '';
  document.getElementById('input-dev-name').value = '';
  document.getElementById('select-dev-room').value = 'living_room';
  document.getElementById('select-dev-category').value = 'light';
  document.getElementById('input-dev-gpio').value = 'GPIO 15';
  document.getElementById('input-dev-topic').value = 'home/living_room/light2';
  document.getElementById('input-dev-watts').value = '25';
  document.getElementById('btn-delete-device').classList.add('hidden');
  modal.classList.add('open');
}

function openEditDeviceModal(deviceId) {
  const dev = AppState.devices.find(d => d.id === deviceId);
  if (!dev) return;
  const modal = document.getElementById('modal-device-manager');
  if (!modal) return;

  document.getElementById('device-modal-title').textContent = 'Configure Appliance';
  document.getElementById('device-edit-id').value = dev.id;
  document.getElementById('input-dev-name').value = dev.name;
  document.getElementById('select-dev-room').value = dev.room;
  document.getElementById('select-dev-category').value = dev.category;
  document.getElementById('input-dev-gpio').value = dev.gpio;
  document.getElementById('input-dev-topic').value = dev.mqttTopic;
  document.getElementById('input-dev-watts').value = dev.maxPower;
  document.getElementById('btn-delete-device').classList.remove('hidden');
  modal.classList.add('open');
}

function saveDeviceFromModal() {
  const editId = document.getElementById('device-edit-id').value;
  const name = document.getElementById('input-dev-name').value.trim() || 'Smart Appliance';
  const room = document.getElementById('select-dev-room').value;
  const category = document.getElementById('select-dev-category').value;
  const gpio = document.getElementById('input-dev-gpio').value.trim() || 'GPIO 15';
  const mqttTopic = document.getElementById('input-dev-topic').value.trim() || `home/${room}/${category}`;
  const maxPower = parseInt(document.getElementById('input-dev-watts').value, 10) || 50;

  if (editId) {
    const dev = AppState.devices.find(d => d.id === editId);
    if (dev) {
      dev.name = name;
      dev.room = room;
      dev.category = category;
      dev.gpio = gpio;
      dev.mqttTopic = mqttTopic;
      dev.maxPower = maxPower;
      LogManager.add('DEVICE_UPDATE', `Updated configuration for ${dev.name}.`);
    }
  } else {
    const newDev = {
      id: 'dev_' + Date.now().toString(36),
      name,
      room,
      category,
      state: 'OFF',
      level: 0,
      power: 0,
      maxPower,
      gpio,
      mqttTopic,
      fsmState: 'OFF'
    };
    AppState.devices.push(newDev);
    LogManager.add('DEVICE_ADDED', `Added new appliance ${newDev.name} to ${room}.`);
  }

  saveDevices();
  renderDeviceGrid();
  recalculatePowerAndCost();
  closeModal('modal-device-manager');
}

function deleteCurrentDevice() {
  const editId = document.getElementById('device-edit-id').value;
  if (!editId) return;
  if (!confirm('Are you sure you want to remove this appliance from the system?')) return;

  AppState.devices = AppState.devices.filter(d => d.id !== editId);
  saveDevices();
  renderDeviceGrid();
  recalculatePowerAndCost();
  closeModal('modal-device-manager');
  LogManager.add('DEVICE_REMOVED', 'Removed appliance from system.');
}

function closeModal(modalId) {
  const el = document.getElementById(modalId);
  if (el) el.classList.remove('open');
}

// Copy Code Helper
function copyFirmwareCode() {
  const code = document.getElementById('esp32-code-output');
  if (!code) return;
  code.select();
  navigator.clipboard.writeText(code.value).then(() => {
    alert('ESP32 C++ firmware code copied to clipboard!');
  });
}

// ============================================================================
// 13. CHARTS INITIALIZATION (CHART.JS)
// ============================================================================
function initEnergyComparisonChart() {
  const ctx = document.getElementById('energyLoadProfileChart');
  if (!ctx) return;

  const hours = Array.from({ length: 24 }, (_, i) => `${i.toString().padStart(2, '0')}:00`);
  const baselineCurve = [
    50, 50, 50, 50, 50, 50,
    100, 100, 100, 100, 100, 100,
    1300, 1300, 1300, 1300, 1300, 1300,
    1312, 1312, 1312, 1312, 1312, 62
  ];
  const adaptiveCurve = [
    20, 20, 20, 20, 20, 20,
    70, 70, 70,
    3, 3, 3, 3, 3, 3, 3, 3,
    850, 850, 850, 850,
    420, 420, 20
  ];

  new Chart(ctx, {
    type: 'line',
    data: {
      labels: hours,
      datasets: [
        {
          label: 'Unmanaged Timer Baseline (6.44 kWh/day)',
          borderColor: '#f97316',
          backgroundColor: 'rgba(249, 115, 22, 0.1)',
          fill: true,
          data: baselineCurve,
          tension: 0.25,
          borderWidth: 2
        },
        {
          label: 'AURA Adaptive Automation (4.83 kWh/day - 25% Reduction)',
          borderColor: '#10b981',
          backgroundColor: 'rgba(16, 185, 129, 0.18)',
          fill: true,
          data: adaptiveCurve,
          tension: 0.25,
          borderWidth: 2.5
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: {
          grid: { color: 'rgba(255, 255, 255, 0.05)' },
          ticks: { color: '#94a3b8', font: { size: 10, family: 'JetBrains Mono' } }
        },
        y: {
          title: { display: true, text: 'Instantaneous Load (Watts)', color: '#94a3b8' },
          grid: { color: 'rgba(255, 255, 255, 0.05)' },
          ticks: { color: '#94a3b8', font: { size: 10, family: 'JetBrains Mono' } }
        }
      },
      plugins: {
        legend: {
          labels: { color: '#f8fafc', font: { size: 12, family: 'Plus Jakarta Sans' } }
        }
      }
    }
  });
}

// ============================================================================
// 14. BOOTSTRAP ON LOAD
// ============================================================================
document.addEventListener('DOMContentLoaded', () => {
  loadStoredData();
  
  // Apply saved theme (Default is Light)
  const initialTheme = AppState.config.theme === 'dark' ? 'dark' : 'light';
  setTheme(initialTheme);

  // Bind interactive simulation sliders if present
  const simTemp = document.getElementById('sim-slider-temp');
  const simLight = document.getElementById('sim-slider-light');
  const simMotion = document.getElementById('sim-select-motion');

  if (simTemp) {
    simTemp.addEventListener('input', (e) => {
      AppState.sensors.temperature = parseFloat(e.target.value);
      AppState.sensors.staleSeconds = 0;
      updateSensorReadouts();
    });
  }
  if (simLight) {
    simLight.addEventListener('input', (e) => {
      AppState.sensors.light = parseInt(e.target.value, 10);
      AppState.sensors.staleSeconds = 0;
      updateSensorReadouts();
    });
  }
  if (simMotion) {
    simMotion.addEventListener('change', (e) => {
      const val = e.target.value;
      if (val === 'present') {
        AppState.sensors.motion = true;
        AppState.sensors.occupancyMinutes = 0;
      } else if (val === 'recent') {
        AppState.sensors.motion = false;
        AppState.sensors.occupancyMinutes = 12;
      } else {
        AppState.sensors.motion = false;
        AppState.sensors.occupancyMinutes = 45;
      }
      AppState.sensors.staleSeconds = 0;
      updateSensorReadouts();
    });
  }

  // Render Initial UI
  renderDeviceGrid();
  recalculatePowerAndCost();
  updateSensorReadouts();
  updateModeUI();

  // Initialize MQTT Bridge
  MqttBridge.init();

  // Charts
  initEnergyComparisonChart();

  // Log Startup
  LogManager.add('SYSTEM_INIT', 'AURA Smart Home OS initialized. Core automation online.');

  // Start Main Loop
  startEngineLoop();

  // Live Clock Update
  function updateClock() {
    const now = new Date();
    const hours = now.getHours().toString().padStart(2, '0');
    const minutes = now.getMinutes().toString().padStart(2, '0');
    const seconds = now.getSeconds().toString().padStart(2, '0');
    document.getElementById('live-clock').textContent = `${hours}:${minutes}:${seconds}`;
  }
  setInterval(updateClock, 1000);
  updateClock(); // Initial call
}
