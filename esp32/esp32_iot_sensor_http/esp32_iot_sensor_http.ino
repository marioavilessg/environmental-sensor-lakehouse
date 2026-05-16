#include <WiFi.h>
#include <HTTPClient.h>
#include <DHT.h>
#include <time.h>

// Configura estos valores antes de cargar el sketch.
const char* WIFI_SSID = "TU_WIFI";
const char* WIFI_PASSWORD = "TU_PASSWORD";
const char* INGEST_URL = "http://IP_DEL_PC:5050/iot/events";

// DHT11 o DHT22.
#define DHT_PIN 4
#define DHT_TYPE DHT22

const char* DEVICE_ID = "sensor_aula_01";
const bool DEMO_BAD_RECORDS = false;
const unsigned long SEND_INTERVAL_MS = 30000;

DHT dht(DHT_PIN, DHT_TYPE);
unsigned long lastSend = 0;
unsigned long seq = 0;

String isoTimestamp() {
  struct tm timeinfo;
  if (!getLocalTime(&timeinfo)) {
    return "";
  }
  char buffer[25];
  strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%S", &timeinfo);
  return String(buffer);
}

String statusFor(float temperature, float humidity, float battery) {
  if (isnan(temperature) || isnan(humidity)) {
    return "ERROR";
  }
  if (temperature >= 30.0 || humidity <= 35.0 || battery <= 20.0) {
    return "WARN";
  }
  return "OK";
}

void connectWifi() {
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
  }
  configTime(0, 0, "pool.ntp.org", "time.nist.gov");
}

void sendReading() {
  float temperature = dht.readTemperature();
  float humidity = dht.readHumidity();

  // Si no tienes medicion real de bateria, usamos un valor estimado decreciente.
  float battery = max(0.0f, 95.0f - (seq * 0.03f));
  String timestamp = isoTimestamp();
  String status = statusFor(temperature, humidity, battery);

  if (DEMO_BAD_RECORDS && seq % 25 == 0) {
    temperature = 150.0;
  }
  if (DEMO_BAD_RECORDS && seq % 40 == 0) {
    timestamp = "";
  }

  String eventId = "esp32_" + String((unsigned long)time(nullptr)) + "_" + String(seq);
  String payload = eventId + ",";
  payload += String(DEVICE_ID) + ",";
  payload += timestamp + ",";
  payload += String(temperature, 2) + ",";
  payload += String(humidity, 2) + ",";
  payload += String(battery, 2) + ",";
  payload += status + ",";
  payload += "esp32";

  HTTPClient http;
  http.begin(INGEST_URL);
  http.addHeader("Content-Type", "text/csv");
  int code = http.POST(payload);
  Serial.print("POST ");
  Serial.print(code);
  Serial.print(" ");
  Serial.println(payload);
  http.end();
  seq++;
}

void setup() {
  Serial.begin(115200);
  dht.begin();
  connectWifi();
  Serial.print("ESP32 IP: ");
  Serial.println(WiFi.localIP());
}

void loop() {
  if (millis() - lastSend >= SEND_INTERVAL_MS) {
    lastSend = millis();
    if (WiFi.status() != WL_CONNECTED) {
      connectWifi();
    }
    sendReading();
  }
}
