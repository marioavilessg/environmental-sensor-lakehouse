#include "DHT.h"

#define DHTPIN 13
#define DHTTYPE DHT11

#define DEVICE_ID "sensor_aula_01"

DHT dht(DHTPIN, DHTTYPE);

void setup() {
  Serial.begin(115200);
  delay(3000);

  dht.begin();

  // Cabecera opcional para saber que arranca
  Serial.println("device_id,temperature,humidity,battery,status");
}

void loop() {
  delay(3000);  // DHT11: mejor no leer demasiado rápido

  float humidity = dht.readHumidity();
  float temperature = dht.readTemperature();
  float battery = 100.00;

  if (isnan(humidity) || isnan(temperature)) {
    Serial.print(DEVICE_ID);
    Serial.print(",");
    Serial.print("");
    Serial.print(",");
    Serial.print("");
    Serial.print(",");
    Serial.print(battery, 2);
    Serial.print(",");
    Serial.println("ERROR");
    return;
  }

  Serial.print(DEVICE_ID);
  Serial.print(",");
  Serial.print(temperature, 2);
  Serial.print(",");
  Serial.print(humidity, 2);
  Serial.print(",");
  Serial.print(battery, 2);
  Serial.print(",");
  Serial.println("OK");
}