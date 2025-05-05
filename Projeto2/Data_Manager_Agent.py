# Subscreve nos tópicos das máquinas: "v3/{GroupID}@ttn/devices/{machine_id}/up" 
# Normaliza e processa os dados e envia-os para um topico a começar por {GroupID} para o Data_Manager_Agent ver valores fora dos limites
# Comunica com o Alert_manager (por UDP) para ver se a máquina está saudável ou não, e se precisa de parar
# É o ÚNICO componente que comunica diretamente com as máquinas de uma forma estranha com 4bits que dizem se tem que parar, se tem q diminuir valores muito altos...


import paho.mqtt.client as mqtt
import time
import json
import socket
from datetime import datetime
from influxdb_client_3 import InfluxDBClient3, Point

GroupID = "11"
machine_id = "M1"
host, port = None, None
ServerSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
# ServerSocket.bind((host, port)) escutar mensagens
# data, addr = ServerSocket.recvfrom(1024) receber mensagens UDP do Alert_manager

#------------- Guardar no InfluxDB ------------------
token = "t-IWyUYSu_EHbRRsjL_RtB8U_fUV5-TCRKJ2BsUJZdzhOsz51Nzr8BZKj4BgtEhmJQ9LE9i4xXOl2_YFZVb9lw==" 			# CHANGE TO YOUR INFLUXDB CLOUD TOKEN
org = "SRSA_PL"		# CHANGE TO YOUR INFLUXDB CLOUD ORGANIZATION
host = "https://eu-central-1-1.aws.cloud2.influxdata.com" # CHANGE CHANGE TO YOUR INFLUXDB CLOUD HOST URL
database = "SRSA"		# CHANGE TO YOUR INFLUXDB CLOUD BUCKET
write_client = InfluxDBClient3(host=host, token=token, database=database, org=org)


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("Connected to MQTT Broker!")
    else:
        print(f"Failed to connect, return code {reason_code}")

    # From Machine
    client.subscribe(f"v3/{GroupID}@ttn/devices/{machine_id}/up")
    # From Machine_Data_Manager
    client.subscribe(f"{GroupID}/{machine_id}/Machine_Data_Manager")

# Callback ao receber mensagem MQTT
def on_message(client, userdata, msg):
    global active, last_message_time, sensor_data, sensor_timeout
    topic = msg.topic
    payload = msg.payload.decode()
    data = json.loads(payload)
    try:
        if "up" in topic:
            # Processamento dos dados dos sensores
            rpm = float(data["decoded_payload"]["rpm"])
            coolant_temp = float(data["decoded_payload"]["coolant_temperature"])
            oil_pressure = float(data["decoded_payload"]["oil_pressure"])
            battery_potential = float(data["decoded_payload"]["battery_potencial"])
            consumption = float(data["decoded_payload"]["consumption"])

            # Aqui processa-se os dados dos sensores recebidos da máquina e envia para machine_manager
            sensors = data["decoded_playload"]["rpm"] + " " + data["decoded_playload"]["coolant_temperature"] + " " + data["decoded_playload"]["oil_pressure"] + " " + data["decoded_playload"]["battery_potencial"] + " " + data["decoded_playload"]["consumption"] 
            client.publish(f"{GroupID}/{machine_id}/Data_Manager_Agent", sensors)

            # Aqui escreve-se os dados recebidos no influxDB
            p = Point("MyData") \
            .tag("machine_id", machine_id) \
            .field("rpm", rpm) \
            .field("coolant_temperature", coolant_temp) \
            .field("oil_pressure", oil_pressure) \
            .field("battery_potential", battery_potential) \
            .field("consumption", consumption) 
            
            write_client.write(p)

        elif "Machine_Data_Manager" in topic:
            # Aqui processa-se os dados recebidos do Data_Manager_Agent
            a=0

    except Exception as e:
        print(f"[ERRO] {e}")

broker = "broker.hivemq.com" #"test.mosquitto.org"
port = 1883
client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
#client.username_pw_set("srsa_sub", "srsa_password")
client.on_connect = on_connect
client.on_message = on_message
client.connect(broker, port)

try:
    client.loop_forever()
except KeyboardInterrupt:
    client.disconnect()
    print("\n[ALARME] Encerrado.")
