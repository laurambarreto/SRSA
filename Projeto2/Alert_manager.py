# Analisa o estado da máquina ao longo do tempo. 
# Se tem tido muitos valores criticos e se pode explodir a qualquer momento patrão!
# E envia por UDP (com muita rapidez e urgência) o valor de CRITICAL num JSON para o Data_Manager_Agent mandar a máquina parar


import paho.mqtt.client as mqtt
import time
import json
import socket

GroupID = "11"  
machine_id = "M1"
ClientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
host, port, message = None, None , None

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("Connected to MQTT Broker!")
    else:
        print(f"Failed to connect, return code {reason_code}")

    # From Machine_Data_Manager
    client.subscribe(f"{GroupID}/{machine_id}/Machine_Data_Manager")

# Callback ao receber mensagem MQTT
def on_message(client, userdata, msg):
    global active, last_message_time, sensor_data, sensor_timeout
    topic = msg.topic
    payload = msg.payload.decode()
    try:
       if "Machine_Data_Manager" in topic:
            data = json.loads(payload)
           
            # Enviar mensagem UDP para Data_Manager_Agent se for crítico para para a maquina
            ClientSocket.sendto(str.encode(message), (host,int(port)))

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
