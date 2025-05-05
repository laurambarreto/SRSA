# Data_Manager_Agent envia os dados processados para um tópico que comece por {Group_id}
# Machine_Data_Manager subscreve-se nesse tópico e lê os dados para ver se estão dentro dos limites (que estão no ficheiro intervals.cfg)
# Por fim, envia mensagens de controlo para o mesmo tópico (i think) para corrigir esses valores anormais


import paho.mqtt.client as mqtt
import time
import json

GroupID = "11"  
machine_id = "M1"


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("Connected to MQTT Broker!")
    else:
        print(f"Failed to connect, return code {reason_code}")

    # From Data_Manager_Agent 
    client.subscribe(f"{GroupID}/{machine_id}/Data_Manager_Agent")

# Callback ao receber mensagem MQTT
def on_message(client, userdata, msg):
    global active, last_message_time, sensor_data, sensor_timeout
    topic = msg.topic
    payload = msg.payload.decode()
    data = payload.split(" ")
    try:
        if "Data_Manager_Agent" in topic:
            # Processamento dos dados recebidos do Data_Manager_Agent
            rpm = data[0]
            coolant_temp = data[1]
            oil_pressure = data[2]
            battery_potential = data[3]
            consumption = data[4]

            # verificar se estão dentro dos limites
            # se nao estiver, enviar mensagem de controlo para o topico {GroupID}/{machine_id}/Machine_Data_Manager

        
            
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
