# Thomas Alpert
import paho.mqtt.client as mqtt
import time
import json
import random as rnd
from datetime import datetime

def create_json(fin,speed,location): ## Funktion zur Erstellung des passenden JSON-Objects
    json_object = json.dumps({"fin": fin ,"zeit": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4],"geschwindigkeit":speed ,"ort": location})
    print(json_object)
    return (json_object)

broker_address="broker.hivemq.com"
client = mqtt.Client("publisher-6363223764237", clean_session=False) #use your own unique ID
client.connect(broker_address)


for i in range(0,10): ## Erstelle 10 Datensätzte und Pubishe sie in den Channel "DataMgmt/FIN"
    client.publish("DataMgmt/FIN", create_json("SNTU411STM9032159",rnd.randint(1,200),"4"), qos=1)
    i = i-1
    time.sleep(1) # wait