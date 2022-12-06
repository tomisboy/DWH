# Thomas Alpert
import paho.mqtt.client as mqtt
import psycopg2 as db
import time

broker_address="broker.hivemq.com" # Setzte Broker URL
conn = db.connect("dbname='postgres' user='postgres' password='123456' host='localhost' port='5432'") ## Verbine DB

def on_message(client, userdata, message): ## Funktion on_message wartet auf Messages im Channel
    msg = (message.payload.decode("utf-8"))
    print("message received:" ,msg)
    print("message topic=", message.topic)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO staging.messung (payload) VALUES (%s)", [msg])
    conn.commit() # <- We MUST commit to reflect the inserted data
    cursor.close()


client = mqtt.Client("S1-62387462378642336748", clean_session=False)
client.on_message=on_message
client.connect(broker_address)


client.loop_start() #start the loop
client.subscribe("DataMgmt/FIN", qos=1)## Abonniere Channel "DataMgmt/FIN"
print("subscribed")
time.sleep(10000) # wait
client.loop_stop() #stop the loop
conn.close()