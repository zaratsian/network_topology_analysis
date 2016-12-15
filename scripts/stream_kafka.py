import os,sys,re,csv
import datetime,time
import random
from kafka import KafkaProducer

try:
   data_file = sys.argv[1]
except:
   data_file   = '/traceroute_google_node_detail.txt'

#brokers     = ['seregion03.cloud.hortonworks.com:6667','seregion04.cloud.hortonworks.com:6667']
#brokers     = ['sandbox.hortonworks.com:6667']
brokers     = ['kafka.dev:9092']
topic       = 'dztopic1'

#producer = KafkaProducer(bootstrap_servers=brokers, value_serializer=lambda m: json.dumps(m).encode('ascii'))
producer = KafkaProducer(bootstrap_servers=brokers)

data   = open(data_file,'rb').read()
events = data.split('\n') 

for i,event in enumerate(events):
    if i != 0:
        time.sleep(0.5)
        signal_strength = 80.1
        signal_noise    = 10.1
        record = event + '|' + str(signal_strength) + '|' + str(signal_noise)    
        print str(record)
        producer.send(topic, record)

#ZEND
