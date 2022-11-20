from confluent_kafka import Consumer, TopicPartition
import pandas as pd
import statsmodels.api as sm
import redis
import json
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import time
from pluck import pluck

N = 100
REDIS_HOST = '0.0.0.0'
REDIS_PORT = '6379'
r = redis.Redis(host=REDIS_HOST,port=REDIS_PORT, decode_responses=True)

plt.style.use('fivethirtyeight')

'''
    Topic: Weather
    partitions: 3
    fr: 2

    p0: d00,d01,d02(commit = True)
    p1: d10,d11(commit = True) 
    p2: d20,d21,d22(commit=True),d23(commit=True)

    d = poll(1seg) -> d11, d23, d22

'''

class DataCapture():
    def __init__(self) -> None:
        self.conf = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test7',     
            'enable.auto.commit': 'false',
            'auto.offset.reset': 'earliest',
            'max.poll.interval.ms': '500000',
            'session.timeout.ms': '120000',
            'request.timeout.ms': '120000'
        }

    def consume(self, topic='test'):
        self.consumer = Consumer(self.conf)
        self.topic = topic
        self.consumer.subscribe([self.topic])
        n = 0

        try:
            while True:
                # msg = self.consumer.poll(1.0) # consume(100, 1.0)
                msgs = self.consumer.consume(N, 1.0)                
                if msgs is None:
                    continue
                
                events = []
                for msg in msgs:
                    event = msg.value()
                    partition = msg.partition()
                    offset = msg.offset()
                    if event is not  None:
                        # process it
                        #print(event)
                        events.append(json.loads(event.decode('utf-8')))
                        r.hmset("Solargis."+events[-1]['Time'], events[-1])
                        r.zadd("Time", {"Solargis."+events[-1]['Time']: 0})

                    self.consumer.commit(offsets=[TopicPartition(topic = self.topic, partition=partition, offset=offset+1)], asynchronous = False)
                
                times = r.zrange("Time", n, n+N-1)
                n = n + N
                data = []
                if(len(times) >= N):
                    for t in times:
                        data.append(r.hgetall(t))

                # TS
                df = pd.DataFrame(data)
                print(df)
                df['Temperature'] = df['Temperature'].astype(float)                
                df['Time'] = pd.to_datetime(df['Time'].str[:10] + ' ' + df['Time'].str[11:])
                df = df.set_index('Time').groupby(pd.Grouper(freq='15min')).mean()
                df = df.dropna()

                #Arima
                modelo = sm.tsa.arima.ARIMA(df['Temperature'].iloc[1:],order=(10,1,0))
                resultados = modelo.fit()
                predict = resultados.forecast(steps=10).to_list()
                
                temperatures = df['Temperature'].to_list()
                plt.plot([i for i in range(1, N + 1)], temperatures, color='blue')
                plt.plot([i for i in range(N + 1, N + 11)], predict, color='green')

                plt.ylabel('Temperature')
                plt.xlabel("")
                plt.tight_layout()
                plt.pause(1)
                time.sleep(10)
                plt.clf()
                print("==============================")

        except KeyboardInterrupt:
            print("interrupted error ")
        
        self.consumer.close()


capture = DataCapture()
plt.show()
capture.consume()