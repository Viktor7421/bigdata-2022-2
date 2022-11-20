from confluent_kafka import Consumer, TopicPartition
import pandas as pd
import statsmodels.api as sm
import xgboost as xgb
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

    def consume(self, i, topic='test'):
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

                #print(data)
                temperatures = pluck(data,'Temperature')
                #print(temperatures)
                # irradiances = events.pluck("GHI")
                # times = events.pluck("Date")

                # TS
                df = pd.DataFrame(data)
                df['Temperature'] = df['Temperature'].astype(float)                
                df['Time'] = pd.to_datetime(df['Time'].str[:10] + ' ' + df['Time'].str[11:])
                df = df.set_index('Time').groupby(pd.Grouper(freq='15min')).mean()
                df = df.dropna()
                
                #print(df)
                #Arima
                modelo = sm.tsa.arima.ARIMA(df['Temperature'].iloc[1:],order=(10,1,0))
                resultados = modelo.fit()
                forecast_ = resultados.forecast(steps=10)
                

                plt.cla()
                #plt.plot([i for i in range(1,len(df_t)+1)], [float(i) for i in df_t])
                plt.plot(df.index,df['Temperature'])
                plt.plot(forecast_,color='green')
                plt.legend()
                # # plt.plot(irradiances, color='blue')
                plt.ylabel('Temperature')
                plt.xlabel("")
                plt.tight_layout()

                plt.pause(5)
                time.sleep(5)

                print("==============================")

        except KeyboardInterrupt:
            print("interrupted error ")
        
        self.consumer.close()


capture = DataCapture()
ani = FuncAnimation(plt.gcf(), capture.consume)
plt.tight_layout()
plt.show()