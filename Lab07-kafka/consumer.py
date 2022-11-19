
from confluent_kafka import Consumer, TopicPartition
import redis
import json
import matplotlib.pyplot as plt
import time
from pluck import pluck

REDIS_HOST = '0.0.0.0'
REDIS_PORT = '6379'
r = redis.Redis(host=REDIS_HOST,port=REDIS_PORT, decode_responses=True)


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
            'bootstrap.servers': 'localhost:9092, localhost:9093, localhost:9094',
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
                msgs = self.consumer.consume(100, 1.0)                
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
                
                times = r.zrange("Time", n, n+99)
                n = n + 100
                data = []
                for t in times:
                    data.append(r.hgetall(t))

                print(data)
                temperatures = pluck(data,'Temperature')
                print(temperatures)
                # irradiances = events.pluck("GHI")
                # times = events.pluck("Date")
                plt.plot([i for i in range(1,len(temperatures)+1)], [float(i) for i in temperatures], color='red')
                # # plt.plot(irradiances, color='blue')
                plt.ylabel('Temperature')
                # 
                plt.show()
                time.sleep(5)



                print("==============================")

        except KeyboardInterrupt:
            print("interrupted error ")
        
        self.consumer.close()


capture = DataCapture()
capture.consume('test') 