import threading
import time
import multiprocessing
import requests
import json
import psycopg2

from kafka import KafkaConsumer, KafkaProducer


class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')

        for raw_rsvp in self.stream_meetup_initial():
            try:
                rsvp = json.loads(raw_rsvp)
                line = str(rsvp['rsvp_id']) + ',' + rsvp['group']['group_country']
                producer.send('rsvp_country3', line.encode())
            except ValueError as e:
                continue

        producer.close()

    def stream_meetup_initial(self):
        uri = "http://stream.meetup.com/2/rsvps"
        response = requests.get(uri, stream=True)
        for chunk in response.iter_content(chunk_size=None):
            yield chunk


class Consumer(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
        consumer.subscribe(['rsvp_country3'])
        print('subscribed')
        conn = psycopg2.connect("dbname='meetups' user='chip' host='meetups.cdpprjjrjqmd.us-east-1.redshift.amazonaws.com' password='John316!' port=5439")
        print('connected')
        cur = conn.cursor()

        while not self.stop_event.is_set():
            print('starting')
            for message in consumer:
                print(str(message.value))
                try:
                    id, country = str(message.value).split(',')
                    query = "INSERT INTO meetup_country (id, country) VALUES ('" + id + "', '" + country + "')"
                    cur.execute(query)
                    conn.commit()
                    print('committed')
                    if self.stop_event.is_set():
                        break
                except ValueError as e:
                    print(e)

        consumer.close()
        conn.close()

def main():
    tasks = [
        Producer(),
        Consumer()
    ]

    for t in tasks:
        t.start()

    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()


if __name__ == "__main__":
    main()
