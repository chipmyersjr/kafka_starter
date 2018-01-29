import threading
import time
import multiprocessing
import requests
import json

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
                producer.send('rsvp_country', raw_rsvp)
            except:
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
        consumer.subscribe(['rsvp_country'])

        while not self.stop_event.is_set():
            for message in consumer:
                rsvp = json.loads(message)
                line = str(rsvp['rsvp_id']) + ',' + rsvp['group']['group_country']
                with open('rsvp_country.csv', 'a') as file:
                    file.write(line)
                    file.write('\n')
                if self.stop_event.is_set():
                    break

        consumer.close()


def main():
    tasks = [
        Producer(),
        Consumer()
    ]

    for t in tasks:
        t.start()

    time.sleep(10000)

    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()


if __name__ == "__main__":
    main()
