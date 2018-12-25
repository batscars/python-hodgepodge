import pika
import time
import threading
import logging

logger = logging.getLogger("rabbitmq")
logger.setLevel(level=logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s  %(message)s',
                              '%Y-%m-%d %H:%M:%S')
handler = logging.FileHandler("rabbitmq.log")
handler.setFormatter(formatter)
handler.setLevel(logging.INFO)
logger.addHandler(handler)
console_handler = logging.StreamHandler()
console_handler.formatter = formatter
console_handler.setLevel(logging.DEBUG)
logger.addHandler(console_handler)
logger.addHandler(console_handler)


class Producer(object):
    def __init__(self, user, password, host, port, queue):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.queue = queue
        self.connection = self.connect()
        self.channel = self.get_channel()

    def connect(self):
        credentials = pika.PlainCredentials(self.user, self.password)
        parameters = pika.ConnectionParameters(self.host, self.port, '/', credentials)
        try:
            connection = pika.BlockingConnection(parameters)
        except Exception as e:
            logger.error('pika connection error:%s' % e)
            connection = None
        return connection

    def get_channel(self):
        channel = self.connection.channel()
        channel.queue_declare(queue=self.queue, durable=True)
        return channel

    def publish(self, data):
        self.channel.basic_publish(exchange='',
                                   routing_key=self.queue,
                                   body=data,
                                   properties=pika.BasicProperties(
                                       delivery_mode=2,
                                   ))

    def close(self):
        self.connection.close()


class Consumer(object):
    def __init__(self, user, password, host, port, queue, interval=50):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.queue = queue
        self.mutex = threading.Lock()
        self.interval = interval
        self.connection = self.connect()
        self.channel = self.get_channel()

    def heartbeat(self):
        time.sleep(self.interval)
        while 1:
            if self.mutex.acquire():
                try:
                    self.connection.process_data_events()
                    self.mutex.release()
                except Exception as e:
                    if isinstance(e, str):
                        logger.error('heart beat msg:' % e)
                    else:
                        logger.error(e)
                    self.mutex.release()
                    break

    def on_message(self, body, user_data=None):
        logger.info("current received body is {}, current received user data is {}".format(body, user_data))
        return True

    def connect(self):
        credentials = pika.PlainCredentials(self.user, self.password)
        parameters = pika.ConnectionParameters(self.host, self.port, '/', credentials)
        try:
            connection = pika.BlockingConnection(parameters)
        except Exception as e:
            logger.error('pika connection error:%s' % e)
            connection = None
        return connection

    def get_channel(self):
        channel = self.connection.channel()
        channel.queue_declare(queue=self.queue, durable=True)
        return channel

    def start(self):
        t = threading.Thread(target=self.heartbeat)
        t.start()
        method_frame = None
        if self.mutex.acquire():
            self.channel.basic_qos(prefetch_count=1)

            user_data = {
                "test_user_data": "test_user_data"
            }
            method_frame, header_frame, body = self.channel.basic_get(self.queue)
            logger.info('read from rabbitmq: %s' % body)
            count = 0
            while not body and count < 100:
                time.sleep(2)
                method_frame, header_frame, body = self.channel.basic_get(self.queue)
                logger.info('read from rabbitmq: %s' % body)
                count += 1
            self.mutex.release()
        if method_frame:
            ret = self.on_message(body, user_data=user_data)
            if ret is False:
                return False
            logger.debug("=====processing done======")

            if self.mutex.acquire():
                self.channel.basic_ack(method_frame.delivery_tag)
                logger.debug("=====sending ack done=====")
                self.connection.close()
                logger.debug("=====connection closed=====")
                self.mutex.release()