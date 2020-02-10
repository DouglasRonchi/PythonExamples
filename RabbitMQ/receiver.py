try:
    import pika
    import ast
except Exception as e:
    print('Some modules are missings {}'.format(e))


class MetaClass(type):
    _instance = {}

    def __call__(cls, *args, **kwargs):
        """ Singleton Design Pattern """
        if cls not in cls._instance:
            cls._instance[cls] = super(MetaClass, cls).__call__(*args, **kwargs)
            return cls._instance[cls]


class RabbitMqServerConfigure(metaclass=MetaClass):
    def __init__(self, host='localhost', queue='hello'):
        self.host = host
        self.queue = queue


class RabbitMqServer():
    def __init__(self, server: RabbitMqServerConfigure):
        self.server = server
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.server.host))
        self._channel = self._connection.channel()
        self._tem = self._channel.queue_declare(queue=self.server.queue)
        print('Server started waiting for messages...')

    @staticmethod
    def callback(ch, method, properties, body) -> None:

        Payload = body.decode('utf-8')
        Payload = ast.literal_eval(Payload)
        print('Data Received: {}'.format(Payload))

    def startserver(self) -> None:
        self._channel.basic_consume(queue=self.server.queue, on_message_callback=RabbitMqServer.callback, auto_ack=True)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self._channel.start_consuming()


if __name__ == '__main__':
    serverconfigure = RabbitMqServerConfigure(host='localhost', queue='hello')
    server = RabbitMqServer(server=serverconfigure)
    server.startserver()
