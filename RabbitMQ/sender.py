try:
    import pika
except Exception as e:
    print('Some modules are missings {}'.format(e))


class MetaClass(type):
    _instance = {}

    def __call__(cls, *args, **kwargs):
        """ Singleton Design Pattern """
        if cls not in cls._instance:
            cls._instance[cls] = super(MetaClass, cls).__call__(*args, **kwargs)
            return cls._instance[cls]


class RabbitMqConfigure(metaclass=MetaClass):
    def __init__(self, queue='hello', host='localhost', routing_key='hello', exchange=''):
        self.host = host
        self.queue = queue
        self.routing_key = routing_key
        self.exchange = exchange


class RabbitMq:

    __slots__ = ["server", "_channel", "_connection"]

    def __init__(self, server: RabbitMqConfigure):
        self.server = server

        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.server.host))
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self.server.queue)

    def __enter__(self):
        print("__enter__")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("__exit__")
        self._connection.close()

    def publish(self, payload: dict) -> None:
        self._channel.basic_publish(exchange=self.server.exchange, routing_key=self.server.routing_key,
                                    body=str(payload))
        print('Published Message: {}'.format(payload))


if __name__ == '__main__':
    server = RabbitMqConfigure(queue='hello', host='localhost', routing_key='hello', exchange='')

    with RabbitMq(server) as rabbitmq:
        for i in range(200000):
            print(i)
            rabbitmq.publish(payload={'data': 'Hello World!'})
