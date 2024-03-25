from confluent_kafka import Producer, Consumer
import json

class Producer_custom(Producer):
    def send_message(self, topic, key, value):
        value = json.dumps(value)
        self.produce(topic, key=key, value=value)
        self.flush()


class Consumer_custom(Consumer):
    def get_message(self, timeout: int=1000) -> tuple:
        msg = self.poll(timeout=timeout)

        if msg is not None:
            msg_data = msg.value().decode('utf-8')
            if msg_data.find("Subscribed topic not available") == -1:
                return json.loads(msg_data), msg.topic()
        
        return None, None
 