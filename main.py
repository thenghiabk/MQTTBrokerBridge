import configparser
import logging
import os.path
import sys
import time

import paho.mqtt.client as mqtt

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class MqttForwarder:
    def __init__(self, config_path):
        self.config_path = config_path
        self.logger = logging.getLogger(__name__)

        self.src_client = None
        self.src_client_id = None
        self.src_broker = None
        self.src_port = None
        self.src_topic = None

        self.dest_client = None
        self.dest_client_id = None
        self.dest_broker = None
        self.dest_port = None
        self.dest_topic = None

    # Load config file
    def load_config(self):
        if not os.path.exists(self.config_path):
            self.logger.error(f"Config file not found at {self.config_path}")
            sys.exit(1)
        config = configparser.ConfigParser()
        config.read(self.config_path)

        # source config params
        self.src_broker = config.get('source', 'broker')
        self.src_port = config.get('source', 'port')
        self.src_client_id = config.get('source', 'client_id')
        self.src_topic = config.get('source', 'topic')

        # destination config params
        self.dest_broker = config.get('destination', 'broker')
        self.dest_port = config.get('destination', 'port')
        self.dest_client_id = config.get('destination', 'client_id')
        self.dest_topic = config.get('destination', 'topic')

        # Display config params for debugging
        self.logger.info(f"Subscriber's Params")
        self.logger.info(f"Broker: `{self.src_broker}` | Port: `{self.src_port}` | Client ID: `{self.src_client_id}` | Topic: `{self.src_topic}`")
        self.logger.info(f"Publisher's Params")
        self.logger.info(f"Broker: `{self.dest_broker}` | Port: `{self.dest_port}` | Client ID: `{self.dest_client_id}` | Topic: `{self.dest_topic}`")

    def on_src_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info(f"Connected to source MQTT Broker")
            self.src_client.subscribe(self.src_topic)
        else:
            self.logger.info(f"Failed to connect to source MQTT Broker, return code `{rc}`")
            sys.exit(1)

    def on_src_message(self, client, userdata, message):
        try:
            self.logger.info(f"Forwarding message `{message.payload.decode()} from `{message.topic}")
            self.dest_client.publish(self.dest_topic, message.payload)

        except Exception as e:
            self.logger.error(f"Failed to forward message: {e}")

    def on_dest_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info(f"Connected to destination MQTT Broker")
        else:
            self.logger.info(f"Failed to connect to destination MQTT Broker, return code `{rc}`")
            sys.exit(1)

    def run(self):
        # Get config values
        self.load_config()

        # Set up source broker connection
        self.src_client = mqtt.Client(self.src_client_id)
        self.src_client.on_connect = self.on_src_connect
        self.src_client.on_message = self.on_src_message
        self.src_client.connect(self.src_broker, int(self.src_port))

        # Set up target broker connection
        self.dest_client = mqtt.Client(self.dest_client_id)
        self.dest_client.on_connect = self.on_dest_connect
        self.dest_client.connect(self.dest_broker, int(self.src_port))

        # Start clients
        self.src_client.loop_start()
        self.dest_client.loop_start()

        try:
            while True:
                # time.sleep(1)
                pass
        except KeyboardInterrupt:
            self.logger.info("Stopping...")
            self.src_client.loop_stop()
            self.dest_client.loop_stop()


if __name__ == '__main__':
    forwarder = MqttForwarder('config.ini')
    forwarder.run()
