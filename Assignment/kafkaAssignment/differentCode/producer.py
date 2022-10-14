from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import pandas as pd
from uuid import uuid4


# FILE_PATH = r"D:\Confluent-Kafka-Setup\restaurant_orders.csv"
columns = ['order_number', 'order_date', 'item_name', 'quantity', 'product_price', 'total_products']
topic = "order_data"

API_KEY = "TDRRFFG5YPAF6XP6"
ENDPOINT_SCHEMA_URL = "https://psrc-mw731.us-east-2.aws.confluent.cloud"
API_SECRET_KEY = 'RXnN4hrBpL6WcnFm2t3NtypfZvdkB1SDyi5zULfcDEGBIJswVTzT8tocRXrfBT6L'
BOOTSTRAP_SERVER = 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'W4RAO4FQG45EHDSG'
SCHEMA_REGISTRY_API_SECRET = 'zZ6kgpIZTyMXcm0PIca4c4ELbPUP25fRUfUWd9wZ4tDMZivYr8U1IfwEdkErXZ9I'


def conf():
    sasl_conf = {
        'sasl.mechanism': SSL_MACHENISM,
        'bootstrap.servers': BOOTSTRAP_SERVER,
        'security.protocol': SECURITY_PROTOCOL,
        'sasl.username': API_KEY,
        'sasl.password': API_SECRET_KEY
    }
    return sasl_conf


def schema_config():
    return {'url': ENDPOINT_SCHEMA_URL,
            'basic.auth.user.info': f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"
            }


# def dict_to_order(data, ctx):
    # FILE_PATH = r"D:\Confluent-Kafka-Setup\restaurant_orders.csv"
    # df = pd.read_csv(FILE_PATH)
    # df = df.iloc[:, :]
    # for data in df.values:
    # return data


def order_to_dict(data, ctx):
    # FILE_PATH = r"D:\Confluent-Kafka-Setup\restaurant_orders.csv"
    # df = pd.read_csv(FILE_PATH)
    # for data in df.values:
    #     orderToDict = dict(zip(columns, data))
    return data


def returnDict():
    FILE_PATH = r"D:\Confluent-Kafka-Setup\restaurant_orders.csv"
    df = pd.read_csv(FILE_PATH)
    df = df.iloc[:, :]
    ordersRestaurantData = []
    for data in df.values:
        resData = dict(zip(columns, data))
        ordersRestaurantData.append(resData)
        yield resData


def delivery_report(err, msg):
    if err is not None:
        print("An error has occur during the data producing for restaurant record: {}, {}".format(msg.key(), err))
        return
    print("User record {} has been successfully producing on {} topic's partition {} and offset {} ".format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


schema_registry_conf = schema_config()
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# getSchema from the SchemaRegistryClient
mySchema = schema_registry_client.get_latest_version("order_data-value")

# Serialize the keys
string_serializer = StringSerializer('utf_8')
# Serialize the json data
json_serializer = JSONSerializer(mySchema.schema.schema_str, schema_registry_client, order_to_dict)

producer = Producer(conf())
print("Producing user records to topic {}. ^C to exit.".format(topic))
producer.poll(0.0)

try:
    for restaurant_data in returnDict():
        producer.produce(topic=topic, key=string_serializer(str(uuid4()), order_to_dict),
                         value=json_serializer(restaurant_data, SerializationContext(topic, MessageField.VALUE)),
                         on_delivery=delivery_report)

except KeyboardInterrupt:
    pass
except ValueError:
    pass

print("\nFlushing records...")
producer.flush()
