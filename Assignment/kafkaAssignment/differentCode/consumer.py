import csv
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
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


def createCSVFromConsumerData(jsonData, w):
    rowList = []
    for col in jsonData:
        rowList.append(col)
    w.writerow(rowList)


def dict_to_order(data, ctx):
    # FILE_PATH = r"D:\Confluent-Kafka-Setup\restaurant_orders.csv"
    # df = pd.read_csv(FILE_PATH)
    # df = df.iloc[:, :]
    # for data in df.values:
    return data


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
json_deserializer = JSONDeserializer(mySchema.schema.schema_str, from_dict=dict_to_order)

consumer_conf = conf()
consumer_conf.update({
        'group.id': 'group1',
        'auto.offset.reset': "earliest"}
    )

consumer = Consumer(consumer_conf)
consumer.subscribe([topic])
with open('./output.csv', 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['order_number', 'order_date', 'item_name', 'quantity', 'product_price', 'total_products'])
    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            resData = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            if resData is not None:
                print("User record key {}: Actual order: {} from {} offset {}\n".format(
                    msg.key(), resData, msg.partition(), msg.offset()
                ))

            createCSVFromConsumerData(resData.values(), w)
        except KeyboardInterrupt:
            pass
        except ValueError:
            pass
    consumer.close()


