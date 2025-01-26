import os
import pika
import time
import psycopg2
import traceback
import json
from tokenize_modules import tokenizer_filter
from functools import partial

rabbitmq_host = "rabbitmq"
queue_input = "filtering"
queue_output = "Modules_Modify"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "data_processing")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgress_password")

INCONSISTENT_PREFIXES = ["AIO", "EID", "OIL", "OILWI", "OILWPI", "UE", "UEWI1", "UEWI2", "UEWPI", "UEWIP", "SOSINETO", "CSC", "OOR", "OOD"]

def connect_to_database():
    print("Connecting to database...")
    while True:
        try:
            db_connection = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            print("Connected to database successfully.")
            return db_connection
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)

def ensure_tables_exist(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS data_filter (
            id SERIAL PRIMARY KEY,
            file_name VARCHAR(255),
            status VARCHAR(50),
            tokens VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS modification (
            id SERIAL PRIMARY KEY,
            file_name VARCHAR(255),
            status VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.connection.commit()

def _set_file_precessing(file_name: str):
    cursor.execute("UPDATE data_filter SET status =%s WHERE file_name=%s", ("Processing", file_name))
    db_connection.commit()

def _set_file_done(file_name, token_length):
    cursor.execute(
        "UPDATE data_filter SET status =%s, tokens=%s WHERE file_name=%s",
        ("Done", token_length, file_name)
    )
    db_connection.commit()
    

def _check_if_file_is_already_processed(file_name : str) -> int:
    if file_name in os.listdir("/output500/") or file_name in os.listdir("/output500InC"): return 500
    if file_name in os.listdir("/output1000/")or file_name in os.listdir("/output1000InC"): return 1000
    if file_name in os.listdir("/output4000/")or file_name in os.listdir("/output4000InC"): return 4000
    return -1



def _publish_processed_file(folder : str, file_name : str, connection, channel):
    cursor.execute("UPDATE data_filter SET status=%s, tokens=%s WHERE file_name=%s", ("Done", folder,  file_name))
    db_connection.commit()

    if not channel.is_open:
        channel = connection.channel()
        channel.queue_declare(queue=queue_output)
    if _file_consistent(file_name):
        channel.basic_publish(exchange="", routing_key=queue_output, body = file_name.split(".")[0] + ".owl")
        print(f"Sent processed message to {queue_output}: {file_name}")
        cursor.execute(
            "INSERT INTO modification (file_name, status) VALUES (%s, %s)",
            (file_name, "Waiting")
        )
        db_connection.commit()



def _file_consistent(file_name : str) -> bool:
    beginning_of_word = file_name.split("_")[0]
    print(f"File starts with: {beginning_of_word}")
    print(f"Prefixes of inconsistent Ontologies: {INCONSISTENT_PREFIXES}")
    print(f"{file_name} is consistent: {beginning_of_word not in INCONSISTENT_PREFIXES}")
    return file_name.split("_")[0] not in INCONSISTENT_PREFIXES

def _save_file_to_disk(file_name, token_length):
    category : int = 10_000
    if token_length <= 4000: category = 4000
    if token_length <= 1000: category = 1000
    if token_length <= 500:  category = 500

    postfix = "InC"
    if _file_consistent(file_name): postfix ="" 
    with open(f"/input/{file_name}", "r") as input_file:
        file_content = json.load(input_file)
    with open(f"/output{category}{postfix}/{file_name}", "w") as output_file:
        json.dump(file_content, output_file)

    return category

def process_message(channel, cursor, db_connection, body, method, token_filter, connection, queue_output):
    file_name = body.decode()
    file_name = file_name.replace("'", "").strip()

    folder = _check_if_file_is_already_processed(file_name)
    if folder > 0:
        _publish_processed_file(str(folder), file_name, connection, channel)
        return
    try:
        _set_file_precessing(file_name)
        token_length = token_filter.main(file_name)
        _set_file_done(file_name, token_length)
        category = _save_file_to_disk(file_name, token_length)
        _publish_processed_file(str(category), file_name, connection, channel)

        if not channel.is_open:
           channel = connection.channel()
        channel.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Error processing message: {e}")
        traceback.print_exc()
        cursor.execute("UPDATE data_filter SET status =%s, tokens=%s, error_message=%s WHERE file_name=%s", ("Failed", "na", "Processing Failed", file_name))  
                                                                                                          
        if not channel.is_open:
            channel=connection.channel()
            channel.queue_declare(queue=queue_output)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def on_message(channel, method, properties, body, token_filter, connection, queue_output):
    try:
        process_message(channel, cursor, db_connection, body, method, token_filter, connection, queue_output)
    except Exception as e:
        print(f"Unhandled error in on_message: {e}")
        traceback.print_exc()
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def start_worker():
    print("Worker Started...")
    connection = None
    channel = None
    token_filter = tokenizer_filter()
    print("Got Tokenizer...")
    while True:
        try:
            print("Attempting to connect to RabbitMQ...")
            credentials = pika.PlainCredentials("rabbitmq_user", "rabbitmq_password")
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials, heartbeat=600))
            channel = connection.channel()

            channel.queue_declare(queue=queue_input, durable=True)
            channel.queue_declare(queue=queue_output, durable=True)

            print(f"Waiting for messages in {queue_input}. To exit press CTRL+C")
            on_message_with_filter = partial(on_message, token_filter=token_filter, connection=connection, queue_output=queue_output)
            channel.basic_consume(queue=queue_input, on_message_callback=on_message_with_filter)
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            print(f"RabbitMQ connection error: {e}. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"Unhandled error in start_worker: {e}")
            traceback.print_exc()

if __name__ == "__main__":
    print("Connecting to Database...")
    db_connection = connect_to_database()
    cursor = db_connection.cursor()
    print("Ensuring Tables Exist...")
    ensure_tables_exist(cursor)
    print("Starting Worker...")
    start_worker()
