from tokenize_modules import main


import os
import pika
import time
import psycopg2
import traceback
import json

rabbitmq_host = "rabbitmq"
queue_input = "filtering"
queue_output = "Modules_Modify"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "data_processing")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgress_password")

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

def get_consistency(cursor, file_name):
    try:
        cursor.execute(
            "SELECT consistent FROM preprocessing WHERE file_name=%s", 
            (file_name.split(".")[0] + ".owl",)
        )
        result = cursor.fetchone()
        if result is None:
            raise ValueError(f"No entry found for file_name: {file_name.split('.')[0] + '.owl'}")
        return result[0]
    except Exception as e:
        print(f"Error checking consistency: {e}")
        raise

def process_message(channel, cursor, db_connection, body):
    file_name = body.decode()
    try:
        if file_name in os.listdir("/output/"):
            cursor.execute("UPDATE data_filter SET status =%s WHERE file_name=%s", ("Done", file_name))
            db_connection.commit()
        else:
            cursor.execute("UPDATE data_filter SET status =%s WHERE file_name=%s", ("Processing", file_name))
            db_connection.commit()

            # Placeholder for your main logic to process the file
            token_length = main(file_name)  # Assuming main() is defined elsewhere

            cursor.execute("UPDATE data_filter SET status =%s, tokens=%s WHERE file_name=%s", ("Done", token_length, file_name))
            db_connection.commit()

            if token_length < 500:
                with open(f"/input/{file_name}", "r") as input_file:
                    file_content = json.load(input_file)

                with open(f"/output/{file_name}", "w") as output_file:
                    json.dump(file_content, output_file)

                if get_consistency(cursor, file_name):
                    channel.basic_publish(exchange="", routing_key=queue_output, body=file_name.split(".")[0] + ".owl")
                    print(f"Sent processed message to {queue_output}: {file_name}")

                cursor.execute("INSERT INTO modification (file_name, status) VALUES (%s, %s)", (file_name, "Waiting"))
                db_connection.commit()

        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error processing message: {e}")
        traceback.print_exc()
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def on_message(channel, method, properties, body):
    try:
        process_message(channel, cursor, db_connection, body)
    except Exception as e:
        print(f"Unhandled error in on_message: {e}")
        traceback.print_exc()
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def start_worker():
    connection = None
    channel = None

    while True:
        try:
            print("Attempting to connect to RabbitMQ...")
            credentials = pika.PlainCredentials("rabbitmq_user", "rabbitmq_password")
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
            channel = connection.channel()

            channel.queue_declare(queue=queue_input, durable=True)
            channel.queue_declare(queue=queue_output, durable=True)

            print(f"Waiting for messages in {queue_input}. To exit press CTRL+C")
            channel.basic_consume(queue=queue_input, on_message_callback=on_message)
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            print(f"RabbitMQ connection error: {e}. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"Unhandled error in start_worker: {e}")
            traceback.print_exc()
        finally:
            if connection:
                connection.close()
                print("Cleaned up RabbitMQ connection")

if __name__ == "__main__":
    db_connection = connect_to_database()
    cursor = db_connection.cursor()
    ensure_tables_exist(cursor)
    start_worker()
