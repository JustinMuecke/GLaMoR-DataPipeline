from tokenize_modules import main


import os
import pika
import time
import psycopg2
import traceback
import json

rabbitmq_host ="rabbitmq"
queue_input = "filtering"
queue_output = "Modules_Modify"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "data_processing")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgress_password")

print("Connection to database...")
cursor = None
while cursor is None:
    try:
        # Connect to PostgreSQL
        db_connection = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = db_connection.cursor()
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        print("Retrying in 5 seconds...")
        time.sleep(5)

# Ensure the necessary table exists in PostgreSQL
cursor.execute("""
    CREATE TABLE IF NOT EXISTS data_filter (
        id SERIAL PRIMARY KEY,
        file_name VARCHAR(255),
        status VARCHAR(50),
        tokens VARCHAR(50), 
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")
db_connection.commit()
print("Connected to Database successfully")


def get_consistency(file_name):
    cursor.execute(
    "SELECT consistent FROM preprocessing WHERE file_name=%s", 
    (file_name.split(".")[0] + ".owl",)
    )
    result = cursor.fetchone()  # Fetch the first row of the result set
    if result is None:
        raise ValueError(f"No entry found for file_name: {file_name.split('.')[0] + '.owl'}")
    return result[0]  # Access the first column of the result

def on_message(channel, method, properties, body):
    try:
        file_name = body.decode()
        if(file_name in os.listdir("/output/")):
            cursor.execute("UPDATE data_filter SET status =%s WHERE file_name=%s", ("Done", file_name, ))
            db_connection.commit()
        else:
            cursor.execute("UPDATE data_filter SET status =%s WHERE file_name=%s", ("Processing", file_name))
            db_connection.commit() 
            token_length = main(file_name)
            cursor.execute("UPDATE data_filter SET status =%s, tokens=%s WHERE file_name=%s", ("Done", token_length, file_name))
            db_connection.commit() 
            if(token_length<500):
                with open("/input/" + file_name, "r") as input:
                    file = json.load(input)
                with open("/output/" + file_name, "w") as output: 
                    json.dump(file, output)
                if get_consistency(file_name):
                # Publish the processed message to the output queue
                    channel.basic_publish(exchange="", routing_key=queue_output, body=file_name.split(".")[0]+".owl")
                    print(f"Sent processed message to {queue_output}: {file_name}")
                cursor.execute("INSERT INTO modification (file_name, status) VALUES (%s, %s)", (file_name, "Waiting"))
                db_connection.commit()
            

        # Acknowledge the message to RabbitMQ
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except:
        traceback.print_exc()




def start_worker():
    """ Main worker function to consume messages """
    connection = None
    channel = None

    # Retry loop until RabbitMQ is available
    while connection is None:
        try:
            print("Attempting to connect to RabbitMQ...")
            credentials = pika.PlainCredentials("rabbitmq_user", "rabbitmq_password")

            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
            channel = connection.channel()

            # Declare the input queue to ensure it exists
            channel.queue_declare(queue=queue_input, durable=True)
            channel.queue_declare(queue=queue_output, durable=True)

            # Start consuming messages
            channel.basic_consume(queue=queue_input, on_message_callback=on_message)
            print(f"Waiting for messages in {queue_input}. To exit press CTRL+C")
            channel.start_consuming()

        except Exception as e:
            print(f"Connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)  # Wait 5 seconds before retrying
    

if __name__ == "__main__":
    start_worker()