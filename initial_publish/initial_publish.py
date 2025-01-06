import pika
import os
import time
import psycopg2


# Fetch environment variables
rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
queue_output_ontologies = "Ontologies"
queue_output_modules = "Modules_Preprocess"
queue_output_filter = "filtering"
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = "postgress_password"

print("GETTING DATABASE CONNECTION")
cursor = None
while cursor is None:
    try:
        print(POSTGRES_DB)
        print(POSTGRES_USER)

        print(POSTGRES_PASSWORD)
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

print("GOT DATABASE")
# Ensure the necessary table exists in PostgreSQL
cursor.execute("""
    CREATE TABLE IF NOT EXISTS modularization (
        id SERIAL PRIMARY KEY,
        file_name VARCHAR(255),
        status VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")
db_connection.commit()

def insert_into_modu_db(file_name):
    # Insert into PostgreSQL table
    cursor.execute("INSERT INTO modularization (file_name, status, cluster) VALUES (%s, %s, %s)", (file_name, "waiting", "TBD"))
    db_connection.commit()  # Commit the insert into the database
    # Acknowledge the message to RabbitMQ
    
def insert_into_preprocess_db(file_name):
    # Insert into PostgreSQL table
    cursor.execute("INSERT INTO preprocessing (file_name, status) VALUES (%s, %s)", (file_name, "waiting"))
    db_connection.commit()  # Commit the insert into the database
    # Acknowledge the message to RabbitMQ

def start_worker():
    """ Main worker function to consume messages """
    connection = None
    channel = None
    input_directory = ("/input")
    output_directory = ("/output")

    # Retry loop until RabbitMQ is available
    while connection is None:
        try:
            print("Attempting to connect to RabbitMQ...")
            credentials = pika.PlainCredentials("rabbitmq_user", "rabbitmq_password")
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
            channel = connection.channel()
            

        except Exception as e:
            print(f"Connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)  # Wait 5 seconds before retrying
    time.sleep(20)        
    done_processing = []
#    for filename in os.listdir(output_directory):
#        done_processing.append(filename.split("_Module")[0]+ ".owl")
#    done_processing = set(done_processing)    
#    print(done_processing)
    for filename in os.listdir(input_directory):
#        print(filename)
        file_path = os.path.join(input_directory, filename)
        
        if os.path.isfile(file_path):  # Ensure it's a file
            channel.basic_publish("", queue_output_filter, filename)  # Send to RabbitMQ queue
            insert_into_modu_db(filename)  # Insert into PostgreSQL
            print(f"Processed file: {filename}")


if __name__ == "__main__":
    start_worker()