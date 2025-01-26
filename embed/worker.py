from typing import List
import os
import pika
import time
import traceback
from owl2vec_star import owl2vec_star
import os
import numpy as np
import traceback
from typing import List, Dict

rabbitmq_host ="rabbitmq"
queue_input = "embed"

PREFIXES : List[str] = ["AIO","EID","OIL","OILWI","OILWPI","UE","UEWI1","UEWI2","UEWPI","UEWIP","SOSINETO","CSC","OOR","OOD"]

def _create_graph_embedding(embeddings):
    ''' 
    Given the KeyedVector Embeddings of all words in an ontology, 
    returns the average of the vectors as graph embeddings
    '''
    words = embeddings.key_to_index
    vectors = [embeddings[word] for word in words]
    graph_embedding = np.mean(vectors, axis=0)
    return graph_embedding
# %%

def process_file(filename):
    """Processes a single file to extract its graph embedding."""
    directory_path = "/input_inconsistent/" if filename.split("_")[0] in PREFIXES else "/input_consistent/"
    try:
        name = filename.split(".")[0] + ".owl"

        gensim_model = owl2vec_star.extract_owl2vec_model(
            f"{directory_path}{name}", "./default.cfg", True, True, True
        )
        graph_embedding = _create_graph_embedding(gensim_model.wv)
        return filename, graph_embedding
    except Exception as exception:
        traceback.print_exc()
        return None

def on_message(channel, method, properties, body):
    try:
        file_name = body.decode()
        if(file_name in os.listdir("/output/")): return
        embedding = process_file(file_name)

        with open(f"/output/{file_name}", "w") as f:
            f.write(str(embedding))

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
            # Start consuming messages
            channel.basic_consume(queue=queue_input, on_message_callback=on_message)
            print(f"Waiting for messages in {queue_input}. To exit press CTRL+C")
            channel.start_consuming()

        except Exception as e:
            print(f"Connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)  # Wait 5 seconds before retrying
    

if __name__ == "__main__":
    start_worker()