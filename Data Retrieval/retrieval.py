import requests
from tqdm import tqdm
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()
api_key = os.getenv("BioPortal_API")
base_url = "https://data.bioontology.org/ontologies"
output_path = Path(__file__).parent.parent / "data" / "ontologies"

# Make a GET request to retrieve all ontologies
response = requests.get(base_url, params={'apikey': api_key})

# Check if the request was successful
if response.status_code == 200:
    ontologies = response.json()
    for ontology in ontologies:
        print(ontology['name'], ontology['acronym'])
else:
    print(f"Error: {response.status_code}")

#for i in tqdm(range(0, len(ontologies))):
for i in tqdm(range(0, 3)):
    response = requests.get(ontology["links"]["download"], params={'apikey': api_key, "download_format" : "rdf"})
    if response.status_code == 200:
    # Save the OWL content to a file
        with open(output_path / f"{ontologies[i]['acronym']}.owl", "wb") as file:
            file.write(response.content)
        print(f"Ontology content saved as {ontologies[i]['acronym']}.owl")
    else:
        print(f"Couldn't Download ontology {ontologies[i]['acronym']}")
