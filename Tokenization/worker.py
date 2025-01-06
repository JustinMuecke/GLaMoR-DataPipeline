import os
from tokenize_modules import tokenizer_filter
import pandas as pd
from pathlib import Path
from tqdm import tqdm

INCONSISTENT_PREFIXES = ["AIO", "EID", "OIL", "OILWI", "OILWPI", "UE", "UEWI1", "UEWI2", "UEWPI", "UEWIP", "SOSINETO", "CSC", "OOR", "OOD"]

def _file_consistent(file_name : str) -> bool:
    return file_name.split("_")[0] not in INCONSISTENT_PREFIXES

def start_worker():
    dataset : pd.DataFrame = pd.DataFrame(columns=["file_name", "consistency", "tokenized_lenght", "body"])

    input_directory = "/input/"
    print("Worker Started...")
    token_filter = tokenizer_filter()
    print("Got Tokenizer...")

    print(os.curdir)
    print(os.listdir(input_directory))
    rows = []

    for file_name in tqdm(os.listdir(input_directory)):
        if file_name.startswith(".git"): continue
        (token_length, body) = token_filter.main(file_name)
        rows.append([file_name, _file_consistent(file_name), token_length, body])

    dataset = pd.DataFrame(rows, columns=["file_name", "consistency", "tokenized_length", "body"])
    print(dataset)


    
    dataset.to_csv(Path("/output/dataset.csv"), index=False)

    print("Finished Dataset Creation")


if __name__ == "__main__":
    start_worker()
    for i in range(1000000000):
        continue