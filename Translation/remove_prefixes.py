import yaml
import json
from typing import List
from pathlib import Path
import os

def _find_prefixes(lines: List[str]):
    prefixes = []
    for line in lines: 
        if "Prefix: " in line: 
            prefix = line.split(":")[1].lstrip(" ")+":"
            prefixes.append(prefix)
    prefixes.remove(":")
    return prefixes

def _remove_prefixes(input_path):
    with open(input_path, "r", encoding="utf8") as file:
        try:
            lines = file.readlines()
            modified_lines = []
            prefixes : List[str] = _find_prefixes(lines)
            print(prefixes)
            for line in lines: 
                for prefix in prefixes:
                    line = line.replace(prefix, "")
                modified_lines.append(line)
            with open("data/prefixless_modules/" + input_path.name, "w") as output:
                output.writelines(modified_lines)
        except:
            with open("log/prefix_removal", "a") as log:
                log.write(f"Couldn't remove prefixes in {input_path.name}\n")




if __name__ == "__main__":
    input_path = Path(__file__).parent.parent / "data" / "processed_modules"
    for i in (input_path.rglob("*")):
        _remove_prefixes(i)



