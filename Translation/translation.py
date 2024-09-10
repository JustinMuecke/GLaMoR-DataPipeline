import yaml
import json
from typing import List
from pathlib import Path



def translate_to_tripels(file) -> List[List[str]]:

    lines : List[str] = file.readlines()
    lines = _concatinate_and_revome(lines)
    triples = _translate_to_triples(lines)
    triples = _clean_triples(triples)
    return triples

    

def _concatinate_and_revome(lines : List[str]) -> List[str]:
    """
    Given a List of lines representing a OWL Ontology in Manchaster syntax,
    removes any occurance of a "Prefix:xxx" definition, and if two consecutive lines are logically connected by and, 
    merges them into one line
    """
    remove = []
    for i in range(0,len(lines)):
        lines[i] = lines[i].strip().replace("\n", "")
        if "and " in lines[i]:
            lines[i-1] = lines[i-1] + " " + lines[i]
            remove.append(i)
        if "Prefix:" in lines[i]:
            remove.append(i)

    for index in remove: 
        lines.pop(index)
    return lines

def _translate_to_triples(lines : List[str]) -> List[str]:
    """
    Translates a list of lines representing a OWL Ontology in Manchaster Syntax into a List of triples.
    Always checks the next two lines. If both lines are empty, the next Object starts in the file. 
    If only one line is empty, the following line starts a new property of the Object.
    """
    line = lines[0]
    for i in range(1, len(lines)):

        line2 = lines[i]
        # Both Lines are Empty: New Object starts at the next Line
        if(line == "" and line2 == ""):
            line = lines[i]
            continue
        # Only one line Empty: Skip
        if(line == ""):
            line = lines[i]
            continue
        
        # If line contains ":": new Object gets defined
        tuple = line.split(":")
        if(":" in line):
            if(tuple[1] != ""):
                sub = tuple[0]
                obj = tuple[1]
                relation = "is"
        #If Line does contains ":" but no subject, new relation gets defined
            else: 
                relation = tuple[0].strip()
                line = lines[i]
                continue
        #If Line does not contain ":": new Subject gets defined
        else:
            sub = line.strip()        
        line = lines[i].replace("\n", "").strip()
        triples.append([obj, relation, sub])
        return triples

def _clean_triples(triples : List[List[str]]) -> List[List[str]]:
    """ 
    Given a list of triples, if a triple contains the relation "Facts", the actual relation is part of the subject. 
    E.g.  ["Person1", "Facts", "hasParent Person2"] -> ["Person1" "hasParent", "Person2"]
    """
    for i in range(len(triples)):
        [sub, relation, obj] = triples[i]
        if(relation == "Facts"):
            tuple = obj.split("  ")
            relation = tuple[0]
            obj = tuple[1]
        triples[i] = [sub, relation, obj]
    return triples


if __name__ == "__main__":
    input_path = Path(__file__).parent.parent / "data" / "ont_modules"

    ##For file in inputpath: translate_to_triples

    #triples : List[List[str]] = translate_to_tripels(input_path)
    #with open("data.jsonl", "w") as output: 
    #    json.dump(triples, output)