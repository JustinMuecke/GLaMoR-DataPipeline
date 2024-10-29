import yaml
import json
from typing import List
from pathlib import Path



def translate_to_tripels(path) -> List[List[str]]:
     with open(path, "r") as file:   
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
    result = []
    for i in range(0,len(lines)):
        lines[i] = lines[i].strip().replace("\n", "")
        if "Prefix:" in lines[i]:
            continue
        if i > 0 and "and " in lines[i]:
            result[-1] = result[-1] + " " + lines[i]
            continue
        result.append(lines[i])
    
    return result

def _translate_to_triples(lines : List[str]) -> List[str]:
    """
    Translates a list of lines representing a OWL Ontology in Manchaster Syntax into a List of triples.
    Always checks the next two lines. If both lines are empty, the next Object starts in the file. 
    If only one line is empty, the following line starts a new property of the Object.
    """
    triples : List[List[str]] = []
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
    for i in range(len(triples)-1, -1, -1):
        [sub, relation, obj] = triples[i]
        if(relation == "Facts"):
            try:
                obj_parts = obj.split("  ")
                new_relation = obj_parts[0]
                new_obj = obj_parts[1]
            except: 
                print([sub, relation, obj])
                print(relation)
                raise IndexError("BOOOOB")
            triples[i] = [sub, new_relation, new_obj]
        if(relation == "DisjointWith"):
            if("," in obj):
                disjoints = obj.split(",")
                for disjoint in disjoints:
                    triples.insert(i+1, [sub, relation, disjoint.lstrip(" ")])
                triples.pop(i)
    return triples


if __name__ == "__main__":
    input_path = Path(__file__).parent.parent / "data" / "prefixless_modules"
    for path in (input_path.rglob("*")):
        try:
            triples : List[List[str]] = translate_to_tripels(path)
        except:
            print(path.name)
            continue
        with open("data/triples/"+path.name, "w") as output: 
            json.dump(triples, output)