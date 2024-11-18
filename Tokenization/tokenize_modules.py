import random
import logging
from pathlib import Path
import json
from classifier import GraphT5Classifier
from graph_t5.tokenization_t5_fast import T5TokenizerFast as T5Tokenizer
from wrapper_functions import Graph, graph_to_graphT5, graph_to_set_of_triplets, get_embedding, Data
import numpy as np
from get_arguments import add_args_shared
from get_arguments import add_args
from get_arguments import get_args
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from tqdm import tqdm
from torch import tensor




def data_to_dataT5(graph:Graph, tokenizer:T5Tokenizer, label:str, label_to_index:dict, graph_representation:str, eos:str):
    """
    :param graph: graph to convert
    :param tokenizer: tokenizer of model
    :param label: label of the relation
    :param label_to_index: mapping from label to index
    :param graph_representation: how to represent the graph. 
    :param eos: end-of-sequence token. Can be `False` for not using an eos token. When using an eos token, there are two ways to use it: `bidirectional` means that the eos token is connected to every other node in the graph, with a relative position of positive infinity (from node to eos) or negative infinity (from eos to node). `unidirectional` means that the eos token is connected to every node in the graph with a relative position of positive infinity (from node to eos), but not the other way around (i.e. no connection from eos to other node). This means, that nodes do not get messages from the eos token, which preserves locality when using the local GLM
    """
    if graph_representation == 'lGLM':
        data = graph_to_graphT5(graph, tokenizer, how='local', eos=eos)
    elif graph_representation == 'set':
        data = graph_to_set_of_triplets(graph, tokenizer, order='random')
    elif graph_representation == 'gGLM':
        data = graph_to_graphT5(graph, tokenizer, how='global', eos=eos)
    elif graph_representation == 'list':
        data = graph_to_set_of_triplets(graph, tokenizer, order='alphabetical')
    else:
        raise ValueError(f"unknown graph_representation {graph_representation}")
    data.label = tensor(label_to_index[label])
    return data


def load_data(kg, dataset_construction, radius, num_masked):
    #TODO Revert back to input multiple graphs

    splits = ['test']
    fn_graphs = [Path(f"data/knowledgegraph/test_data.jsonl")]
    fn_labels = [Path(f"data/knowledgegraph/test_labels.jsonl")]
    fn_label2index = Path(f"data/knowledgegraph/label2index.json")

    iterate = zip(splits, fn_graphs)
    for (split, fn) in iterate:
        triple_list = json.load(fn.open("r"))
        total_words = sum(len(word.split()) for sublist in triple_list for word in sublist)
        print(f"Number of Words: {total_words}")
        graphs = {split : [Graph(triple_list)]}
        



    #graphs = {split: [Graph(json.loads(l)) for l in tqdm(fn.open('r'))] for split, fn in zip(splits, fn_graphs)}

    labels = {split: fn.open('r').readlines() for split, fn in zip(splits, fn_labels)}
    for split in splits:
        labels[split] = [l.strip() for l in labels[split] if l.strip()]

    label_to_index = json.load(fn_label2index.open('r'))

    #TODO uncomment as soon as i have all data sets lol
    #assert set(labels['train']) == set(labels['dev']) == set(labels['test']), (set(labels['train']), set(labels['dev']), set(labels['test']))

    return graphs, labels, label_to_index


def main(file_name):

    parser = ArgumentParser(
        formatter_class=ArgumentDefaultsHelpFormatter  # makes wandb log the default values
    )
    add_args_shared(parser)
    add_args(parser)
    args = get_args(parser)

    if not args.device.startswith('cuda'):
        logging.warning(f'using CPU {args.device}, training might be slow.')
    else:
        logging.info(f'using GPU {args.device}')

    random.seed(args.seed)
    np.random.seed(args.seed)
    file_path = Path(f"/input/{file_name}")

# Open the file and load JSON content
    with file_path.open("r", encoding="utf-8") as f:
        triple_list = json.load(f)
    total_words = sum(len(word.split()) for sublist in triple_list for word in sublist)
    print(f"Number of Words: {total_words}")
    graph = Graph(triple_list)
    label = "Consistent"
    label_to_index = {"Inconsistent": 0, "Consistent": 1}

    logging.info('load T5 encoder')
    num_classes = 2
    
    model = GraphT5Classifier(config=GraphT5Classifier.get_config(num_classes=num_classes, modelsize=args.modelsize, num_additional_buckets=args.num_additional_buckets))
  
    data = data_to_dataT5(graph, model.tokenizer, label, label_to_index, args.graph_representation, eos=args.eos_usage)
    
    print(len(data.input_ids[0]))
    return len(data.input_ids[0])

    