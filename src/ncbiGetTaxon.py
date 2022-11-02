import argparse
from ncbiBuildTree import Tree
import pickle

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Get taxonomic tree for ncbi taxon ID's")
    parser.add_argument('id', type=int)
    args = parser.parse_args()

    with open("../generatedFiles/taxonTree", "rb") as fp:
        tree = pickle.load(fp)

    for line in tree.getTaxonomy(args.id):
        print(line)