# John Vivian
# 4-23-15

"""
Directed Acyclic Graph
to
jobTree
"""

import networkx as nx
import matplotlib.pyplot as plt
import random

def convert(G):
    """
    Converts DAG (in networkX) to Tree
    """
    assert nx.is_directed_acyclic_graph(G), 'Graph provided is not a "networkx" DAG.'

    #################################################
    # If number of source nodes is greater than 1:  #
    #   Create new node, S                          #
    #   For node in source_nodes:                   #
    #       (S, node)                               #
    #################################################

    source_nodes = [node for node in G.nodes() if not nx.ancestors(G,node)]
    if len(source_nodes) > 1:
        G.add_edges_from([(G, node) for node in source_nodes])

    

    return G

def main():

    # Describe test Graph
    G = nx.DiGraph()
    G.add_edges_from([(2,4),(3,4),(3,5)])

    plt.subplot(121)
    plt.title('start DAG')
    nx.draw_circular(G)

    convert(G)

    plt.subplot(122)
    plt.title('End')
    nx.draw_circular(G)
    plt.show()

    # http://stackoverflow.com/questions/20133479/how-to-draw-directed-graphs-using-networkx-in-python

if __name__ == '__main__':
    main()