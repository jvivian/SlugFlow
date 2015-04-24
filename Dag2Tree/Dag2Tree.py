# John Vivian
# 4-23-15

"""
Directed Acyclic Graph
to
jobTree
"""

import networkx as nx
import matplotlib.pyplot as plt
from itertools import chain, izip


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
        assert not G.has_node('S'), 'Graph must not contain a node labelled "S". Reserved for Source Node.'
        G.add_edges_from([('S', node) for node in source_nodes])

    ##################
    # While there exists node with more than 1 parent:
    #   If node's parents have only 1 parent each:
    #       Find the MRCA
    #

    neighbors = list(chain.from_iterable([G.neighbors(node) for node in G.nodes()]))
    seen = set()
    multi_parents = set(x for x in neighbors if x in seen or seen.add(x))

    #while multi_parents:
    for node in multi_parents:
        # If parents of "node" have only one parent.
        parents = [parent for parent in G.predecessors(node)]
        if all(z==1 for z in [len(G.predecessors(x)) for x in parents]):
            # Find the Most Recent Common Ancestor (MRCA) for the parents
            ancestors = [nx.topological_sort( G.subgraph(nx.ancestors(G, parent))) for parent in parents]
            temp_ancestor = ancestors[0]
            for ancestor in ancestors:
                temp_ancestor = [i for i, j in izip(temp_ancestor, ancestor) if i == j]
            mrca = temp_ancestor[-1]
            print 'node: {}, parents: {}, mrca: {}'.format(node, parents, mrca)

    return G


def main():

    # Describe test Graph
    G = nx.DiGraph()
    #G.add_edges_from([(2, 4), (3, 4), (3, 5)], type='child')
    G.add_edges_from([(1, 2), (2, 3), (2, 4), (2, 8), (3, 5), (4, 6), (5, 7), (6, 7), (8, 9), (9, 7)], type='child')

    plt.subplot(121)
    plt.title('start DAG')
    nx.draw_networkx(G)

    convert(G)

    plt.subplot(122)
    plt.title('End')
    nx.draw_networkx(G)
    #plt.show()

    # http://stackoverflow.com/questions/20133479/how-to-draw-directed-graphs-using-networkx-in-python

if __name__ == '__main__':
    main()