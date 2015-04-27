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
    Converts networkx DAG to jobTree Tree
    """
    assert nx.is_directed_acyclic_graph(G), 'Graph provided is not a "networkx" DAG.'

    #################################################
    # I. Ensure there is only one source node       #
    #                                               #
    # If number of source nodes is greater than 1:  #
    #   Create new node, S                          #
    #   For node in source_nodes:                   #
    #       (S, node)                               #
    #################################################

    source_nodes = [node for node in G.nodes() if not nx.ancestors(G, node)]
    if len(source_nodes) > 1:
        assert not G.has_node('S'), 'Graph must not contain a node labelled "S". Reserved for Source Node.'
        G.add_edges_from([('S', node) for node in source_nodes],type='child')

    #############################################################################################################
    # II. Convert DAG to Tree by breaking nodes with more than one parent and generating pseudonodes            #
    #                                                                                                           #
    # While there exists a node with more than 1 parent:                                                        #
    #   If node's parents have only 1 parent each:                                                              #
    #       Find the MRCA: Y'                                                                                   #
    #       Find Y: the node on a path from Y' to X that is connected to Y' by a sequence of follow-on edges.   #
    #       Make new child of Y, Z                                                                              #
    #       Remove edges from (Y, incident nodes)                                                               #
    #       Remove edges from (Parents of node, node)                                                           #
    #       Add child edges from (Z, incident nodes)                                                            #
    #       Add follow-on edge from (Z, node)                                                                   #
    #############################################################################################################

    pseudonode_count = 0

    neighbors = chain.from_iterable([G.neighbors(node) for node in G.nodes()])
    seen = set()
    multi_parents = list(set(x for x in neighbors if x in seen or seen.add(x)))

    while multi_parents:

        for node in multi_parents:

            # If parents of "node" have each have only ONE parent.
            parents = [parent for parent in G.predecessors(node)]
            if sum(len(G.predecessors(x))-1 for x in parents) == 0:

                # Find the Most Recent Common Ancestor (MRCA) for the parents
                ancestors = [nx.topological_sort(G.subgraph(nx.ancestors(G, parent))) for parent in parents]
                temp_ancestor = ancestors[0]
                for ancestor in ancestors:
                    temp_ancestor = [i for i, j in izip(temp_ancestor, ancestor) if i == j]
                mrca = temp_ancestor[-1]

                # Determine Y -- Y = mrca unless it is connected to a follow-on, then Y is = chain of follow-ons
                Y = if_follow_on(G, mrca)
                print 'node: {}, parents: {}, mrca: {}, Y: {}'.format(node, parents, mrca, Y)

                # Create a child of Y, Z (pseudo-node)
                assert not G.has_node('Z{}'.format(pseudonode_count)), "Z{int} is a reserved naming scheme for nodes."
                G.add_edge(Y, 'Z{}'.format(pseudonode_count),type='child')
                nx.set_node_attributes(G,'Z{}'.format(pseudonode_count),'pseudonode')

                # Find first nodes on the path from Y to node
                incident_nodes = [path[1] for path in nx.all_simple_paths(G,Y,node)]

                # Remove these incident edges from Y DELETE(Y, incident node)
                G.remove_edges_from([(Y,n) for n in incident_nodes])

                # Attach these incident edges from Z to
                G.add_edges_from([('Z{}'.format(pseudonode_count),n) for n in incident_nodes],type='child')

                # Delete parent edges from node
                G.remove_edges_from([(n,node) for n in parents])

                # Add follow-on edge from Z to node
                G.add_edge('Z{}'.format(pseudonode_count), node, type='follow-on')

                # Remove node from pool of nodes with multiple parents
                multi_parents.remove(node)
                pseudonode_count += 1

    assert nx.is_tree(G), 'convert function failed to convert DAG to tree'

    #########################################################################
    # III. Collapse redundant pseudonodes                                   #
    #                                                                       #
    # If a pseudnode has only one follow-on and no children:                #
    #   Collapse pseudonode into parent, convert follow-on to child edge.   #                                                                   #
    # If a pseudnode has a parent whose only child is the pn:               #
    #    Collapse pseudonode into parent, retaining edge type.              #
    #########################################################################

    pseudonodes = [node for node in G.nodes() if nx.get_node_attributes(G,node)]
    for pn in pseudonodes:

        if len(G.neighbors(pn)) == 1:
            # If pseudonodes only child is a follow-on
            if G.get_edge_data(pn, G.neighbors(pn)[0])['type'] == 'follow-on':
                # Collapse pseudonode into parent as a child edge
                G.add_edge( G.predecessors(pn)[0], list(nx.descendants(G, pn))[0], type='child')
                G.remove_node(pn)

        # If a parent of a pseudonode has only one descendent
        elif len(G.neighbors(G.predecessors(pn)[0])) == 1:
            # If that descendent is a child edge
            if G.get_edge_data(G.predecessors(pn)[0], pn)['type'] == 'child':
                # Transfer all edges from pseudonode's descendents to parent.
                parent = G.predecessors(pn)[0]
                for child in G.neighbors(pn):
                    G.add_edge(parent, child, type=G.get_edge_data(pn, child)['type'] )
                G.remove_node(pn)

    return G

def if_follow_on(G, mrca):
    if any(x['type'] == 'follow-on' for x in [G.get_edge_data(mrca, x) for x in G.neighbors(mrca)]):
        print '\tmrca: {}, has a follow-on'.format(mrca)
        mrca_follow_on = [x for x in G.neighbors(mrca) if G.get_edge_data(mrca,x)['type'] == 'follow-on'][0]
        if not any(x['type'] == 'follow-on' for x in [G.get_edge_data(mrca_follow_on, x) for x in G.neighbors(mrca_follow_on)]):
            print '\tnew mrca: {}'.format(mrca_follow_on)
            return mrca_follow_on
        else:
            if_follow_on(G, mrca_follow_on)
    else:
        return mrca

def main():

    # Describe test Graph
    G = nx.DiGraph()
    #G.add_edges_from([(1, 2), (2, 3), (2, 4), (2, 8), (3, 5), (4, 6), (5, 7), (6, 7), (8, 9), (9, 7)],None,type='child')
    G.add_edges_from([(1, 2), (1, 8), (1, 9), (2, 3), (2, 5), (9, 5), (8, 6), (5, 6), (3, 4), (6, 7), (6, 4)],type='child')

    plt.subplot(121)
    plt.title('start DAG')
    nx.draw_networkx(G)

    convert(G)

    plt.subplot(122)
    plt.title('End Tree (post-collapse)')
    red_edges =  [edge for edge in G.edges() if G.get_edge_data(edge[0],edge[1])['type'] == 'follow-on']
    edge_colours = ['black' if not edge in red_edges else 'red' for edge in G.edges()]
    black_edges = [edge for edge in G.edges() if edge not in red_edges]
    node_labels = {node:node for node in G.nodes()};
    #pos = nx.spring_layout(G)
    pos = nx.circular_layout(G)
    nx.draw_networkx_labels(G, pos, labels=node_labels)
    nx.draw_networkx_nodes(G, pos, cmap=plt.get_cmap('jet'))
    nx.draw_networkx_edges(G, pos, edgelist=red_edges, edge_color='r', arrows=True)
    nx.draw_networkx_edges(G, pos, edgelist=black_edges, arrows=True)
    plt.show()

    # http://stackoverflow.com/questions/20133479/how-to-draw-directed-graphs-using-networkx-in-python

if __name__ == '__main__':
    main()