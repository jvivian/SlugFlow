# John Vivian
# 4-23-15

"""
Directed Acyclic Graph
to
jobTree

Stipulations:   All edges must have a "type" field.  Either "child" or "follow-on"
"""

import networkx as nx
import matplotlib.pyplot as plt
from itertools import chain, izip
import random

def cwl_to_dag(tool, job, wf=None):
    '''
    Converts CWL to a networkx DAG
    '''
    pass


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
                print 'Working on node with multiple parents: {}'.format(node)

                # Find the Most Recent Common Ancestor (MRCA) for the parents
                ancestors = [nx.topological_sort(G.subgraph(nx.ancestors(G, parent))) for parent in parents]
                temp_ancestor = ancestors[0]
                for ancestor in ancestors:
                    temp_ancestor = [i for i, j in izip(temp_ancestor, ancestor) if i == j]
                mrca = temp_ancestor[-1]

                # Determine Y -- Y = mrca unless it is connected to a follow-on, then Y is = chain of follow-ons
                Y = if_follow_on(G, mrca)
                print '\tnode: {}, parents: {}, mrca: {}, Y: {}'.format(node, parents, mrca, Y)

                # Create a child of Y, Z (pseudo-node)
                assert not G.has_node('Z{}'.format(pseudonode_count)), "Z{int} is a reserved naming scheme for nodes."
                G.add_node('Z{}'.format(pseudonode_count), pseudo=True)
                G.add_edge(Y, 'Z{}'.format(pseudonode_count), type='child')

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

    pseudonodes = [node for node in G.nodes() if 'pseudo' in G.node[node]]
    for pn in pseudonodes:

        if len(G.neighbors(pn)) == 1:
            # If pseudonodes only child is a follow-on
            if G.get_edge_data(pn, G.neighbors(pn)[0])['type'] == 'follow-on':
                # Collapse pseudonode into parent as a child edge
                G.add_edge( G.predecessors(pn)[0], G.neighbors(pn)[0], type='child')
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


def evaluate(G, node):
    '''
    Dynamically and recursively spawns jobTree targets
    '''

    #


def if_follow_on(G, mrca):
    if any(x['type'] == 'follow-on' for x in [G.get_edge_data(mrca, x) for x in G.neighbors(mrca)]):
        mrca_follow_on = [x for x in G.neighbors(mrca) if G.get_edge_data(mrca,x)['type'] == 'follow-on'][0]
        if not any(x['type'] == 'follow-on' for x in [G.get_edge_data(mrca_follow_on, x) for x in G.neighbors(mrca_follow_on)]):
            return mrca_follow_on
        else:
            if_follow_on(G, mrca_follow_on)
    else:
        return mrca

def main():

    # Describe test Graph
    G = nx.DiGraph()
    #G.add_edges_from([(1, 2), (2, 3), (2, 4), (2, 8), (3, 5), (4, 6), (5, 7), (6, 7), (8, 9), (9, 7)],None,type='child')
    #G.add_edges_from([(1, 2), (1, 8), (1, 9), (2, 3), (2, 5), (9, 5), (8, 6), (5, 6), (3, 4), (6, 7), (6, 4)],type='child')
    #G.add_edges_from([(1,2), (1,3), (2,4), (2,6), (3,6), (3,5), (4,7), (6,7), (5,7), (7,8)],type='child')
    G = nx.gnp_random_graph(15,0.25,directed=True)
    G = nx.DiGraph([(u,v,{'weight':random.randint(-10,10)}) for (u,v) in G.edges() if u<v], type='child')
    for edge in G.edges():
        G.edge[edge[0]][edge[1]]['type']='child'

    plt.subplot(121)
    plt.title('start DAG')
    nx.draw_networkx(G)

    convert(G)

    plt.subplot(122)
    plt.title('End Tree (post-collapse)')

    red_edges =  [edge for edge in G.edges() if G.get_edge_data(edge[0],edge[1])['type'] == 'follow-on']
    edge_colours = ['black' if not edge in red_edges else 'red' for edge in G.edges()]
    black_edges = [edge for edge in G.edges() if edge not in red_edges]

    pseudonodes = {node:node for node in G.nodes() if 'pseudo' in G.node[node]}
    node_labels = {node:node for node in G.nodes() if node not in pseudonodes}

    #pos = nx.spring_layout(G)
    #pos = nx.circular_layout(G)
    pos = nx.shell_layout(G)
    nx.draw_networkx_labels(G, pos, labels={node:node for node in G.nodes()})
    nx.draw_networkx_nodes(G, pos, nodelist=node_labels, node_color='r', alpha=0.8, node_size=500)
    nx.draw_networkx_nodes(G, pos, nodelist=pseudonodes, node_color='y', alpha=0.8, node_size=500)
    nx.draw_networkx_edges(G, pos, edgelist=red_edges, edge_color='r', arrows=True, alpha=0.5)
    nx.draw_networkx_edges(G, pos, edgelist=black_edges, arrows=True, alpha=0.5)
    plt.show()

if __name__ == '__main__':
    main()

"""
evaluate( node ):
    Do the task in the node
    for child in node.children:
        target.add_child( evaluate, child )
    for follow-on in node.follow-ons:
        target.add_follow-on( evaluate, follow-on )
"""