#    Copyright (C) 2011-2012 by
#    Aric Hagberg <hagberg@lanl.gov>
#    Dan Schult <dschult@colgate.edu>
#    Pieter Swart <swart@lanl.gov>
#    All rights reserved.
#    BSD license.
__author__ = """Aric Hagberg <aric.hagberg@gmail.com>"""
import json
import networkx as nx
from networkx.readwrite import json_graph
import http_server
import Dag2Tree


G = nx.DiGraph()
G.add_edges_from([(1, 2), (1, 8), (1, 9), (2, 3), (2, 5), (9, 5), (8, 6), (5, 6), (3, 4), (6, 7), (6, 4)],type='child')

G = Dag2Tree.convert(G)

# this d3 example uses the name attribute for the mouse-hover value,
# so add a name to each node
for n in G:
    G.node[n]['name'] = n

red_edges =  [edge for edge in G.edges() if G.get_edge_data(edge[0],edge[1])['type'] == 'follow-on']
edge_colours = ['black' if not edge in red_edges else 'red' for edge in G.edges()]
black_edges = [edge for edge in G.edges() if edge not in red_edges]

pseudonodes = {node:node for node in G.nodes() if 'pseudo' in G.node[node]}
node_labels = {node:node for node in G.nodes() if node not in pseudonodes}

pos = nx.circular_layout(G)
nx.draw_networkx_labels(G, pos, labels=node_labels)
nx.draw_networkx_nodes(G, pos, nodelist=node_labels, node_color='r')
nx.draw_networkx_nodes(G, pos, nodelist=pseudonodes, node_color='k')
nx.draw_networkx_edges(G, pos, edgelist=red_edges, edge_color='r', arrows=True)
nx.draw_networkx_edges(G, pos, edgelist=black_edges, arrows=True)

# write json formatted data
d = json_graph.node_link_data(G) # node-link format to serialize
# write json
json.dump(d, open('force/force.json','w'))
print('Wrote node-link JSON data to force/force.json')
# open URL in running web browser
http_server.load_url('force/force.html')
print('Or copy all files in force/ to webserver and load force/force.html')