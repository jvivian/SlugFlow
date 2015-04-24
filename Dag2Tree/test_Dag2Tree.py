import unittest
from Dag2Tree import *


class Dag2Tree(unittest.TestCase):

    def test_isDag(self):
        G = nx.DiGraph()
        G.add_edges_from([(1, 1)])
        self.assertRaises(AssertionError, convert, G)

    def test_isNxObject(self):
        G = 'potato'
        self.assertRaises(AttributeError, convert, G)

    def test_hasOneSourceNode(self):
        G = nx.DiGraph()
        G.add_edges_from([(2, 4), (3, 4), (3, 5)])
        G = convert(G)
        self.assertEqual(1, len([node for node in G.nodes() if not nx.ancestors(G, node)]))


def main():
    unittest.main()

if __name__ == '__main__':
    main()