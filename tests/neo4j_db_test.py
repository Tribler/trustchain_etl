import unittest

from py2neo import Graph, Relationship, Node
from py2neo.ogm import GraphObject, Property, RelatedTo

from pyipv8.ipv8.attestation.trustchain.block import TrustChainBlock


class Peer(GraphObject):
    __primarykey__ = "key"

    key = Property()

    last_down = RelatedTo("Peer")
    last_up = RelatedTo("Peer")

    def __init__(self, key):
        self.key = key

    def node(self):
        return self.__node__


class Transaction(GraphObject):
    __primarykey__ = "block_id"

    block_id = Property()
    time = Property()
    seq = Property()

    down_from = RelatedTo("Peer")
    down_to = RelatedTo("Peer")

    def __init__(self, b_id, timestamp, seq_n, uploader, downloader):
        self.block_id = b_id
        self.time = timestamp
        self.seq = seq_n
        self.down_from.add(uploader)
        self.down_to.add(downloader)


class Neo4JTest(unittest.TestCase):

    def setUp(self):
        url = "bolt://localhost:7687"
        user = "neo4j"
        password = "123"
        self._graph = Graph(url, auth=(user, password))
        self.db_query = "UNWIND {v} as tx " \
                        "MERGE (a:Peer {key: tx.from}) " \
                        "MERGE (b:Peer {key: tx.to}) " \
                        "CREATE UNIQUE (b)-[lr:LAST_UP]->(a)" \
                        "SET lr.time=tx.time " \
                        "CREATE UNIQUE (a)-[rr:LAST_DOWN]->(b)" \
                        "SET rr.time=tx.time " \
                        "CREATE (tx_val:Transaction {amount:tx.amount, time:tx.time, block_id:tx.block_id}) " \
                        "CREATE (a)-[dr:DOWN]->(tx_val) " \
                        "CREATE (b)-[ur:UP]->(tx_val)"

    def tearDown(self):
        self._graph = None

    def test_read_all_persons(self):
        # Read all data
        val = self._graph.run("MATCH (per:Peer) RETURN per").data()
        print(val)
        self.assertNotEqual(None, val)
        self.assertGreater(len(val), 0, "Peer list return zero")

    def test_add_peer(self):
        p1 = Peer("AMAML")
        p2 = Peer("BUBKB")
        tx_time = 190

        tx = Transaction("block3", tx_time, 2, p1, p2)
        p1.last_up.add(p2, properties={"time": tx_time})
        p2.last_down.add(p1, properties={"time": tx_time})
        self._graph.merge(p1, "Peer", "key")
        self._graph.merge(p2, "Peer", "key")
        self._graph.merge(tx, "Transaction", "block_id")

    def test_batch_add_tx(self):
        db_tx = self._graph.begin()

        for i in range(1000):
            p1 = Peer("k" + str(i))
            p2 = Peer("ko" + str(i))
            tx = Transaction("t_ra" + str(i), 1000 + i, i, p1, p2)
            db_tx.create(tx)
            tx_time = 10003 + i
            p1.last_up.add(p2, properties={"time": tx_time})
            p2.last_down.add(p1, properties={"time": tx_time})
            db_tx.merge(p1, "Peer", "key")
            db_tx.merge(p2, "Peer", "key")

        db_tx.commit()

    def test_batch_merge_subgraph(self):
        p1 = Node("Peer", key="fji23")
        p2 = Node("Peer", key="f1234")
        self._graph.merge(p1 | p2, "Peer", "key")
        subg = None
        for i in range(1000):
            tx = Node("Transaction", block_id="t_" + str(i), time=2000 + i)
            rel = Relationship(tx, "DOWN_FROM", p1)
            rel2 = Relationship(tx, "DOWN_TO", p2)
            if subg is None:
                subg = tx | rel | rel2
            else:
                subg = subg | tx | rel2 | rel
        self._graph.merge(subg, "Transaction", "block_id")

    def test_multi_trustchain_blocks(self):
        blocks = [TrustChainBlock() for _ in range(10)]
        for i in range(10):
            blocks[i].transaction = {'from': "q" + str(i), 'to': "qt" + str(i), "amount": 23, "time": i}
            blocks[i].sequence_number = i
        trustchain_query = "UNWIND {v} as tx " \
                           "MERGE (a:Peer {key: tx.transaction.from}) " \
                           "MERGE (b:Peer {key: tx.transaction.to}) " \
                           "CREATE UNIQUE (b)-[lr:LAST_UP]->(a)" \
                           "SET lr.time=tx.timestamp " \
                           "CREATE UNIQUE (a)-[rr:LAST_DOWN]->(b)" \
                           "SET rr.time=tx.timestamp " \
                           "CREATE (tx_val:Transaction {amount:tx.transaction.amount, " \
                           "time:tx.timestamp, block_id:tx.block_id}) " \
                           "CREATE (a)-[dr:DOWN]->(tx_val) " \
                           "CREATE (b)-[ur:UP]->(tx_val)"
        self._graph.run(trustchain_query, v=blocks)

    def test_cypher(self):
        txs = []
        for i in range(1000):
            tx = {'from': "p" + str(i), 'to': "pt" + str(i), "amount": 23, "time": i, "block_id": str("fe") + str(i)}
            txs.append(tx)
        self._graph.run(self.db_query,
                        v=txs)
