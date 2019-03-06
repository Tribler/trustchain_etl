from py2neo import Graph

NEO4J_DB_QUERY = 'UNWIND {v} as tx ' \
                 'MERGE (a:Peer {key: tx.from, full_key:tx.full_from}) ' \
                 'ON CREATE SET a.create_time=tx.time, a.txs=1, a.balance=tx.amount ' \
                 'ON MATCH SET a.txs=a.txs+1, a.balance=a.balance+tx.amount, a.create_time= ' \
                 ' CASE ' \
                 '    WHEN EXISTS(a.create_time) THEN a.create_time' \
                 '    ELSE tx.time' \
                 ' END ' \
                 'MERGE (b:Peer {key: tx.to, full_key:tx.full_to}) ' \
                 'ON CREATE SET b.ask_time=tx.time, b.txs=0, b.balance=0 ' \
                 'CREATE UNIQUE (a)-[lr:LAST_TX]->(b)' \
                 'SET lr.time=tx.time ' \
                 'CREATE (tx_val:Transaction {down:tx.down, ' \
                 'up:tx.up, time:tx.time, ' \
                 'from:tx.full_from, to:tx.full_to, ' \
                 'seq_n:tx.seq_n, lseq_n:tx.lseq_n}) ' \
                 'CREATE (a)-[:OUT_TX]->(tx_val) ' \
                 'CREATE (b)<-[:IN_TX]-(tx_val)'


class Neo4JDB(object):

    def __init__(self, url, user, password):
        self._graph = Graph(url, auth=(user, password))

    def create_indexes(self):
        return self._graph.run("CREATE INDEX ON :Peer(key)") \
               and self._graph.run("CREATE INDEX ON :Peer(full_key)") \
               and self._graph.run("CREATE INDEX ON :Transaction(time)") \
               and self._graph.run("CREATE INDEX ON :Transaction(from)") \
               and self._graph.run("CREATE INDEX ON :Transaction(to)") \
               and self._graph.run("CREATE INDEX ON :Transaction(seq_n)") \
               and self._graph.run("CREATE INDEX ON :Transaction(lseq_n)")

    def push_batch(self, batch):
        """
        Push to Neo4j database batch of transactions in dict format
        :param batch: array of transactions in dictionary format
        :return: False if failed
        """
        return self._graph.run(NEO4J_DB_QUERY, v=batch)
