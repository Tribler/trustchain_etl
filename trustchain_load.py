from py2neo import Graph

NEO4J_DB_QUERY = "UNWIND {v} as tx " \
                 "MERGE (a:Peer {key: tx.from}) " \
                 "MERGE (b:Peer {key: tx.to}) " \
                 "CREATE UNIQUE (a)-[lr:LAST_TX]->(b)" \
                 "SET lr.time=tx.time " \
                 "CREATE (tx_val:Transaction {amount:tx.amount, time:tx.time, block_id:tx.block_id}) " \
                 "CREATE (a)-[dr:DOWN {amount:tx.down}]->(tx_val) " \
                 "CREATE (b)-[ur:UP {amount:tx.up}]->(tx_val)"


class Neo4JDB(object):

    def __init__(self, url, user, password):
        self._graph = Graph(url, auth=(user, password))

    def push_batch(self, batch):
        """
        Push to Neo4j database batch of transactions in dict format
        :param batch: array of transactions in dictionary format
        :return: False if failed
        """
        return self._graph.run(NEO4J_DB_QUERY, v=batch)
