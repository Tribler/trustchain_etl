from sqlite3 import Error
import sqlite3

from pyipv8.ipv8.attestation.trustchain.block import TrustChainBlock


class SQLiteTrustchainExtractor(object):

    def create_connection(self):
        """ create a database connection to the SQLite database
            specified by the db_file
        :return: Connection object or None
        """
        try:
            conn = sqlite3.connect(self.db_dir)
            return conn
        except Error as _:
            # Print error
            return None

    def close_connection(self):
        """
        Close database connection
        """
        if self.conn:
            self.conn.close()

    def __init__(self, db_dir, init_offset=0):
        self.db_dir = db_dir
        self.conn = self.create_connection()
        self.offset = init_offset

    def is_connected(self):
        return self.conn is not None

    def sql_query(self, query):
        if self.conn is None:
            return None
        cur = self.conn.cursor()
        cur.execute(query)
        return cur.fetchall()

    def read_transactions(self, tr_count=50):
        """
        Fetch transactions from database ordered by timestamps
        :param tr_count: number of transaction to read
        :return:
        """
        if self.conn is None:
            # possibly throw exception
            return None
        else:
            cur = self.conn.cursor()
            cur = cur.execute("SELECT * FROM blocks "
                              "ORDER BY block_timestamp "
                              "LIMIT {},{}".format(self.offset, tr_count))
            if cur is None:
                return None
            self.offset += tr_count
            res = []
            for db_val in cur.fetchall():
                block = TrustChainBlock(db_val)
                if block.type == 'tribler_bandwidth':
                    res.append(block)
            return res

