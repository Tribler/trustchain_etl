import argparse
from time import time

from trustchain_extract import SQLiteTrustchainExtractor
from trustchain_transform import TrustChainBlock2Dict
from trustchain_load import Neo4JDB

if __name__ == '__main__':

    parser = argparse.ArgumentParser("Migrate sqlite trustchain database to neo4j format")
    parser.add_argument("-s", "--sql_db", help="SQLite database path",
                        default="/Volumes/Elements/Databases/trustchain.db")
    parser.add_argument("-n", "--neo_db", help="Neo4j db address",
                        default="bolt://0.0.0.0:7687")
    parser.add_argument("--neo_user", help="Neo4j user", default="neo4j")
    parser.add_argument("--neo_pass", help="Neo4j pass", default="123")

    parser.add_argument("-o", '--offset', help="SQLite offset", type=int, default=0)
    parser.add_argument("-t", '--tx_count', help="Batch size to read from db", type=int, default=5000)
    args = parser.parse_args()

    DB_DIR = args.sql_db
    NEO_URL = args.neo_db
    NEO_USER = args.neo_user
    NEO_PASS = args.neo_pass
    OFFSET = args.offset
    TX_COUNT = args.tx_count

    # Internal stats for status
    begin = time()
    total_read = OFFSET
    total_pushed = 0

    sql_db = SQLiteTrustchainExtractor(DB_DIR, OFFSET)
    neo_db = Neo4JDB(NEO_URL, NEO_USER, NEO_PASS)
    if not sql_db.is_connected():
        print("Cannot open sqlite database @ " + str(DB_DIR))
        exit(1)

    if not neo_db.create_indexes():
        print("Cannot create index on neo database ")
        exit(1)

    while True:
        print("Reading transactions from sql database")
        sql_db_read = time()
        res = sql_db.read_transactions(TX_COUNT)
        if res is None:
            sql_db.close_connection()
            break
        print("Tribler blocks " + str(len(res)) + " / " + str(TX_COUNT))
        print("Time since experiment " + str(time() - begin))
        print("Time to read sql db " + str(time() - sql_db_read))
        print("Pushing to neo4j")
        neo_db_push = time()
        if not neo_db.push_batch(map(TrustChainBlock2Dict.transform, res)):
            sql_db.close_connection()
            print("Cannot push to neo4j database @ " + str(NEO_URL) + str((NEO_USER, NEO_PASS)))
            break
        total_read += TX_COUNT
        total_pushed += len(res)
        print("Time to push to neo db " + str(time() - neo_db_push))
        print("Total read: " + str(total_read))
        print("Total pushed: " + str(total_pushed))
        print("Time since experiment " + str(time() - begin))
        print("------------------------------")
