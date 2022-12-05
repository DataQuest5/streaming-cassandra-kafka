# parse arguments provided through CLI/Input
import argparse

# Cassandra connector
from cassandra.cluster import Cluster

parser = argparse.ArgumentParser(description='simple load generator for cassandra')
parser.add_argument('--hosts', default='127.0.0.1',
                    type=str,
                    help='comma separated list of hosts to use for contact points')
parser.add_argument('--port', default=9042, type=int, help='port to connect to')
parser.add_argument('--trans', default=1000000, type=int, help='number of transactions') 
parser.add_argument('--idfrom', default=50, type=int, help='start ids from') 
parser.add_argument('--inflight', default=25, type=int, help='number of operations in flight') 
parser.add_argument('--errors', default=-1, type=int, help='number of errors before stopping. default is unlimited') 
args = parser.parse_args()

hosts = args.hosts.split(",")
cluster = Cluster(hosts, port=args.port)

try:
    session = cluster.connect('testdb')
    print("prepare queries")
    insert = session.prepare("INSERT INTO transactions(id, account, transaction, day, hash) VALUES (?, ?, ?, ?, ?);")
    delete = session.prepare("DELETE from transactions WHERE id=? IF EXISTS;")
    row_lookup = session.prepare("SELECT * FROM transactions WHERE id = ?")

    threads = []
    ids = []
    error_counter = 0
    query = None
    params = []
    ids = []
    
    print("starting transactions")
    for i in range(args.idfrom, args.trans + args.idfrom):
        new_id = i
        ids.append(new_id)
        account = 'A{}'.format(i)
        transaction = 'T{}'.format(i)
        day = 'd{}'.format(i)
        hash_value = 'HV{}'.format(i)
        bound_insert = insert.bind([new_id, account, transaction, day, hash_value])
        bound_delete = delete.bind([new_id])
        # Insert new transaction to transactions table
        threads.append(session.execute_async(bound_insert))

        # Delete a transaction by ID
        # TODO: Uncomment if you want to make deletes
        # threads.append(session.execute_async(bound_delete))
        if i % args.inflight == 0:
            for t in threads:
                try:
                    t.result() #we don't care about result so toss it
                except Exception as e:
                    print("unexpected exception %s" % e)
                    if args.errors > 0:
                        error_counter = error_counter + 1
                        if error_counter > args.errors:
                            print("too many errors stopping. Consider raising --errors flag if this happens more quickly than you'd like")
                            break
            threads = []
            print("submitted %i of %i transactions" % (i, args.trans))
finally:
    cluster.shutdown()
