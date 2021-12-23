from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from datetime import datetime
import sys, os, re, gzip

def main(input_dir, keyspace, table):

    #session
    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect(keyspace)
    rows = session.execute('SELECT path, bytes FROM nasalogs WHERE host=%s', [keyspace])

    #regex
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

    #batch objects
    batch = BatchStatement()
    insert_user = session.prepare("INSERT INTO nasalogs (host, bytes, datetime, path, uniq_id) VALUES (?, ?, ?, ?, UUID())")

    i = 1
    for f in os.listdir(input_dir):
        with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                return_arr = line_re.split(line)
                #executing batch every ~200 records
                if i%200 == 0:
                    session.execute(batch)
                    batch.clear()
                    i=1

                #filtering faulty records
                if len(return_arr) == 6:
                    host = return_arr[1]
                    dt = datetime.strptime(return_arr[2], '%d/%b/%Y:%H:%M:%S')
                    path = return_arr[3]
                    bytes = int(return_arr[4])

                    batch.add(insert_user, [host, bytes, dt, path])
                    i+=1

    #final batch execution
    session.execute(batch)

if __name__ == '__main__':
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    main(input_dir, keyspace, table)

