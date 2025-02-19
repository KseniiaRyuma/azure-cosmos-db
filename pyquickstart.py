from cassandra.auth import PlainTextAuthProvider
import config as cfg
from cassandra.query import BatchStatement, SimpleStatement
from prettytable import PrettyTable
import time
import ssl
import cassandra
from cassandra.cluster import Cluster
from cassandra.policies import *
from ssl import PROTOCOL_TLSv1_2, SSLContext, CERT_NONE
from requests.utils import DEFAULT_CA_BUNDLE_PATH
import csv

def PrintTable(rows):
    header = None
    data = []
    for r in rows:
        if header == None:
            print(r.__dict__)
            print(list(r.__dict__))
            header = list(r.__dict__)
        
        values = list(r)
        data.append(values)
    
    f = open('uprofile.csv', 'w')

    writer = csv.writer(f)

    # write the header
    writer.writerow(header)

    # write the raws from data
    for i in data:
        writer.writerow(i)
    

#<authenticateAndConnect>
ssl_context = SSLContext(PROTOCOL_TLSv1_2)
ssl_context.verify_mode = CERT_NONE
auth_provider = PlainTextAuthProvider(username=cfg.config['username'], password=cfg.config['password'])
cluster = Cluster([cfg.config['contactPoint']], port = cfg.config['port'], auth_provider=auth_provider,ssl_context=ssl_context)
session = cluster.connect()
#</authenticateAndConnect>

#<createKeyspace>
print ("\nCreating Keyspace")
session.execute('CREATE KEYSPACE IF NOT EXISTS uprofile WITH replication = {\'class\': \'NetworkTopologyStrategy\', \'datacenter\' : \'1\' }');
#</createKeyspace>

#<createTable>
print ("\nCreating Table")
session.execute('CREATE TABLE IF NOT EXISTS uprofile.user (user_id int PRIMARY KEY, user_name text, user_bcity text)');
#</createTable>

#<insertData>
session.execute("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (%s,%s,%s)", [1,'Lybkov','Seattle'])
session.execute("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (%s,%s,%s)", [2,'Doniv','Dubai'])
session.execute("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (%s,%s,%s)", [3,'Keviv','Chennai'])
session.execute("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (%s,%s,%s)", [4,'Ehtevs','Pune'])
session.execute("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (%s,%s,%s)", [5,'Dnivog','Belgaum'])
session.execute("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (%s,%s,%s)", [6,'Ateegk','Narewadi'])
session.execute("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (%s,%s,%s)", [7,'KannabbuS','Yamkanmardi'])
#</insertData>

#<queryAllItems>
print ("\nSelecting All")
rows = session.execute('SELECT * FROM uprofile.user')
PrintTable(rows)
#</queryAllItems>


cluster.shutdown()
