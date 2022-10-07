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


#<queryAllItems>
print ("\nSelecting All")
rows = session.execute('SELECT * FROM uprofile.user')
PrintTable(rows)
#</queryAllItems>

#<queryByID>
# print ("\nSelecting Id=1")
# rows = session.execute('SELECT * FROM uprofile.user where user_id=1')
# PrintTable(rows)
#</queryByID>

cluster.shutdown()
