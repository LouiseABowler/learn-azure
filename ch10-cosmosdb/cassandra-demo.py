from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import config as cfg
from prettytable import PrettyTable
from ssl import PROTOCOL_TLSv1_2
from requests.utils import DEFAULT_CA_BUNDLE_PATH

# Utility function for printing the pizza table nicely
def PrintTable(rows):
    t = PrettyTable(["UserID", "Name", "Pizza"])
    for r in rows:
        t.add_row([r.user_id, r.user_name, r.user_favourite_pizza])
    print(t)


ssl_opts = {"ca_certs": DEFAULT_CA_BUNDLE_PATH,     # Default location of all root certificates
            "ssl_version": PROTOCOL_TLSv1_2
           }

auth_provider = PlainTextAuthProvider(username=cfg.config["username"], password=cfg.config["password"])

# Set up an instance of a Cassandra cluster by passing our Azure details in
# Don't need to list all nodes here, as the driver will discover further connected nodes
cluster = Cluster([cfg.config["contactPoint"]], port = cfg.config["port"], auth_provider=auth_provider, ssl_options=ssl_opts)

# The above command doesn't actually connect to a cluster - to do so, we need to set up a session
session = cluster.connect()

# Keyspaces contain tables, views etc.
# Have one keyspace per node - keyspaces are used to control data replication
print("Creating keyspace...")
session.execute('CREATE KEYSPACE IF NOT EXISTS userprofile WITH replication = {\'class\': \'NetworkTopologyStrategy\', \'datacenter\' : \'1\' }')

print("Creating table...")
# session.execute('CREATE TABLE IF NOT EXISTS uprofile.user (user_id int PRIMARY KEY, user_name text, user_bcity text)')
session.execute('CREATE TABLE IF NOT EXISTS userprofile.user (user_id int PRIMARY KEY, user_name text, user_favourite_pizza text)')


# Prepare the insert statement in advance - this should speed up execution as there is less network traffic and Cassandra will only have to parse the query once
# This doesn't seem to work - why?
# insert_data = session.prepare("INSERT INTO userprofile.user (user_id, user_name, user_favourite_pizza) VALUES (?, ?, ?)")

# But we can execute single statements just fine
session.execute("INSERT INTO userprofile.user (user_id, user_name, user_favourite_pizza) VALUES (1, 'Louise', 'Ham and mushroom')")

print("Selecting all...")
rows = session.execute('SELECT * FROM userprofile.user')
PrintTable(rows)
 
print("\nSelecting Id=1...")
rows = session.execute('SELECT * FROM userprofile.user where user_id=1')
PrintTable(rows)

cluster.shutdown()