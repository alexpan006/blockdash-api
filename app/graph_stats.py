from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt
#import seaborn as sns
import statistics

load_dotenv()

uri = os.getenv("DB_URL")  
username = os.getenv("DB_USR_NAME")  
password = os.getenv("DB_PWD") 

print("uri", uri)
print("username", username)
print("password", password)

driver = GraphDatabase.driver(uri, auth=(username, password))

def test_connection():
    try:
        with driver.session(database="neo4j") as session:
            # Running a simple query to fetch the Neo4J version
            result = session.run("RETURN 'Connection successful!' as message")
            message = result.single()[0]
            return message
    except Exception as e:
        return f"An error occurred: {e}"

# Testing the connection
connection_status = test_connection()
print(connection_status)

def run_query(query, parameters=None):
    with driver.session(database="neo4j") as session:
        result = session.run(query, parameters)
        return result.single()[0]
        # return [record for record in result]


# ***** Total number of nodes ******************

# total number of Accounts
query = """
    MATCH (n:Account)
    RETURN COUNT(n) AS nodeCount
"""
total_accounts = run_query(query)
print("Total number of accounts:", total_accounts)

# total number of NFTs
query = """
    MATCH (n:NFT)
    RETURN COUNT(n) AS nodeCount
"""
total_nft = run_query(query)
print("Total number of NFTs:", total_nft)


# ***** number of nodes without relationships ********

# Number of account-nodes with no relationship 
query = """
    MATCH (n:Account)  
    WHERE NOT (n)-[]-()  
    RETURN COUNT(n) 
"""
account_no_relation = run_query(query)
print("Number of account nodes with no relation:", account_no_relation)

# Number of NFT-nodes with no relationship 
# How are the NFT-nodes named in Neo4J? 
query = """               
        MATCH (n:Nft)
        WHERE NOT (n)-[]-()  
        RETURN COUNT(n) 
        """
# Result should be 0 as every nood is supposed to have a mint relation
nft_no_relation = run_query(query)
print("Number of NFT nodes with no relationships:", nft_no_relation)


# ***** numer of nodes without certain relationship *********

# Number of accounts with no transaction-relationship 
query = """
        MATCH (n:Account)  
        WHERE NOT (n)-[:TRANSACTED]-()  
        RETURN COUNT(n) 
        """
no_transaction = run_query(query)
print("Number of accounts with no transaction-relationship:", no_transaction)

# Number of accounts with no owned-relationship 
query = """
        MATCH (n:Account)  
        WHERE NOT (n)-[:OWNED]-()  
        RETURN COUNT(n) 
        """
no_owned_account = run_query(query)
print("Number of accounts with no owned-relationship:", no_owned_account)

# Number of NFTs with no owned-relationship 
query = """
        MATCH (n:NFT)  
        WHERE NOT (n)-[:OWNED]-()  
        RETURN COUNT(n) 
        """
no_owned_nft = run_query(query)
print("Number of NFTs with no owned-relationship:", no_owned_nft)

# Number of accounts with no mint-relationship
query = """
        MATCH (n:Account)  
        WHERE NOT (n)-[:MINT]-()  
        RETURN COUNT(n) 
        """
no_mint_account = run_query(query)
print("Number of accounts with no mint-relationship:", no_mint_account)

# Number of NFT with no mint-relationship
query = """
        MATCH (n:NFT)  
        WHERE NOT (n)-[:MINT]-()  
        RETURN COUNT(n) 
        """
no_mint_nft = run_query(query)
print("Number of accounts with no mint-relationship:", no_mint_nft)


# ******* indegree and outdegree of transaction below threshold ********

# indegree + outdegree for some specific values 
for threshold in [2, 5, 10, 15, 20, 30, 40]:
    query = f"""
        MATCH (n:Account)  
        OPTIONAL MATCH (n)-[r:TRANSACTED]->()  
        WITH n, COUNT(r) AS outdegree  
        OPTIONAL MATCH (n)<-[r:TRANSACTED]-()  
        WITH n, outdegree, COUNT(r) AS indegree  
        WITH n, outdegree + indegree AS totalDegree  
        WHERE totalDegree < {threshold} 
        RETURN COUNT(n) AS nodesBelowThreshold  
    """
    degree_below_threshold = run_query(query)
    print(f"Number of accounts with indegree + outdegree less than {threshold}:", degree_below_threshold)

# maximum indegree + outdegree
query = """
    MATCH (n:Account)
    OPTIONAL MATCH (n)-[r:TRANSACTED]->()
    WITH n, COUNT(r) AS outdegree
    OPTIONAL MATCH (n)<-[r:TRANSACTED]-()
    WITH n, outdegree, COUNT(r) AS indegree
    WITH n, outdegree, indegree, (outdegree + indegree) AS totalDegree
    RETURN MAX(totalDegree) AS maxTotalDegree
    """
degree_max = run_query(query)
print("Maximum indegree+outdegree:", degree_max)

# ********** median and mean of indegree + outdegree **********
def fetch_node_degrees_for_median_mean(driver, query):
    with driver.session() as session:
        result = session.run(query)
        degrees = [record["totalDegree"] for record in result]
    return degrees

# for transaction relation 
query = """
    MATCH (n:Account)
    OPTIONAL MATCH (n)-[r:TRANSACTED]->()
    WITH n, COUNT(r) AS outdegree
    OPTIONAL MATCH (n)<-[r:TRANSACTED]-()
    WITH n, outdegree, COUNT(r) AS indegree
    RETURN (outdegree + indegree) AS totalDegree
    """
# Fetch degrees
total_degrees = fetch_node_degrees_for_median_mean(driver, query)
# Calculate median and mean
median_degree = statistics.median(total_degrees)
mean_degree = statistics.mean(total_degrees)
print("Median of total degrees (indegree+outdegree) for TRANSACTION relationship:", median_degree)
print("Mean of total degrees (indegree+outdegree) for TRANSACTION relationship:", mean_degree)

# for own relation of accounts
query = """
    MATCH (n:Account)
    OPTIONAL MATCH (n)-[r:OWNED]->()
    WITH n, COUNT(r) AS outdegree
    OPTIONAL MATCH (n)<-[r:OWNED]-()
    WITH n, outdegree, COUNT(r) AS indegree
    RETURN (outdegree + indegree) AS totalDegree
    """
# Fetch degrees
total_degrees = fetch_node_degrees_for_median_mean(driver, query)
# Calculate median and mean
median_degree = statistics.median(total_degrees)
mean_degree = statistics.mean(total_degrees)
print("Median of total degrees (indegree+outdegree) for OWNED relationship of Accounts:", median_degree)
print("Mean of total degrees (indegree+outdegree) for OWNED relationship of Accounts:", mean_degree)

# for own relation of NFTs
query = """
    MATCH (n:NFT)
    OPTIONAL MATCH (n)-[r:OWNED]->()
    WITH n, COUNT(r) AS outdegree
    OPTIONAL MATCH (n)<-[r:OWNED]-()
    WITH n, outdegree, COUNT(r) AS indegree
    RETURN (outdegree + indegree) AS totalDegree
    """
# Fetch degrees
total_degrees = fetch_node_degrees_for_median_mean(driver, query)
# Calculate median and mean
median_degree = statistics.median(total_degrees)
mean_degree = statistics.mean(total_degrees)
print("Median of total degrees (indegree+outdegree) for OWNED relationship of NFT:", median_degree)
print("Mean of total degrees (indegree+outdegree) for OWNED relationship of NFT:", mean_degree)


# plot histogram of indegree + outdegree distribution

# Function to fetch node degrees
def fetch_node_degrees(driver, query):
    with driver.session() as session:
        result = session.run(query)
        degrees = [(record["n"].id, record["totalDegree"]) for record in result]
    return degrees
# Cypher query to fetch in-degree and out-degree of transaction relations
query = """
    MATCH (n:Account)
    OPTIONAL MATCH (n)-[r:TRANSACTED]->()
    WITH n, COUNT(r) AS outdegree
    OPTIONAL MATCH (n)<-[r:TRANSACTED]-()
    WITH n, outdegree, COUNT(r) AS indegree
    RETURN n, outdegree, indegree, (outdegree + indegree) AS totalDegree
    """
# Fetch degrees
degrees = fetch_node_degrees(driver, query)
# Extract total degrees
total_degrees = [degree for _, degree in degrees]
# define the value-range for the plot 
filtered_degrees = [degree for degree in total_degrees if 0 <= degree <= 20]
# Plot the distribution
# sns.kdeplot(filtered_degrees, fill=True, color="skyblue")
plt.hist(filtered_degrees, bins=range(0, 21), color="skyblue", edgecolor="black")
plt.title('Distribution of Total Degrees (In-Degree + Out-Degree) for transaction-relation')
plt.xlabel('Total Degree')
plt.ylabel('Count')
plt.show()


# Cypher query to fetch in-degree and out-degree of own relations for accounts
query = """
    MATCH (n:Account)
    OPTIONAL MATCH (n)-[r:OWNED]->()
    WITH n, COUNT(r) AS outdegree
    OPTIONAL MATCH (n)<-[r:OWNED]-()
    WITH n, outdegree, COUNT(r) AS indegree
    RETURN n, outdegree, indegree, (outdegree + indegree) AS totalDegree
    """
# Fetch degrees
degrees = fetch_node_degrees(driver, query)
# Extract total degrees
total_degrees = [degree for _, degree in degrees]
# define the value-range for the plot 
filtered_degrees = [degree for degree in total_degrees if 0 <= degree <= 20]
# Plot the distribution
# sns.kdeplot(filtered_degrees, fill=True, color="skyblue")
plt.hist(filtered_degrees, bins=range(0, 21), color="skyblue", edgecolor="black")
plt.title('Distribution of Total Degrees (In-Degree + Out-Degree) for owned-relation for account nodes')
plt.xlabel('Total Degree')
plt.ylabel('Count')
plt.show()


# Cypher query to fetch in-degree and out-degree of own relations for NFTs
query = """
    MATCH (n:NFT)
    OPTIONAL MATCH (n)-[r:OWNED]->()
    WITH n, COUNT(r) AS outdegree
    OPTIONAL MATCH (n)<-[r:OWNED]-()
    WITH n, outdegree, COUNT(r) AS indegree
    RETURN n, outdegree, indegree, (outdegree + indegree) AS totalDegree
    """
# Fetch degrees
degrees = fetch_node_degrees(driver, query)
# Extract total degrees
total_degrees = [degree for _, degree in degrees]
# define the value-range for the plot 
filtered_degrees = [degree for degree in total_degrees if 0 <= degree <= 40]
# Plot the distribution
# sns.kdeplot(filtered_degrees, fill=True, color="skyblue")
plt.hist(filtered_degrees, bins=range(0, 40), color="skyblue", edgecolor="black")
plt.title('Distribution of Total Degrees (In-Degree + Out-Degree) for owned-relation for NFT nodes')
plt.xlabel('Total Degree')
plt.ylabel('Count')
plt.show()


# Don't forget to close the driver connection when you're done
driver.close()