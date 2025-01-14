"""
Author: Valentin Leuthe
Description: creates most important statistics about the community detection with Louvain 
considering the transaction-relationship
"""

from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import random
import string
import json
import matplotlib.pyplot as plt
import statistics
load_dotenv()

class Neo4jInstance:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def run_query(self,target_db ,query, parameters=None):
        with self.driver.session(database=target_db) as session:
            result = session.run(query, parameters)
            return [record for record in result]

    def test_connection(self,target_db):
        try:
            with self.driver.session(database=target_db)  as session:
                # Running a simple query to fetch the Neo4J version
                result = session.run("RETURN 'Connection successful!' as message")
                message = result.single()[0]
                return message
        except Exception as e:
            return f"An error occurred: {e}"
        


    def test_query_community_detection(self):
        print(self.test_connection('neo4j'))
        list_graphs = self.run_query('neo4j', "CALL gds.graph.list()")
        graph_names = [graph["graphName"] for graph in list_graphs]

        if 'accountGraph' not in graph_names:
            self.run_query('neo4j', """
            CALL gds.graph.project(
                'accountGraph',
                'Account',
                ['TRANSACTED']
            )
            """)

        result = self.run_query('neo4j', """
        CALL gds.louvain.stream('accountGraph')
        YIELD nodeId, communityId, intermediateCommunityIds
        RETURN gds.util.asNode(nodeId).address AS address, communityId
        ORDER BY communityId ASC
        """)
        return [{"accountAddress": record["address"], "communityId": record["communityId"]} for record in result]
    

    def test_query_community_detection_v2(self):
        print(self.test_connection('neo4j'))
        list_graphs = self.run_query('neo4j', "CALL gds.graph.list()")
        graph_names = [graph["graphName"] for graph in list_graphs]

        if 'accountGraph' not in graph_names:
            self.run_query('neo4j', """
            CALL gds.graph.project(
                'accountGraph',
                'Account',
                ['TRANSACTED']
            )
            """)

        result = self.run_query('neo4j', """
        CALL gds.louvain.stream('accountGraph')
        YIELD nodeId, communityId, intermediateCommunityIds
        RETURN gds.util.asNode(nodeId).address AS address, communityId
        ORDER BY communityId ASC
        """)

        community_dict = {}
        for record in result:
            address = record["address"]
            community_id = record["communityId"]
            if community_id not in community_dict:
                community_dict[community_id] = []
            community_dict[community_id].append(address)

        return community_dict
    
    
def main():
    # Load environment variables from .env file
    uri = os.getenv("DB_URL")
    username = os.getenv("DB_USR_NAME")
    password = os.getenv("DB_PWD")
    db = Neo4jInstance(uri, username, password)
    
    # Run community detection query
    result = db.test_query_community_detection_v2()
    print(f"There are a total of {len(result)} communities")
    count = 1
    # Print the first community and its addresses
    
    for community_id, addresses in result.items():
        print(f"Community ID: {community_id}")
        print("Addresses:", addresses)
        if count > 3:
            break
        count += 1
    
    # plot the community sizes
    # Calculate the size of each community
    community_sizes = [len(addresses) for addresses in result.values()] 
    # Filter the sizes to include only communities with size < 20
    filtered_community_sizes = [size for size in community_sizes if size < 20]

    # plot basic statistics for community sizes 
    print(f"Biggest community includes {max(community_sizes)} accounts")
    print(f"{community_sizes.count(1)} communities are of size one")
    print(f"{sum(1 for size in community_sizes if size > 10)} communities include more than 10 addresses")
    print(f"{sum(1 for size in community_sizes if size > 20)} communities include more than 20 addresses")
    print(f"{sum(1 for size in community_sizes if size > 50)} communities include more than 50 addresses")
    print(f"{sum(1 for size in community_sizes if size > 100)} communities include more than 100 addresses")
    print(f"The average community size is {statistics.mean(community_sizes)}")
    print(f"The median community size is {statistics.median(community_sizes)}")

    # Sort the community sizes in descending order
    sorted_sizes = sorted(community_sizes, reverse=True)
    # Take the sizes of the 20 largest communities
    top_20_sizes = sorted_sizes[:30]
    # Create a bar plot
    plt.figure(figsize=(10, 6))
    plt.bar(range(len(top_20_sizes)), top_20_sizes, color='skyblue')

    
    plt.xlabel('Community Index')
    plt.ylabel('Community Size (Number of Addresses)')
    plt.title('Sizes of the 30 Largest Communities')
    plt.yticks(range(0, max(top_20_sizes) + 250, 250))
    plt.xticks(range(len(top_20_sizes)), [i for i in range(1, len(top_20_sizes) + 1)])
    plt.grid(axis='y')
    plt.show()

    
    # Plot the histogram
    plt.figure(figsize=(10, 6))
    plt.hist(filtered_community_sizes, bins=range(1, max(filtered_community_sizes) + 1), edgecolor='black', align='left')

    plt.xlabel('Community Size (Number of Addresses)')
    plt.ylabel('Count of Communities')
    plt.title('Distribution of Community Sizes')
    plt.xticks(range(1, max(filtered_community_sizes) + 1))  # Set x-ticks to be integer community sizes
    plt.grid(axis='y')
    plt.show()
    
    
    db.close()

if __name__ == '__main__':
    main()