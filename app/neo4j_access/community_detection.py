from dotenv import load_dotenv
from app.db import get_db
from typing import List
from app.cache import delete_cache_keys
import json
import logging
load_dotenv()

class CommunityDetection:
    """
    The class includes the logic running the community detection. 

    Methods:
    --------
    - run_community_detection(self,limit:int,collection_name:str)
        handle projected graph and call method running the community detection.
    - get_processed_collection_name(self,collection_name:str)
        return processed collection name.
    - get_all_node_properties(self,node_ids, property_name)
        retrieve the values of nodes given the name of the key.
    - update_node_properties(self, node_updates, property_name)
        method to convert the scope that it suits the database.
    - update_nodes_com_property(self,collection_name,communityId,nodes,scope)
        update nodes what community they belong to.
    - update_project_graph(self,collection_name)
        update projected graph in neo4j.
    - update_project_graph_complete(self)
        update projected graph in neo4j.(specialized for combined collection)
    - update_community_id_info(self,idList,collection_name,scope)
        update community id info nodes.
    - update_community_detection(self,limit,collection_name)
        run the community detection in 3 different scopes.
    - build_community_info_node(self)
        create the community id info node.
    - get_transactions(self, all_node_ids: List[str])
        return all transaction relationship between given nodes.
    - get_ownerships(self, all_node_ids: List[str])
        return all ownership relationship between given nodes.
    - are_relationship_nodes_in_communities(self, final_result: dict)
        check the final result if there are nodes not in any community.
        
    Author
    ------
    Kai-Yan Pan
    """

    def __init__(self):
        self.db =get_db()
        
        
    def run_community_detection(self,limit:int,collection_name:str):
        """
        Given the collection name, firstly project all related nodes into desired format,
        then run the the Louvain community detection.

        Parameters:
        - collection_name: str
            Run the algorithm on nodes related to the collection name.
        """

        logging.info(f"Running community detection, Collection Name:{collection_name},Limit:{limit}")
        try:
            if collection_name != "complete":
                self.update_project_graph(collection_name)
            else:
                self.update_project_graph_complete()

            self.update_community_detection(limit,collection_name)
            delete_cache_keys("application-cache:community:*")
            return "Update Completed"
        
        except Exception as err:
            logging.error(err)
            return "Something went wrong QAQ"
        

    def get_processed_collection_name(self,collection_name:str):
        """
        Given the collection name, return processed collection.
        Since in neo4j db, the "-" is now allowed in property naming.

        Parameters:
        - collection_name: str
            collection name to be processed
        """

        if collection_name == 'degods-eth':
            return 'degods'
        elif collection_name == 'boredapeyachtclub':
            return 'boredapeyachtclub'
        elif collection_name =='complete':
            return 'complete'
        else:
            return collection_name
        
    def get_all_node_properties(self,node_ids, property_name):
        """
        Get the property value of nodes given a list of nodeId and key of property .

        Parameters:
        - node_ids: List[int]
            List of nodes ID to be queried.
        - property_name: str
            Name of the property to be queried.
        """

        query = f"""
        MATCH (n)
        WHERE (n:Account OR n:NFT) AND id(n) IN {node_ids}
        RETURN id(n) AS node_id, n.{property_name} AS property_value
        """
        result = self.db.run_query('neo4j',query)
        return {record['node_id']: record['property_value'] for record in result}  
      
    def update_node_properties(self, node_updates, property_name):
        """
        Update the property value of nodes given a dictionary of {node ID: new value} .

        Parameters:
        - node_updates: dict[ str : List[int] ]
            Dictionary of nodes id and their new property value.
        - property_name: str
            Name of the property to be updated.
        """
        query = f"""
            UNWIND $updates AS update
            MATCH (n)
            WHERE id(n) = update.node_id
            SET n.{property_name} = update.property_value
            RETURN n
            """        
        self.db.run_query('neo4j',query,parameters={"updates": node_updates})
        
    def update_nodes_com_property(self,collection_name,communityId,nodes,scope):
        """
        Update the property value of nodes given a dictionary of {node ID: new value} .

        Parameters:
        - node_updates: dict[ str : List[int] ]
            Dictionary of nodes id and their new property value.
        - property_name: str
            Name of the property to be updated.
        """
        node_ids = [node.id for node in nodes]
        property_name = f"{self.get_processed_collection_name(collection_name)}_com_id_list"
        original_values = self.get_all_node_properties(node_ids, property_name)
        node_updates = []
        for node in nodes:
            original_value = original_values.get(node.id)
            # if(original_value == None):
            #     print(node)
            original_value[scope] = communityId
            node_updates.append({"node_id": node.id, "property_value": original_value})

        self.update_node_properties(node_updates, property_name)
            
    def update_project_graph(self,collection_name):
        """
        Remove all projected graphs, then project three new graph with 3 different scopes,
        which are respectively, Account + NFT nodes w/ OWNED relationship, Account nodes w/ TRANSACTED relationship,
        and Account + NFT nodes w/ OWNED + TRANSACTED relationship.
        
        Parameters:
        - collection_name: str
            Project graph with nodes related to this collection name.

        """

        #First drop all projected graph
        list_graphs = self.db.run_query('neo4j', "CALL gds.graph.list()")
        graph_names = [graph["graphName"] for graph in list_graphs]
        for graph in graph_names:
            self.db.run_query('neo4j', f"CALL gds.graph.drop('{graph}')")
        
        #Account + NFT ; OWNED
        project_query =f"""
        CALL gds.graph.project.cypher(
            '{collection_name}_owned',
            'MATCH (n:Account) RETURN id(n) AS id
            UNION
            MATCH (n:NFT) WHERE n.collection_name = "{collection_name}" RETURN id(n) AS id',
            'MATCH (n:Account)-[r:OWNED]->(m:NFT) WHERE m.collection_name = "{collection_name}" RETURN id(n) AS source, id(m) AS target
            UNION
            MATCH (n:NFT)-[r:OWNED]->(m:Account) WHERE n.collection_name = "{collection_name}" RETURN id(n) AS source, id(m) AS target',
            {{validateRelationships: false}}
        )
        """
        self.db.run_query('neo4j',project_query )
        
        #Account ; TRX
        project_query =f"""
        CALL gds.graph.project.cypher(
            '{collection_name}_trx',
            'MATCH (n:Account) RETURN id(n) AS id',
            'MATCH (n:Account)-[r:TRANSACTED]->(m:Account) WHERE r.collection_name = "{collection_name}" RETURN id(n) AS source, id(m) AS target',
            {{validateRelationships: false}}
        )
        """
        self.db.run_query('neo4j',project_query )
        
        #Account + NFT ; OWNED + TRX
        project_query =f"""
        CALL gds.graph.project.cypher(
            '{collection_name}_owned_trx',
            'MATCH (n:Account) RETURN id(n) AS id
            UNION
            MATCH (n:NFT) WHERE n.collection_name = "{collection_name}" RETURN id(n) AS id',
            'MATCH (n:Account)-[r:OWNED]->(m:NFT) WHERE m.collection_name = "{collection_name}" RETURN id(n) AS source, id(m) AS target
            UNION
            MATCH (n:Account)-[r:TRANSACTED]->(m:Account) WHERE r.collection_name = "{collection_name}" RETURN id(n) AS source, id(m) AS target',
            {{validateRelationships: false}}
        )
        """
        self.db.run_query('neo4j',project_query )
        
    def update_project_graph_complete(self):
        """
        Same purpose as update_project_graph() function, but only for combined collection.  
        Therefore, no needs for specify the collection name.  
        Remove all projected graphs, then project three new graph with 3 different scopes,  
        which are respectively, Account + NFT nodes w/ OWNED relationship,  Account nodes w/ TRANSACTED relationship,  
        and Account + NFT nodes w/ OWNED + TRANSACTED relationship.
        """
        #First drop all projected graph
        list_graphs = self.db.run_query('neo4j', "CALL gds.graph.list()")
        graph_names = [graph["graphName"] for graph in list_graphs]
        for graph in graph_names:
            self.db.run_query('neo4j', f"CALL gds.graph.drop('{graph}')")
        
        #Account + NFT ; OWNED
        project_query =f"""
        CALL gds.graph.project.cypher(
            'complete_owned',
            'MATCH (n:Account) RETURN id(n) AS id
            UNION
            MATCH (n:NFT) RETURN id(n) AS id',
            'MATCH (n:Account)-[r:OWNED]->(m:NFT) RETURN id(n) AS source, id(m) AS target
            UNION
            MATCH (n:NFT)-[r:OWNED]->(m:Account) RETURN id(n) AS source, id(m) AS target',
            {{validateRelationships: false}}
        )
        """
        self.db.run_query('neo4j',project_query )
        
        #Account ; TRX
        project_query =f"""
        CALL gds.graph.project.cypher(
            'complete_trx',
            'MATCH (n:Account) RETURN id(n) AS id',
            'MATCH (n:Account)-[r:TRANSACTED]->(m:Account) RETURN id(n) AS source, id(m) AS target',
            {{validateRelationships: false}}
        )
        """
        self.db.run_query('neo4j',project_query )
        
        #Account + NFT ; OWNED + TRX
        project_query =f"""
        CALL gds.graph.project.cypher(
            'complete_owned_trx',
            'MATCH (n:Account) RETURN id(n) AS id
            UNION
            MATCH (n:NFT) RETURN id(n) AS id',
            'MATCH (n:Account)-[r:OWNED]->(m:NFT) RETURN id(n) AS source, id(m) AS target
            UNION
            MATCH (n:Account)-[r:TRANSACTED]->(m:Account) RETURN id(n) AS source, id(m) AS target',
            {{validateRelationships: false}}
        )
        """
        self.db.run_query('neo4j',project_query )


    def update_community_id_info(self,idList,collection_name,scope):
        """
        Update the community id info node, which stores information about 100 biggest community id with 3 different scopes.
        
        Parameters:
        - idList: str
            List storing the ids of 100 biggest communities.
        - collection_name: str
            Specify which community id info node to update.
        - scope: str
            To which scope the community ids is related.
        """
        query_get = f"""
        MATCH (n:Community_Info)                     
        RETURN n.{self.get_processed_collection_name(collection_name)}_id_list AS array_property
        """
        result = self.db.run_query('neo4j',query_get )
        record = result[0]
        array_property = record['array_property']
        array_property = json.loads(array_property) # convert from json string to 2D array
        #Modify the array
        array_property[scope] = idList
        query_set = f"""
        MATCH (n:Community_Info)
        SET n.{self.get_processed_collection_name(collection_name)}_id_list = '{json.dumps(array_property)}'
        SET n.{self.get_processed_collection_name(collection_name)}_updated_at = timestamp()
        """
        self.db.run_query('neo4j',query_set )


    def update_community_detection(self,limit,collection_name):
        """
        Run the Louvain community detection with 3 different scopes given the target collection,  
        then update the community info node. 
        
        Parameters:
        - limit: int
            Store only the x biggest community result.
        - collection_name: str
            Specify which collection to process.
        """
        
        #Account + NFT ; OWNED
        com_query = f"""
        CALL gds.louvain.stream('{collection_name}_owned')
        YIELD nodeId, communityId
        WITH gds.util.asNode(nodeId) AS node, communityId
        RETURN communityId,
            COLLECT(CASE
                WHEN 'Account' IN labels(node) THEN {{type: 'address', value: node.address, image: ""}}
                ELSE {{type: 'identifier', value: node.identifier, image: node.image_url}}
            END) AS nodes,
        COLLECT(node) AS nodeList
        ORDER BY size(nodes) DESC
        """
        if limit!=0:
            com_query+=f" LIMIT {limit}"  
        community_result = self.db.run_query('neo4j', com_query)
        communities=[]
        community_id=[]
        for record  in community_result:
            communities.append({"communityId": record["communityId"], "nodes": record["nodes"], "nodeList": record["nodeList"]})
            self.update_nodes_com_property(collection_name,record["communityId"],record["nodeList"],0) #for updating the community node
            community_id.append(record["communityId"])
        #Update the community id node

        self.update_community_id_info(community_id,collection_name,0)   

        #Account ; TRX
        com_query = f"""
        CALL gds.louvain.stream('{collection_name}_trx')
        YIELD nodeId, communityId
        WITH gds.util.asNode(nodeId) AS node, communityId
        RETURN communityId,
            COLLECT(CASE
                WHEN 'Account' IN labels(node) THEN {{type: 'address', value: node.address, image: ""}}
                ELSE {{type: 'identifier', value: node.identifier, image: node.image_url}}
            END) AS nodes,
        COLLECT(node) AS nodeList
        ORDER BY size(nodes) DESC
        """
        if limit!=0:
            com_query+=f" LIMIT {limit}"  
        community_result = self.db.run_query('neo4j', com_query)
        communities=[]
        community_id=[]
        for record  in community_result:
            communities.append({"communityId": record["communityId"], "nodes": record["nodes"], "nodeList": record["nodeList"]})
            self.update_nodes_com_property(collection_name,record["communityId"],record["nodeList"],1) #for updating the community node
            community_id.append(record["communityId"])
        #Update the community id node
        self.update_community_id_info(community_id,collection_name,1)     

        
        #Account + NFT ; OWNED + TRX
        com_query = f"""
        CALL gds.louvain.stream('{collection_name}_owned_trx')
        YIELD nodeId, communityId
        WITH gds.util.asNode(nodeId) AS node, communityId
        RETURN communityId,
            COLLECT(CASE
                WHEN 'Account' IN labels(node) THEN {{type: 'address', value: node.address, image: ""}}
                ELSE {{type: 'identifier', value: node.identifier, image: node.image_url}}
            END) AS nodes,
        COLLECT(node) AS nodeList
        ORDER BY size(nodes) DESC
        """
        if limit!=0:
            com_query+=f" LIMIT {limit}" 
        # print("Doing on the third one...")  
        community_result = self.db.run_query('neo4j', com_query)
        communities=[]
        community_id=[]
        for record  in community_result:
            communities.append({"communityId": record["communityId"], "nodes": record["nodes"], "nodeList": record["nodeList"]})
            self.update_nodes_com_property(collection_name,record["communityId"],record["nodeList"],2) #for updating the community node
            community_id.append(record["communityId"])    
        #Update the community id node
        self.update_community_id_info(community_id,collection_name,2)     
        
        
        
    def build_community_info_node(self):
        """
        Build up the community id info nodes with mock data.
        
        """
        
        two_d_array = [[1, 2], [3, 4], [5, 6], [7,8]]
        serialized_array = json.dumps(two_d_array)
        query = f"""
        CREATE (n:Community_Info {{
            boredapeyachtclub_id_list: '{serialized_array}',
            degods_id_list: '{serialized_array}'
        }})
        """
        self.db.run_query('neo4j', query)
    

    def get_transactions(self, all_node_ids: List[str]):
        """
        Retrieve all transactions relationship given a list of node id.
        
        Parameters:
        - all_node_ids: List[str]
            List of nodes id.
        """


        relationships_query = """
        UNWIND $all_node_ids AS nodeId
        MATCH (node)-[r:TRANSACTED]->(other)
        WHERE id(node) = nodeId AND id(other) IN $all_node_ids
        RETURN node, r, other
        """

        rels_result = self.db.run_query('neo4j', relationships_query, {"all_node_ids": all_node_ids})

        relationships = [
            {
                "from_": {
                    "value": rel["node"]["address"] if 'Account' in rel["node"].labels else rel["node"]["identifier"],
                    "type": "address" if 'Account' in rel["node"].labels else "identifier"
                },
                "to": {
                    "value": rel["other"]["address"] if 'Account' in rel["other"].labels else rel["other"]["identifier"],
                    "type": "address" if 'Account' in rel["other"].labels else "identifier"
                },
                "relationship": {
                    "property": rel["r"]["transaction_hash"],
                    "type": "transaction"
                }
            }
            for rel in rels_result
        ]

        return relationships
    

    def get_ownerships(self, all_node_ids: List[str]):
        """
        Retrieve all ownership relationship given a list of node id.
        
        Parameters:
        - all_node_ids: List[str]
            List of nodes id.
        """
        relationships_query = """
        UNWIND $all_node_ids AS nodeId
        MATCH (node)-[r:OWNED]->(other)
        WHERE id(node) = nodeId AND id(other) IN $all_node_ids
        RETURN node, r, other
        """

        rels_result = self.db.run_query('neo4j', relationships_query, {"all_node_ids": all_node_ids})

        relationships = [
            {
                "from_": {
                    "value": rel["node"]["address"] if 'Account' in rel["node"].labels else rel["node"]["identifier"],
                    "type": "address" if 'Account' in rel["node"].labels else "identifier"
                },
                "to": {
                    "value": rel["other"]["address"] if 'Account' in rel["other"].labels else rel["other"]["identifier"],
                    "type": "address" if 'Account' in rel["other"].labels else "identifier"
                },
                "relationship": {
                    "property": str(rel["r"]["currently_owned"]),
                    "type": "ownership"
                }
            }
            for rel in rels_result
        ]

        return relationships
    
    def are_relationship_nodes_in_communities(self, final_result: dict):
        """
        Check if there's node not belongs to any community.  
        Return True if all nodes are in some communities.
        
        Parameters:
        - all_node_ids: List[str]
            List of nodes id.
        """
        # Extract all node values from the communities
        community_node_values = {
            node["value"]
            for community in final_result["communities"]
            for node in community["nodes"]
        }

        # Check if each node in the relationships exists in the extracted node values
        for relationship in final_result["relationships"]:
            from_value = relationship["from_"]["value"]
            to_value = relationship["to"]["value"]
        
            if from_value not in community_node_values or to_value not in community_node_values:
                return False

        return True
