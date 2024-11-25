from typing import List
import logging
from app.db import get_db
import datetime
from app.exceptions.not_exists import NotExistsException
from app.neo4j_access.utilities import Utilities

class CentralityLogic:

    """
    The class includes the logic for the endpoint to get the most central nodes and their relationships. 

    Methods:
    --------
    - get_transactions(self, all_node_ids: List[str])
        derives and aggregates the TRANSACTION-relationships between the central nodes returned
    - get_ownerships(self, all_node_ids: List[str])
        derives and aggregates the OWNED-relationships between the central nodes returned
    - get_mint(self, all_node_ids: List[str])
        derives and aggregates the MINT-relationships between the central nodes returned
    - get_collection(self, collection: List[str])
        internal method to convert the collection that it suits the database
    - get_most_central_nodes(self, limit: int, collection: List[str])
        method to identify the most central nodes and return those together with their relationships

    Author
    ------
    Valentin Leuthe 
    """
    
    def get_transactions(self, all_node_ids: List[str]):

        """
        Based on a id list of central nodes, the method derives all TRANSACTION-relationships between those nodes
        and returns the result in the correct form for the response model. As there might be several 
        transactions between the same nodes, they are aggregated into one relationship so simplyfy visualization 
        in the frontend. Each aggregated relation includs the information of all the aggregated transactions.

        Parameters:
        - all_nodes_ids: List[str]
            A list of ids of the nodes that should be considered for the relationship extraction. Nodes that 
            form either the start node or end node of the relationship
        """

        logging.info("get all TRANSACTION-relationships between the given set of nodes")
        db = get_db()
        utilities = Utilities()

        # query to get the respective relationships, grouped by relationships with same 
        # start and end node
        relationships_query = """
        UNWIND $all_node_ids AS nodeId
        MATCH (node)-[r:TRANSACTED]->(other)
        WHERE elementId(node) = nodeId AND elementId(other) IN $all_node_ids
        RETURN node, other, collect(r) AS relationships
        """

        rels_result = db.run_query('neo4j', relationships_query, {"all_node_ids": all_node_ids})

        # create the Transaction-objects for the response model 
        relationships = []
        for rel in rels_result:
            from_node = rel["node"]
            to_node = rel["other"]
            trans_rels = rel["relationships"]

            # Aggregating transaction properties
            trans_properties = {
                "transaction_hash": [],
                "link": [],
                "event_type": [],
                "identifier": [],
                "collection_name": []
            }
            for trans_rel in trans_rels:
                trans_properties["transaction_hash"].append(trans_rel["transaction_hash"])
                trans_properties["link"].append(utilities.get_etherscan_url_transaction(trans_rel["transaction_hash"]))
                trans_properties["event_type"].append(trans_rel["event_type"])
                trans_properties["identifier"].append(trans_rel["identifier"])
                trans_properties["collection_name"].append(trans_rel["collection_name"])

            relationships.append({
                "from_": {
                    "value": from_node["address"] if 'Account' in from_node.labels else from_node["identifier"],
                    "collection": "" if 'Account' in from_node.labels else from_node["collection_name"],
                    "type": "address" if 'Account' in from_node.labels else "identifier"
                },
                "to": {
                    "value": to_node["address"] if 'Account' in to_node.labels else to_node["identifier"],
                    "collection": "" if 'Account' in to_node.labels else to_node["collection_name"],
                    "type": "address" if 'Account' in to_node.labels else "identifier"
                },
                "relationship": {
                    "property": trans_properties["transaction_hash"],
                    "link": trans_properties["link"],
                    "type": "transaction",
                    "transaction_event_type": trans_properties["event_type"],
                    "nft_identifier": trans_properties["identifier"],
                    "nft_collection": trans_properties["collection_name"],
                    "relationship_count": len(trans_rels)
                }
            })

        return relationships

    def get_ownerships(self, all_node_ids: List[str]):

        """
        Based on a id list of central nodes, the method derives all OWNED-relationships between those nodes
        and returns the result in the correct form for the response model. As there might be several 
        transactions between the same nodes, they are aggregated into one relationship so simplyfy visualization 
        in the frontend. Each aggregated relation includs the information of all the aggregated transactions.

        Parameters:
        - all_nodes_ids: List[str]
            A list of ids of the nodes that should be considered for the relationship extraction. Nodes that 
            form either the start node or end node of the relationship
        """

        logging.info("get all OWN-relationships between the given set of nodes")
        db = get_db()
        
        # query to get the respective relationships, grouped by relationships with same 
        # start and end node
        relationships_query = """
        UNWIND $all_node_ids AS nodeId
        MATCH (node)-[r:OWNED]->(other)
        WHERE elementId(node) = nodeId AND elementId(other) IN $all_node_ids
        RETURN node, other, collect(r) AS relationships
        """

        rels_result = db.run_query('neo4j', relationships_query, {"all_node_ids": all_node_ids})

        # create the Transaction-objects for the response model 
        relationships = []
        for rel in rels_result:
            from_node = rel["node"]
            to_node = rel["other"]
            trans_rels = rel["relationships"]

            # Aggregating transaction properties
            trans_properties = {
                "currently_owned": [],
                "link": [],
                "event_type": [],
                "identifier": [],
                "collection_name": []
            }
            for trans_rel in trans_rels:
                trans_properties["currently_owned"].append(str(trans_rel["currently_owned"]))
                trans_properties["link"].append("")
                trans_properties["event_type"].append("")
                trans_properties["identifier"].append("")
                trans_properties["collection_name"].append("")

            relationships.append({
                "from_": {
                    "value": from_node["address"] if 'Account' in from_node.labels else from_node["identifier"],
                    "collection": "" if 'Account' in from_node.labels else from_node["collection_name"],
                    "type": "address" if 'Account' in from_node.labels else "identifier"
                },
                "to": {
                    "value": to_node["address"] if 'Account' in to_node.labels else to_node["identifier"],
                    "collection": "" if 'Account' in to_node.labels else to_node["collection_name"],
                    "type": "address" if 'Account' in to_node.labels else "identifier"
                },
                "relationship": {
                    "property": trans_properties["currently_owned"],
                    "link": trans_properties["link"],
                    "type": "ownership",
                    "transaction_event_type": trans_properties["event_type"],
                    "nft_identifier": trans_properties["identifier"],
                    "nft_collection": trans_properties["collection_name"],
                    "relationship_count": len(trans_rels)
                }
            })

        return relationships
    
    def get_mint(self, all_node_ids: List[str]):

        """
        Based on a id list of central nodes, the method derives all MINT-relationships between those nodes
        and returns the result in the correct form for the response model. As there might be several 
        transactions between the same nodes, they are aggregated into one relationship so simplyfy visualization 
        in the frontend. Each aggregated relation includs the information of all the aggregated transactions.

        Parameters:
        - all_nodes_ids: List[str]
            A list of ids of the nodes that should be considered for the relationship extraction. Nodes that 
            form either the start node or end node of the relationship
        """

        logging.info("get all MINT-relationships between the given set of nodes")
        db = get_db()
        
        # query to get the respective relationships, grouped by relationships with same 
        # start and end node
        relationships_query = """
        UNWIND $all_node_ids AS nodeId
        MATCH (node)-[r:MINT]->(other)
        WHERE elementId(node) = nodeId AND elementId(other) IN $all_node_ids
        RETURN node, other, collect(r) AS relationships
        """

        rels_result = db.run_query('neo4j', relationships_query, {"all_node_ids": all_node_ids})

        # create the Transaction-objects for the response model 
        relationships = []
        for rel in rels_result:
            from_node = rel["node"]
            to_node = rel["other"]
            trans_rels = rel["relationships"]

            # Aggregating transaction properties
            trans_properties = {
                "date": [],
                "link": [],
                "event_type": [],
                "identifier": [],
                "collection_name": []
            }
            for trans_rel in trans_rels:
                trans_properties["date"].append(str(datetime.datetime.fromtimestamp(trans_rel["date"])))
                trans_properties["link"].append("")
                trans_properties["event_type"].append("")
                trans_properties["identifier"].append("")
                trans_properties["collection_name"].append("")

            relationships.append({
                "from_": {
                    "value": from_node["address"] if 'Account' in from_node.labels else from_node["identifier"],
                    "collection": "" if 'Account' in from_node.labels else from_node["collection_name"],
                    "type": "address" if 'Account' in from_node.labels else "identifier"
                },
                "to": {
                    "value": to_node["address"] if 'Account' in to_node.labels else to_node["identifier"],
                    "collection": "" if 'Account' in to_node.labels else to_node["collection_name"],
                    "type": "address" if 'Account' in to_node.labels else "identifier"
                },
                "relationship": {
                    "property": trans_properties["date"],
                    "link": trans_properties["link"],
                    "type": "mint",
                    "transaction_event_type": trans_properties["event_type"],
                    "nft_identifier": trans_properties["identifier"],
                    "nft_collection": trans_properties["collection_name"],
                    "relationship_count": len(trans_rels)
                }
            })

        return relationships

    def get_collection(self, collection: List[str]):
        collection_upper = [item.upper() for item in collection]
        if len(collection_upper) == 1 and collection_upper[0] == "BOREDAPES":
            return "boredapeyachtclub"
        elif len(collection_upper) == 1 and collection_upper[0] == "DEGODS":
            return "degods-eth"
        elif len(collection_upper) == 2 and "BOREDAPES" in collection_upper and "DEGODS" in collection_upper:
            return ""
        else:
            raise NotExistsException(f"Collection {collection} does not exist")   
    
    def get_most_central_nodes(self, limit: int, collection: List[str]):

        """
        Method to calculate the most central nodes of the entire network (based on the selected collections).
        It returns the most central nodes (based on the specified limit) together with all the relationships
        between those nodes. The relationships are aggregated if there are relations of the same type between the 
        same start node and end node.

        Parameters:
        - limit: int
            specifies the number of most central nodes that are returned
        - collection: List[str]
            specifies the collections that are considered for the centrality measurement 
        """

        logging.info(f"START: get the most central nodes of the network with a node limit {limit} for the collection(s) {collection}")

        db = get_db()
        utilities = Utilities()

        # query to clean up the graph projection which is used for centrality measurement 
        drop_tempgraph_query = """
            CALL gds.graph.drop('centralityGraph')
            """
        collection_processed = self.get_collection(collection)

        list_graphs = db.run_query('neo4j', "CALL gds.graph.list()")
        graph_names = [graph["graphName"] for graph in list_graphs]
        if 'centralityGraph' in graph_names:
            db.run_query('neo4j', drop_tempgraph_query)
            logging.info("graph projection clean-up took place")

        # create graph projection as the basis for the centrality measurement
        # depends whether the user filters for a single community or not 
        if collection_processed == "":
            # in case both collections are considered = no collection filter
            graph_projection_query = """
            CALL gds.graph.project(
                'centralityGraph',
                ['Account', 'NFT'],
                ['MINT', 'OWNED', 'TRANSACTED']
            ) 
            """
        else:
            # query including collection filter 
            graph_projection_query = f"""
            CALL gds.graph.project.cypher(
                'centralityGraph',
                'MATCH (n:NFT) WHERE n.collection_name = "{collection_processed}" RETURN id(n) AS id 
                UNION MATCH (a:Account)-[r:TRANSACTED]->() WHERE r.collection_name = "{collection_processed}" RETURN id(a) AS id 
                UNION MATCH (a:Account)-[r:MINT]->() WHERE r.collection_name = "{collection_processed}" RETURN id(a) AS id',
                'MATCH (n1)-[r]->(n2) WHERE (r:TRANSACTED OR r:OWNED OR r:MINT) RETURN id(n1) AS source, id(n2) AS target, id(r) AS id',
                {{validateRelationships: false}}
            ) 
            """
        
        db.run_query('neo4j', graph_projection_query)
        logging.info("graph projection as basis for centrality measurement created")

        # query to apply degree centrality 
        degree_centrality_query = """
            CALL gds.degree.stream('centralityGraph', { orientation: 'UNDIRECTED' })
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId) AS node, score
            ORDER BY score DESC
            LIMIT $limit
        """
        central_nodes = db.run_query('neo4j', degree_centrality_query, {"limit": limit})

        db.run_query('neo4j', drop_tempgraph_query)
        logging.info("graph projection clean-up took place")

        all_node_ids = [node['node'].element_id for node in central_nodes]
        
        # getting all different kinds of relationships between the selected nodes 
        ownerships = self.get_ownerships(all_node_ids)
        transactions = self.get_transactions(all_node_ids)
        mint = self.get_mint(all_node_ids)
        
        logging.info("relationships between central nodes derived")

        # derive node-information for the output
        nodes_list = []
        for record in central_nodes:
            node = record['node']
            if "Account" in node.labels:
                node_info = {
                    "value": node.get("address", ""),
                    "link": utilities.get_etherscan_url_address(node.get("address", "")),
                    "collection": "",
                    "type": "address",
                    "image": "",
                    "centrality_score": record["score"]
                }
            else:
                node_info = {
                    "value": node.get("identifier", ""),
                    "link": utilities.get_opensea_url(node.get("collection_name"), node.get("identifier", "")),
                    "collection": node.get("collection_name"),
                    "type": "NFT",
                    "image": node.get("image_url", ""),
                    "centrality_score": record["score"]
                }
            nodes_list.append(node_info)

        # putting the output together
        final_result = {
            "nodes": nodes_list,
            "ownership_relations": ownerships,
            "transaction_relations": transactions,
            "mint_relations": mint
        }

        return final_result

        

    