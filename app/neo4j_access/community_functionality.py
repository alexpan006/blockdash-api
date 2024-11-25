import logging
from app.db import get_db
from typing import List
from app.exceptions.not_exists import NotExistsException
from app.neo4j_access.utilities import Utilities
import json
import datetime

class CommunityLogic:

    """
    The class includes the logic for the endpoints which consider community detection. 

    Methods:
    --------
    - get_transactions(self, all_node_ids: List[str])
        derives all TRANSACTED-relationships for given set of nodes
    - get_ownerships(self, all_node_ids: List[str])
        derives all OWNED-relationships for given set of nodes
    - get_mint(self, all_node_ids: List[str])
        derives all OWNED-relationships for given set of nodes
    - get_scope(self, scope: str)
        method to convert the scope that it suits the database
    - get_collection(self, collection: str)
        method to convert the collection that it suits the database
    - get_single_community(self, community_id: int, scope: str, collection: str, limit: int)
        returns the nodes and relationships of the community with the community-id
    - get_community_sum(self, limit: int, scope: int, collection: str)
        creates and returns a summary of the biggest communities

    Author
    ------
    Valentin Leuthe
    """

    def get_transactions(self, all_node_ids: List[str]):

        """
        Based on a id list of nodes, the method derives all TRANSACTION-relationships between those nodes
        and returns the result in the correct form for the response model

        Parameters:
        - all_nodes_ids: List[str]
            A list of ids of the nodes that should be considered for the relationship extraction. Nodes tha 
            form either the start node or end node of the relationship
        """

        db = get_db()
        utilities = Utilities()

        # query to get the TRANSACTED relationships between the nodes in the all_node_ids list
        relationships_query = """
        UNWIND $all_node_ids AS nodeId
        MATCH (node)-[r:TRANSACTED]->(other)
        WHERE elementId(node) = nodeId AND elementId(other) IN $all_node_ids
        RETURN node, r, other
        """

        rels_result = db.run_query('neo4j', relationships_query, {"all_node_ids": all_node_ids})

        # format the result for the response model
        relationships = [
            {
                "from_": {
                    "value": rel["node"]["address"] if 'Account' in rel["node"].labels else rel["node"]["identifier"],
                    "collection": "" if 'Account' in rel["node"].labels else rel["node"]["collection_name"],
                    "type": "address" if 'Account' in rel["node"].labels else "identifier"
                },
                "to": {
                    "value": rel["other"]["address"] if 'Account' in rel["other"].labels else rel["other"]["identifier"],
                    "collection": "" if 'Account' in rel["node"].labels else rel["node"]["collection_name"],
                    "type": "address" if 'Account' in rel["other"].labels else "identifier"
                },
                "relationship": {
                    "property": rel["r"]["transaction_hash"],
                    "type": "transaction",
                    "transaction_event_type": rel["r"]["event_type"],
                    "nft_identifier": rel["r"]["identifier"],
                    "nft_collection": rel["r"]["collection_name"],
                    "link_etherscan": utilities.get_etherscan_url_transaction(rel["r"]["transaction_hash"])
                }
            }
            for rel in rels_result
        ]

        return relationships

    def get_ownerships(self, all_node_ids: List[str]):
        
        """
        Based on a id list of nodes, the method derives all OWNED-relationships between those nodes
        and returns the result in the correct form for the response model

        Parameters:
        - all_nodes_ids: List[str]
            A list of ids of the nodes that should be considered for the relationship extraction. Nodes tha 
            form either the start node or end node of the relationship
        """

        db = get_db()
        
        # query to get the TRANSACTED relationships between the nodes in the all_node_ids list
        relationships_query = """
        UNWIND $all_node_ids AS nodeId
        MATCH (node)-[r:OWNED]->(other)
        WHERE elementId(node) = nodeId AND elementId(other) IN $all_node_ids
        RETURN node, r, other
        """

        rels_result = db.run_query('neo4j', relationships_query, {"all_node_ids": all_node_ids})


        # format the result for the response model
        relationships = [
            {
                "from_": {
                    "value": rel["node"]["address"] if 'Account' in rel["node"].labels else rel["node"]["identifier"],
                    "collection": "" if 'Account' in rel["node"].labels else rel["node"]["collection_name"],
                    "type": "address" if 'Account' in rel["node"].labels else "identifier"
                },
                "to": {
                    "value": rel["other"]["address"] if 'Account' in rel["other"].labels else rel["other"]["identifier"],
                    "collection": "" if 'Account' in rel["node"].labels else rel["node"]["collection_name"],
                    "type": "address" if 'Account' in rel["other"].labels else "identifier"
                },
                "relationship": {
                    "property": str(rel["r"]["currently_owned"]),
                    "type": "ownership",
                    "transaction_event_type": "",
                    "nft_identifier": "",
                    "nft_collection": "",
                    "link_etherscan": ""
                }
            }
            for rel in rels_result
        ]

        return relationships
    
    def get_mint(self, all_node_ids: List[str]):
        
        """
        Based on a id list of nodes, the method derives all MINT-relationships between those nodes
        and returns the result in the correct form for the response model

        Parameters:
        - all_nodes_ids: List[str]
            A list of ids of the nodes that should be considered for the relationship extraction. Nodes tha 
            form either the start node or end node of the relationship
        """

        db = get_db()
        
        # query to get the TRANSACTED relationships between the nodes in the all_node_ids list
        relationships_query = """
        UNWIND $all_node_ids AS nodeId
        MATCH (node)-[r:MINT]->(other)
        WHERE elementId(node) = nodeId AND elementId(other) IN $all_node_ids
        RETURN node, r, other
        """

        rels_result = db.run_query('neo4j', relationships_query, {"all_node_ids": all_node_ids})


        # format the result for the response model
        relationships = [
            {
                "from_": {
                    "value": rel["node"]["address"] if 'Account' in rel["node"].labels else rel["node"]["identifier"],
                    "collection": "" if 'Account' in rel["node"].labels else rel["node"]["collection_name"],
                    "type": "address" if 'Account' in rel["node"].labels else "identifier"
                },
                "to": {
                    "value": rel["other"]["address"] if 'Account' in rel["other"].labels else rel["other"]["identifier"],
                    "collection": "" if 'Account' in rel["node"].labels else rel["node"]["collection_name"],
                    "type": "address" if 'Account' in rel["other"].labels else "identifier"
                },
                "relationship": {
                    "property": str(datetime.datetime.fromtimestamp(rel["r"]["date"])),
                    "type": "mint",
                    "transaction_event_type": "",
                    "nft_identifier": "",
                    "nft_collection": "",
                    "link_etherscan": ""
                }
            }
            for rel in rels_result
        ]

        return relationships
    
    def get_scope(self, scope: str):

        """
        method turns the community detection scope as it comes as a parameter from the endpoint 
        into a numerical value to access the correct community-id stored for every node

        Parameters:
        - scope: str
            scope for the community detection as it comes from the endpoint 

        Raises:
        - NotExistException: if the specified scope doesn't exist
        """

        if scope.upper() == "OWNERSHIP":
            return 0
        elif scope.upper() == "TRANSACTION":
            return 1
        elif scope.upper() == "ALL":
            return 2
        else: 
            raise NotExistsException(f"scope {scope} does not exists")
        
    def get_collection(self, collection: List[str]):

        """
        method turns the collection as it comes as a parameter from the endpoint 
        into the correct form as it is required for the database to get the correct values

        Parameters:
        - collection: str
            collection as it comes from the endpoint 

        Raises:
        - NotExistException: if the specified collection doesn't exist
        """

        collection_upper = [item.upper() for item in collection]
        if len(collection_upper) == 1 and collection_upper[0] == "BOREDAPES":
            return "boredapeyachtclub"
        elif len(collection_upper) == 1 and collection_upper[0] == "DEGODS":
            return "degods"
        elif len(collection_upper) == 2 and "BOREDAPES" in collection_upper and "DEGODS" in collection_upper:
            return "complete"
        else:
            raise NotExistsException(f"Collection {collection} does not exist")
        
    
    def get_nft_share(self, collection_processed: str, community_ids: List[int], scope_processed: int):

        """
        internal method to calculate the share of nft that belong to either collection.

        Parameters:
        - collection_processed: str
            already processed collection identifier as it needs to be for getting the correct query-results
        - community_ids: List[int]
            list of communities for which the share needs to be calculated
        - scope_processed: int
            already processed scope identifier as it needs to be for getting the correct query-results
        """

        db = get_db()
        # query the number of boredapes and degods nfts and the total number of nft for each community
        # in order to calculate the share of nft of each community
        nft_share = f"""
        UNWIND $community_ids AS communityId
        MATCH (n:NFT)
        WHERE n.{collection_processed}_com_id_list[$scope] = communityId
        WITH
            communityId,
            count(n) AS totalNFTs, 
            sum(CASE WHEN n.collection_name = 'degods-eth' THEN 1 ELSE 0 END) AS degodsCount,
            sum(CASE WHEN n.collection_name = 'boredapeyachtclub' THEN 1 ELSE 0 END) AS boredapesCount
        RETURN 
            communityId, 
            totalNFTs, 
            degodsCount, 
            boredapesCount
        """

        nft_share_result = db.run_query('neo4j', nft_share, {"community_ids": community_ids, "scope": scope_processed}) 
        
        # dictionary to later use in the final result
        nft_share_dict = {
            record['communityId']: {
                'totalNFTs': record['totalNFTs'],
                'degodsCount': record['degodsCount'],
                'boredapesCount': record['boredapesCount']
            }
            for record in nft_share_result
        }

        return nft_share_dict


    def get_single_community(self, community_id: int, scope: str, collection: List[str], limit: int):

        """
        Based on a community_id, the method returns a set of most important nodes (Owner, NFT) of that community together with 
        the different relationships (MINT, TRANSACTED, OWNED) between the selected nodes. The most important nodes are identified 
        through degree-centrality, the most central nodes are returned.

        Parameters:
        - community_id: int 
            Id of the community to be returned
        - scope: str 
            scope for the community detection: 
                "ownership": Community detection is conducted on all NFT, Owners and the OWNED-relationships
                "transaction": Community detection is conducted on all Owners and the TRANSACTED-relationships
                "all": Community detection is conducted on all NFT, Owners and the relationships OWNED, TRANSACTED, MINT
        - collection: str 
            specifies the collection for the community detection
        - limit: int 
            number of most important nodes of a community to be returned

        Raises:
        - NotExistException: in case there is no community with the given id, for the given scope and collections
        """

        logging.info(f"""START: get a single community with id {community_id}, scope: {scope}, collection: {collection} 
                     and limit the number of returned nodes with limit: {limit}""")

        db = get_db()
        utilities = Utilities()

        scope_processed = self.get_scope(scope)
        collection_processed = self.get_collection(collection)

        # to prevent errors, in case the clean up didn't take place
        list_graphs = db.run_query('neo4j', "CALL gds.graph.list()")
        graph_names = [graph["graphName"] for graph in list_graphs]
        if 'tempGraph' in graph_names:
            drop_tempgraph_query = """
            CALL gds.graph.drop('tempGraph')
            """
            db.run_query('neo4j', drop_tempgraph_query)
            logging.info("graph projection clean-up took place")

        # query to get the overall amount of nodes in the community
        node_count = f"""
        MATCH (n) 
        WHERE (n:Account OR n:NFT) AND n.{collection_processed}_com_id_list[{scope_processed}] = {community_id} 
        RETURN count(n) AS nodeCount
        """
        node_count_result = db.run_query('neo4j', node_count)
        total_node_count = node_count_result[0]["nodeCount"]
        
        # if the community doesn't exist, the exception is raised as otherwise the graph projection would fail and cause an error
        if total_node_count == 0:
            raise NotExistsException(f"the collection with id {community_id} does not exist for the scope {scope} and collection(s) {collection}")

        # graph projection to only get the nodes and respective relations with correct collection, scope, community_Id
        temp_grap = f"""
        CALL gds.graph.project.cypher(
        'tempGraph',
        'MATCH (n) 
        WHERE (n:Account OR n:NFT) AND n.{collection_processed}_com_id_list[{scope_processed}] = {community_id} RETURN id(n) AS id',
        'MATCH (n1)-[r]->(n2) WHERE (n1:Account OR n1:NFT) AND (n2:Account OR n2:NFT) 
        AND n1.{collection_processed}_com_id_list[{scope_processed}] = {community_id} 
        AND n2.{collection_processed}_com_id_list[{scope_processed}] = {community_id} 
        RETURN id(n1) AS source, id(n2) AS target'
        )
        """

        db.run_query('neo4j', temp_grap)
        logging.info("Graph projection created")

        # calculate centrality for the selected nodes within the specified community 
        # the limit can be adjusted, depending what is handable in the frontend
        degree_centrality_query = """
        CALL gds.degree.stream('tempGraph', { orientation: 'UNDIRECTED' })
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId) AS node, score
        ORDER BY score DESC
        LIMIT $limit
        """
        top_nodes = db.run_query('neo4j', degree_centrality_query, {"limit": limit})
        logging.info("degree centrality run on the nodes of the community, most important nodes are returned")

        drop_tempgraph_query = """
        CALL gds.graph.drop('tempGraph')
        """
        db.run_query('neo4j', drop_tempgraph_query)
        logging.info("graph projection clean-up took place")

        all_node_ids = [node['node'].element_id for node in top_nodes]

        # get all different kinds of relationships
        ownerships = self.get_ownerships(all_node_ids)
        transactions = self.get_transactions(all_node_ids)
        mint = self.get_mint(all_node_ids)

        nft_share_dict = self.get_nft_share(collection_processed, [community_id], scope_processed)

        total_nfts = 0
        degods_count = 0
        boredapes_count = 0

        if nft_share_dict:
            total_nfts = next(iter(nft_share_dict.values()))['totalNFTs']
            degods_count = next(iter(nft_share_dict.values()))['degodsCount']
            boredapes_count = next(iter(nft_share_dict.values()))['boredapesCount']

        share_nft_boredapes = 0
        share_nft_degods = 0

        if total_nfts != 0:
            share_nft_boredapes = round(boredapes_count/total_nfts, 4)
            share_nft_degods = round(degods_count/total_nfts, 4)
        
        # preparing the nodes for the response model
        nodes_list = []
        for record in top_nodes:
            node = record['node']
            centrality_score = record['score']
            if "Account" in node.labels:
                node_info = {
                    "value": node.get("address", ""),
                    "collection": "",
                    "type": "address",
                    "image": "",
                    "link": utilities.get_etherscan_url_address(node.get("address", "")),
                    "centrality_score": centrality_score
                }
            else:
                node_info = {
                    "value": node.get("identifier", ""),
                    "collection": node.get("collection_name"),
                    "type": "NFT",
                    "image": node.get("image_url", ""),
                    "link": utilities.get_opensea_url(node.get("collection_name"), node.get("identifier", "")),
                    "centrality_score": centrality_score
                }
            nodes_list.append(node_info)

        final_result = {
            "community_id": community_id,
            "total_node_count": total_node_count,
            "total_nft": total_nfts,
            "nft_share_degods": share_nft_degods,
            "nft_share_boredapes": share_nft_boredapes,
            "nodes": nodes_list,
            "ownership_relations": ownerships,
            "transaction_relations": transactions,
            "mint_relations": mint
        }

        return final_result

    def get_community_sum(self, limit: int, scope: int, collection: List[str]):

        """
        The method creates and returns a summary of the community detection, by returning the biggest communities, with 
        the respective size together with the amount of relationships within the communities. The relationships are 
        aggregated and not differenciated by their type. 

        Parameters:
        - limit:int 
            limit the number of biggest communities returned
        - scope: str 
            scope for the community detection: 
                "ownership": Community detection is conducted on all NFT, Owners and the OWNED-relationships
                "transaction": Community detection is conducted on all Owners and the TRANSACTED-relationships
                "all": Community detection is conducted on all NFT, Owners and the relationships OWNED, TRANSACTED, MINT
        - collection: str 
            specifies the collection for the community detection
        """

        logging.info(f"START: get the community summary with a limit {limit}, the scoope {scope} for the collection: {collection}")

        scope_pro = self.get_scope(scope)
        collection_pro = self.get_collection(collection)

        logging.info(f"get_community_sum with limit {limit}, scope {scope_pro} and collection {collection_pro}")

        db = get_db()

        # Query the ids of the biggest communities
        biggest_communities = f"""
        MATCH (n:Community_Info)
        RETURN n.{collection_pro}_id_list AS biggest_communities
        """

        result_biggest_communities = db.run_query('neo4j', biggest_communities)
        community_ids = json.loads(result_biggest_communities[0]['biggest_communities'])
        # filter the amount of community ids based on the specified limit - considering the biggest ones
        selected_community_ids = community_ids[scope_pro][:limit]
        logging.info("Ids of biggest communities derviced and limited")
        
        # Get the Node count for every community id => to get the size of the respective community 
        node_count = f"""
        UNWIND $community_ids AS communityId
        MATCH (n)
        WHERE (n:Account OR n:NFT) AND n.{collection_pro}_com_id_list[$scope] = communityId
        RETURN communityId, count(n) AS nodeCount
        """

        community_sizes = db.run_query('neo4j', node_count, {"community_ids": selected_community_ids, "scope": scope_pro}) 
        logging.info("the size of the biggest communities are calculated")

        # count relationships between the communities
        relationships = f"""
        MATCH (a)-[r]->(b)
        WHERE (a:Account OR a:NFT) AND (b:Account OR b:NFT) 
        AND a.{collection_pro}_com_id_list[$scope] IN $community_ids AND b.{collection_pro}_com_id_list[$scope] IN $community_ids
        AND a.{collection_pro}_com_id_list[$scope] <> b.{collection_pro}_com_id_list[$scope]
        RETURN a.{collection_pro}_com_id_list[$scope] AS start_node, b.{collection_pro}_com_id_list[$scope] AS end_node, count(r) AS relationCount
        """

        relation_overview = db.run_query('neo4j', relationships, {"scope": scope_pro, "community_ids": selected_community_ids}) 
        logging.info("3rd query to count the relationships between communities completed")
        
        # Prepare the final result by iterating over community_sizes and combining with nft_share_dict
        final_result = {
            "communities": [],
            "relationships": [
                {
                    "start_node": record['start_node'],
                    "end_node": record['end_node'],
                    "count": record['relationCount']
                } for record in relation_overview
            ]
        }

        nft_share_dict = self.get_nft_share(collection_pro, selected_community_ids, scope_pro)

        for record in community_sizes:
            community_id = record['communityId']
            node_count = record['nodeCount']
            
            # Initialize share variables
            nft_share_degods = 0
            nft_share_boredapes = 0
            
            # Get the corresponding NFT share data
            nft_data = nft_share_dict.get(community_id)
            
            # calculate the shares with 4 decimals
            if nft_data:
                total_nfts = nft_data['totalNFTs']
                if total_nfts != 0:
                    nft_share_degods = round(nft_data['degodsCount'] / total_nfts, 4)
                    nft_share_boredapes = round(nft_data['boredapesCount'] / total_nfts, 4)
            
            # Append to the final result
            final_result['communities'].append({
                "community_id": community_id,
                "size": node_count,
                "nft_share_degods": nft_share_degods,
                "nft_share_boredapes": nft_share_boredapes
            })

        return final_result