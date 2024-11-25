from typing import List, Dict
import logging
from app.db import get_db
from app.exceptions.node_not_found import NodeNotFoundException
import datetime
from app.neo4j_access.utilities import Utilities

# Including the logic to find Accounts (basend on their address) and NFTs (based on their identifier and collection)
class SearchLogic:

    """
    This class includes the functionality to search an Owner or NFT in the database. In both cases 
    the search NFT or Owner is returned together with its first-order neighbors and the 
    relationships between the searched NFT/Owner and its neighbors.

    Methods
    -------
    - process_neighbors(self, neighbors: List[Dict])
        internal method to create the neighbor-response objects for the search result
    - process_relationships(self, neighbor_relationships: List[Dict])
        internal method to create the relationship-response objects for the search result
    - find_account(self, address: str)
        method to search an Owner with the given address
    - find_nft(self, identifier: str, collection: str)
        method to search an NFT with the given identifier in the given collection

    Author
    ------
    Valentin Leuthe
    """

    def process_neighbors(self, neighbors: List[Dict]):

        """
        Based on a list of neighbor-nodes as they are returned from the cypher query, this 
        method extract the relevant information (depending on the type of node - Owner or NFT) and 
        creates the node-objects for the endpoint response model. 

        Parameters:
        - neighbors: List[Dict]
            List of neighbor nodes as the are returned by the cypher query defined in the 
            methods to search an Owner or NFT
        """

        utilities = Utilities()

        processed_neighbors = []
        for neighbor in neighbors:
            if 'Account' in neighbor.labels:
                neighbor_dict = {
                    "value": neighbor['address'],
                    "link": utilities.get_etherscan_url_address(neighbor['address']),
                    "collection": "",
                    "type": "Account",
                    "image": ""
                }
            else:
                neighbor_dict = {
                    "value": neighbor['identifier'],
                    "link": utilities.get_opensea_url(neighbor["collection_name"], neighbor['identifier']),
                    "collection": neighbor["collection_name"],
                    "type": "NFT",
                    "image": neighbor["image_url"]
                }
            processed_neighbors.append(neighbor_dict)

        return processed_neighbors
    
    def process_relationships(self, neighbor_relationships: List[Dict]):

        """
        Based on a list of relationships as they are returned from the cypher query, this 
        method extract the relevant information (depending on the type of relationship) and 
        creates the relationship-objects for the endpoint response model. 

        Parameters:
        - neighbor_relationships: List[Dict]
            List of relationship-objects as the are returned by the cypher query defined in the 
            methods to search an Owner or NFT
        """

        utilities = Utilities()

        processed_relations = []
        for relation in neighbor_relationships:

            start_node = relation['start']
            end_node = relation['end']
            rel = relation['relation']
            
            # Start node = Owner?
            if 'Account' in start_node.labels:
                start_node_dict = {
                    "value": start_node['address'],
                    "collection": "",
                    "type": "Account",
                    "image": ""
                }
            else: # Start node = NFT
                start_node_dict = {
                    "value": start_node['identifier'],
                    "collection": start_node["collection_name"],
                    "type": "NFT",
                    "image": start_node["image_url"]
                }
            # Start node = Owner?
            if 'Account' in end_node.labels:
                end_node_dict = {
                    "value": end_node['address'],
                    "collection": "",
                    "type": "Account",
                    "image": ""
                }
            else: # Start node = NFT
                end_node_dict = {
                    "value": end_node['identifier'],
                    "collection": end_node["collection_name"],
                    "type": "NFT",
                    "image": end_node["image_url"]
                }
            # check for the correct transaction type
            if rel.type== "TRANSACTED":
                rel_dict = {
                    "property": rel["transaction_hash"],
                    "type": rel.type,
                    "transaction_event_type": rel["event_type"],
                    "nft_identifier": rel["identifier"],
                    "nft_collection": rel["collection_name"],
                    "link_etherscan": utilities.get_etherscan_url_transaction(rel["transaction_hash"])
                }
            elif rel.type == "OWNED":
                rel_dict = {
                    "property": str(rel["currently_owned"]),
                    "type": rel.type,
                    "transaction_event_type": "",
                    "nft_identifier": "",
                    "nft_collection": "",
                    "link_etherscan": ""
                }
            else: # MINT
                rel_dict = {
                    "property": str(datetime.datetime.fromtimestamp(rel["date"])),
                    "type": rel.type,
                    "transaction_event_type": "",
                    "nft_identifier": "",
                    "nft_collection": "",
                    "link_etherscan": ""
                }

            relation_dict = {
                "from_": start_node_dict,
                "to": end_node_dict,
                "relationship": rel_dict
            }
            processed_relations.append(relation_dict)

        return processed_relations

    def find_account(self, address: str):

        """
        Method to search and find an Owner with a given address. The searched owner is returned 
        together with its 1st order neighbors and the all different relationsships between the 
        search owner and its neighbors.

        Parameter:
        - address: str
            the address of the owner that should be searched

        Raise
        - NodeNotFoundException if there is no Owner with the given address in the database
        """

        logging.info(f"search the owner with the address {address}")

        db = get_db()
        utlities = Utilities()

        # Query to find the owner. Returns the searched Owner, his direct neighbors and the 
        # respective realtionships
        query = """
        MATCH (account:Account {address: $address})
        OPTIONAL MATCH (account)-[r]-(neighbor)
        WHERE neighbor:Account OR neighbor:NFT
        RETURN account.address AS accountAddress, 
        collect(DISTINCT neighbor) AS neighbors, 
        [r IN collect(r) | {start: startNode(r), end: endNode(r), relation: r}] AS neighborRelationships
        """

        neighborhood = db.run_query('neo4j', query, {"address": address}) 

        # if the address doesn't exist 
        if not neighborhood:
            logging.info("address doesn't exist")
            raise NodeNotFoundException("Address not found")

        # extract the different parts of the query-result
        account_data = neighborhood[0]
        account_address = account_data['accountAddress']
        neighbors = account_data['neighbors']
        neighbor_relationships = account_data['neighborRelationships']

        count_nft_boredapes = 0
        count_nft_degods = 0
        for neighbor in neighbors:
            if "NFT" in neighbor.labels:
                if neighbor["collection_name"] == "degods-eth":
                    count_nft_degods += 1
                else:
                    count_nft_boredapes += 1
        
        # Process neighbors
        processed_neighbors = self.process_neighbors(neighbors)

        # Process relationships
        processed_relations = self.process_relationships(neighbor_relationships)

        final_result = {
            "account": account_address,
            "link": utlities.get_etherscan_url_address(account_address),
            "count_nft_boredapes": count_nft_boredapes,
            "count_nft_degods": count_nft_degods,
            "neighbors": processed_neighbors,
            "relationships": processed_relations
        }

        logging.info("Search successfully completed")

        return final_result

    def find_nft(self, identifier: str, collection: str):

        """
        Method to search and find an NFT with a given Identifier of a given collection. 
        The searched NFT is returned together with its 1st order neighbors and the all 
        different relationsships between the search NFT and its neighbors.

        Parameter:
        - Identifier: str
            the identifier of the NFT that is searched
        - collection: str
            the collection of the searched NFT (collection is part of the key for an NFT,
            together with its identifier, and therefore needs to be specified)

        Raise
        - NodeNotFoundException if there is no NFT with the given identifier for the 
            given collection in the database
        """

        logging.info(f"START: search the NFT with identifier: {identifier} and collection: {collection}")

        db = get_db()
        utilities = Utilities()

        # Query to find the NFT. Returns the searched NFT, his direct neighbors and the 
        # respective realtionships
        query = """
        MATCH (nft:NFT {identifier: $identifier, collection_name: $collection})
        OPTIONAL MATCH (nft)-[r]-(neighbor)
        WHERE neighbor:Account OR neighbor:NFT
        RETURN nft.identifier AS nftIdentifier, nft.collection_name AS nftCollectionName, nft.image_url AS image_url, 
        collect(DISTINCT neighbor) AS neighbors, 
        [r IN collect(r) | {start: startNode(r), end: endNode(r), relation: r}] AS neighborRelationships
        """
        collection_processed = utilities.get_collection([collection])
        neighborhood = db.run_query('neo4j', query, {"identifier": identifier, "collection": collection_processed}) 

        # if the address doesn't exist 
        if not neighborhood:
            logging.info("address doesn't exist")
            raise NodeNotFoundException("NFT not found")

        # extract the different parts of the query-result
        nft_data = neighborhood[0]
        nft_identifier = nft_data['nftIdentifier']
        nft_image = nft_data['image_url']
        nft_collection = nft_data['nftCollectionName']
        neighbors = nft_data['neighbors']
        neighbor_relationships = nft_data['neighborRelationships']

        # Process neighbors
        processed_neighbors = self.process_neighbors(neighbors)

        # Process relationships
        processed_relations = self.process_relationships(neighbor_relationships)

        final_result = {
            "identifier": nft_identifier,
            "collection": nft_collection,
            "opensea_url": utilities.get_opensea_url(nft_collection, nft_identifier),
            "image_url": nft_image,
            "neighbors": processed_neighbors,
            "relationships": processed_relations
        }

        logging.info("Search successfully completed")

        return final_result