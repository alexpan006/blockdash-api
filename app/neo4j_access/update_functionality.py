from dotenv import load_dotenv
from app.db import get_db
from app.cache import connect_to_redis, delete_cache_keys
from ..opensea_api import query_api
from datetime import datetime
import logging
import time
load_dotenv()

class UpdateFunctionality:

    @staticmethod
    def saveUpdateTimeToRedis():
        redis_connection = connect_to_redis()
        return redis_connection.set("lastUpdateAt", datetime.now().timestamp())


    """
    The class includes the logic of updating transaction records from Opensea. 

    Methods:
    --------
    - run_update(self,collection_name)
        start the update process.
    - create_update_node(self,collection_name,updated_at)
        create nodes to record the status of updating.
    - get_identifier_id_list(self,collection_name)
        retrieve all identifier related to specified collection.
    - insert_new_trx_data(self,data)
        insert new transaction record into neo4j database.
    - get_data_from_opensea(self,updated_at,update_till,collection_address,ids_list)
        retrieve new transaction records from Opensea.
    - update_update_info(self,latest_block_time,collection_name)
        update the status nodes
    - get_last_update(self)
        get last update blocktime.
    - get_processed_collection_name(self,collection_name)
        return processed collection name.
        
    Author
    ------
    Kai-Yan Pan
    """
    def __init__(self):
        self.db =get_db()
        
    def run_update(self,collection_name):
        """
        Fetch new NFTs transaction records from Opensea and update the neo4j database.
        Parameters:
        - collection_name: str
            which collection to update.
        Raise
        - Exception if there is error while getting new data from Ethereum.

        """
        
        error = False
        
        #First get the current blocktime
        current_unix_timestamp = int(time.time())
        logging.info(f"Current time in unix:{current_unix_timestamp}")
        query = f"""
        MATCH (n:Update_Info)
        WHERE n.collection_name = '{self.get_processed_collection_name(collection_name)}'
        RETURN n
        """
        #Get all ids to update
        result = self.db.run_query('neo4j', query)
        result = result[0]['n']._properties
        idList_info = self.get_identifier_id_list(collection_name)
        # print(len(idList_info))
        # print(idList_info[0])
        
        #Query and update relationship in db
        try:
            self.get_data_from_opensea(collection_name,current_unix_timestamp,result['collection_address'],idList_info)
        except Exception as err:
            logging.error(err)
        logging.info(f"Complete Update Collection:{collection_name},till {current_unix_timestamp} is done.")
        print("done")
        self.saveUpdateTimeToRedis()
        delete_cache_keys("application-cache:*")

    def set_update_frequency(self,collection_name,frequency):
        query = f"""
        MATCH (n:Update_Info)
        WHERE n.collection_name = '{self.get_processed_collection_name(collection_name)}'
        SET n.update_frequency	 = $frequency
        """
        self.db.run_query('neo4j',query,parameters={'frequency':frequency})
        
    def get_identifier_id_list(self,collection_name):
        """
        Retrieve a list of NFT identifiers given the collection name.
        Parameters:
        - collection_name: str
            relate to which collection.
        """
        query = f"""
        MATCH (n:NFT)
        WHERE n.collection_name = '{self.get_processed_collection_name(collection_name)}'
        RETURN n.identifier AS identifier, n.updated_at AS updated_at
        """
        idList = self.db.run_query('neo4j',query)
        idList = [{'id': one['identifier'],'updated_at':one['updated_at']} for one in idList]
        return idList
    
    def insert_new_trx_data(self,data):
        """
        Insert new transaction or transfer record into neo4j database. 
        
        Parameters:
        - data: dict
            transaction data.
        """
        mock_data= [1,2,3]
        query0 = """
        MERGE (to_address:Account {address: $to_address,boredapeyachtclub_com_id_list:$mock_data,complete_com_id_list:$mock_data,degods_com_id_list:$mock_data})
        MERGE (from_address:Account {address: $from_address,boredapeyachtclub_com_id_list:$mock_data,complete_com_id_list:$mock_data,degods_com_id_list:$mock_data})
        """
        
        query1 = """
        MATCH (from_address:Account {address: $from_address}),(nft:NFT {identifier: $identifier,collection_name: $collection_name})
        MERGE (from_address)-[ro:OWNED]->(nft)
        ON CREATE SET ro.currently_owned = $not_owned, ro.until = $from_until
        ON MATCH SET ro.currently_owned = $not_owned, ro.until = $from_until
        
        WITH ro

        MATCH (to_address:Account {address: $to_address}), (nft:NFT {identifier: $identifier,collection_name: $collection_name})
        MERGE (to_address)-[r:OWNED]->(nft)
        ON CREATE SET r.currently_owned = $yes_owned, r.from = $time_stamp, r.until = $to_until
        ON MATCH SET r.currently_owned = $yes_owned, r.from = $time_stamp, r.until = $to_until    

        """
        #For transfer trx
        query2_transfer = """
        MATCH (from_address:Account {address: $from_address}),(to_address:Account {address: $to_address})
        CREATE (from_address)-[:TRANSACTED {
        event_type: $event_type,
        collection_name: $collection_name,
        transaction_hash: $transaction_hash,
        transaction_timestamp: $time_stamp,
        identifier: $identifier
        }]->(to_address)
        """
        #For sale
        query2_sale = """
        MATCH (from_address:Account {address: $from_address}),(to_address:Account {address: $to_address})
        CREATE (from_address)-[:TRANSACTED {
        event_type: $event_type,
        collection_name: $collection_name,
        transaction_hash: $transaction_hash,
        transaction_timestamp: $time_stamp,
        identifier: $identifier,
        transaction_value: $transaction_value,
        transaction_token_symbol: $transaction_token_symbol,
        transaction_token_decimals: $transaction_token_decimals,
        transaction_token: $transaction_token
        }]->(to_address)
        """
        self.db.run_query("neo4j",query0,parameters={"to_address":data['to_address'],
                    "from_address" : data['from_address'],
                    "mock_data" : mock_data})

        self.db.run_query("neo4j",query1,parameters={"to_address":data['to_address'],
                    "from_address" : data['from_address'],
                    "time_stamp":data['event_timestamp'],
                    "not_owned" : False,
                    "from_until" : data['event_timestamp'],
                    "to_until" : 0,
                    "yes_owned" :True,
                    "identifier": data['nft']['identifier'],
                    "collection_name" : data['nft']['collection']})
        
        if data['event_type'] == "transfer":
            #For transfer
            self.db.run_query("neo4j",query2_transfer,parameters={"to_address":data['to_address'],
                        "from_address" : data['from_address'],
                        "transaction_hash" : data['transaction'],
                        "collection_name" : data['nft']['collection'],
                        "time_stamp":data['event_timestamp'],
                        "event_type" : "Transfer",
                        "identifier": data['nft']['identifier']})
        else: #For sale
            #For sale
            if data['payment'] != None:
                self.db.run_query("neo4j",query2_sale,parameters={"to_address":data['to_address'],
                            "from_address": data['from_address'],
                            "collection_name" : data['nft']['collection'],
                            "transaction_hash" : data['transaction'],
                            "time_stamp":data['event_timestamp'],
                            "transaction_value" : data['payment']['quantity'],
                            "transaction_token_symbol" : data['payment']['symbol'],
                            "transaction_token_decimals" : data['payment']['decimals'],
                            "transaction_token" : data['payment']['token_address'],
                            "event_type" : "Sale and Transfer",
                            "identifier": data['nft']['identifier']})
            else:
                self.db.run_query("neo4j",query2_sale,parameters={"to_address":data['to_address'],
                            "from_address": data['from_address'],
                            "collection_name" : data['nft']['collection'],
                            "transaction_hash" : data['transaction'],
                            "time_stamp":data['event_timestamp'],
                            "transaction_value" : 0,
                            "transaction_token_symbol" : "unknown",
                            "transaction_token_decimals" : "unknown",
                            "transaction_token" : "unknown",
                            "event_type" : "Sale and Transfer",
                            "identifier": data['nft']['identifier']})
        
    
    def get_data_from_opensea(self,collection_name,current_time_unix,collection_address,ids_list):
        """
        Insert new transaction or transfer record into neo4j database.   
        Skip updating if the last update block is close to current blocktime(difference less 2 days).
        Parameters:
        - data: dict
            transaction data.
        - collection_name: str
            target collection.
        - current_time_unix: str
            time to stop update in unix.
        - collection_address:str
            contract address of NFT collection.
        - ids_list:dict
            dict storing identifier and last update blocktime.
        """
        logging.info(f"Current time in unix:{current_time_unix}")
        for id_info in ids_list:
            last_updated_unix=int(id_info['updated_at'])
            if (int(current_time_unix) - int(last_updated_unix))>= (2 * 86400):
                try:
                    result,status,status_code = query_api.query_nft_trx_data(collection_contract=collection_address,
                                                                             identifier=id_info['id'],
                                                                             after=last_updated_unix,
                                                                             till=current_time_unix)
                    if(len(result['asset_events']) != 0):
                        for one in result['asset_events']:
                            self.insert_new_trx_data(one)
                        break
                except Exception as err: #Error handle
                    logging.error(err)
                        
                while result['next'] != "":
                    try:
                        result,status,status_code = query_api.query_nft_trx_data(collection_contract=collection_address,
                                                                             identifier=id_info['id'],
                                                                             after=last_updated_unix,
                                                                             till=current_time_unix,
                                                                             cursor=result['next'])
                        if(len(result['asset_events']) != 0):
                            for one in result['asset_events']:
                                self.insert_new_trx_data(one)
                    except Exception as err: #Error handle
                        logging.error(err)
                
                if status_code == 200:   
                    logging.info(f"Updating collection={collection_name}, identifier={id_info['id']}, last updated= {current_time_unix} done...")
                    # print("Update one node fininsshed!",collection_name,id_info['id'],current_time_unix)
                    self.update_nft_node(collection_name,id_info['id'],current_time_unix)
            else:
                logging.info(f"Skipping collection={collection_name}, identifier={id_info['id']}, last updated unix= {last_updated_unix} done...")
                # print(f"Current block number:{current_blockNumber},last update blocknubmer={last_updated_blockNumber}")
            
    def update_nft_node(self,collection_name,identifier,updated_at):
        """
        Update the NFT node .
        Parameters:
        - updated_at: str
            latest update time.
        - collection_name: str
            related collection.
        - identifier: str
            target which NFT to update.
        """
        query = f"""
        MATCH(n:NFT)
        WHERE n.collection_name = "{collection_name}" AND n.identifier='{identifier}'
        SET n.updated_at = '{updated_at}'
        """
        self.db.run_query('neo4j',query)
           
    def update_update_info(self,latest_block_time,collection_name):
        """
        Update the node of update info.
        Parameters:
        - latest_block_time: str
            last update time.
        - collection_name: str
            related collection.
        """
        query = f"""
        MATCH(n:Update_Info)
        WHERE n.collection_name = "{self.get_processed_collection_name(collection_name)}"
        SET n.updated_at = '{latest_block_time}'
        """
        self.db.run_query('neo4j',query)
        
    def get_last_update(self):
        """
        Retrieve last update time.
        """
        query = f"""
        MATCH(n:Update_Info)
        RETURN n.collection_name AS name, n.updated_at as update_time
        """
        result = self.db.run_query('neo4j', query)
        test = [{"Collection_name": one['name'],'Last_updated_at':one['update_time']}  for one in result]
        return test
        # result = result[0]['n']._properties
        

    def get_processed_collection_name(self,collection_name):
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
        
        
    def get_update_frequency(self,collection_name):
        query = f"""
        MATCH(n:Update_Info)
        WHERE n.collection_name = '{self.get_processed_collection_name(collection_name)}'
        RETURN n.update_frequency AS frequency
        """
        result = self.db.run_query('neo4j', query)
        if len(result) > 0:
            return result[0]['frequency']
        else:
            return 86000
        

        # test = [{"Collection_name": one['name'],'Last_updated_at':one['update_time']}  for one in result]
        # return test
        
