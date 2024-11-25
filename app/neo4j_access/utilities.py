from typing import List
from app.exceptions.not_exists import NotExistsException

"""
Author: Valentin Leuthe 
"""

class Utilities:

    def get_etherscan_url_address(self, hash: str):
        base_path = "https://etherscan.io/address/"
        return base_path + hash
    
    def get_etherscan_url_transaction(self, hash: str):
        base_path = "https://etherscan.io/tx/"
        return base_path + hash
    
    def get_opensea_url(self, collection: str, identifier: str):
        if collection.upper() == "DEGODS-ETH":
            base_path = "https://opensea.io/assets/ethereum/0x8821bee2ba0df28761afff119d66390d594cd280/"
        else:
            base_path = "https://opensea.io/assets/ethereum/0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d/"

        return base_path + str(identifier)
    

    def get_collection(self, collection: List[str]):

        """
        method turns the collection as it comes as a parameter from the endpoint 
        into the correct form as it is required for the database to get the correct values. 

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
            return "degods-eth"
        elif len(collection_upper) == 2 and "BOREDAPES" in collection_upper and "DEGODS" in collection_upper:
            return "all"
        else:
            raise NotExistsException(f"Collection {collection} does not exist")