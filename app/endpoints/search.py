from fastapi import APIRouter, Query, HTTPException
import logging
from app.neo4j_response_model.neo4j_response_model import FindAccountResponse, FindNFTResponse
from app.neo4j_access.search_functionality import SearchLogic
from app.exceptions.node_not_found import NodeNotFoundException
from fastapi_cache.decorator import cache

"""
Author: Valentin Leuthe 
"""

search_router = APIRouter()

@search_router.get("/address", response_model=FindAccountResponse)
@cache(namespace="search")
def find_address(
    address:str = Query("", description="owner's address to be searched")
    ):

    """
    Search and owner. Retrieve the owner together with its first-order neighbors (Owners and NFTs) and the 
    relationships between the searched owner and his first-order neighbors.

    Raises:
    - HTTPException: If the address wasn't found.
    """

    logging.info(f"search Owner with address {address}")

    search_logic = SearchLogic()
    try:
        return search_logic.find_account(address)
    except NodeNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))


@search_router.get("/nft", response_model=FindNFTResponse)
@cache(namespace="search")
def find_nft(
    identifier:str = Query("", description="Identifier of the NFT to be searched"), 
    collection:str = Query("", description="collection of the NFT to be searched")
    ):

    """
    Search and NFT. Retrieve the NFT together with its first-order neighbors (Owners) and the 
    relationships between the searched NFT and his first-order neighbors.

    Raises:
    - HTTPException: If the NFT (identifier + collection) wasn't found.
    """

    logging.info(f"search NFT with identifier {identifier} and collection {collection}")

    search_logic = SearchLogic()
    try:
        return search_logic.find_nft(identifier, collection)
    except NodeNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
