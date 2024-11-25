from fastapi import APIRouter, Query, HTTPException
import logging
from typing import List
from app.neo4j_response_model.neo4j_response_model import CentralityResponse
from app.neo4j_access.centrality_functionality import CentralityLogic
from app.exceptions.not_exists import NotExistsException
from fastapi_cache.decorator import cache


"""
Author: Valentin Leuthe 
"""

centrality_router = APIRouter()

@centrality_router.get("/degree", response_model=CentralityResponse)
@cache(namespace="centrality")
def get_centrality(
    limit: int = Query(500, description="number of most central nodes to be returned"),
    collection: List[str] = Query(..., description="collection(s) that should be considered: boredapes or degods")
    ):

    """
    Retrieve the most central nodes (NFT and Owner) within the entire network of the selected collections

    Raises:
    - HTTPException: If one of the selected collections doesn't exist
    """

    logging.info(f"get_centrality with limit = {limit} and collection(s) = {collection}")
    centrality_logic = CentralityLogic()
    try:
        return centrality_logic.get_most_central_nodes(limit, collection)
    except NotExistsException as e:
        raise HTTPException(status_code=404, detail=str(e))
