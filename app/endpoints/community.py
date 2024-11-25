from fastapi import APIRouter, Query, HTTPException
from typing import List
from app.neo4j_response_model.neo4j_response_model import NewCommunityResponse, CommunitySummaryResponse
from app.exceptions.not_exists import NotExistsException
import logging
from app.neo4j_access.community_functionality import CommunityLogic
from fastapi_cache.decorator import cache
from app.neo4j_access.community_detection import CommunityDetection

"""
Author: Valentin Leuthe 
"""

community_router = APIRouter()

@community_router.get("/runCommunity")
def test_run_community(
    limit:int = 100,
    collection_name: str ="boredapeyachtclub"):  # boredapeyachtclub, degods-eth, complete
    """
    Method for testing purpose to run the Louvain community detection on the specified collection. It will only 
    store the 100 biggest communities.
    
    Parameter:
    - limit: int
        only to record the x biggest communities
    - collection_name: str
        community detection will be run with given the collection name

    Raise
    - CollectionFoundException if there is no such collection in the database
    """
    com_detection = CommunityDetection()
    # db= get_db()
    # return db.run_community_detection(limit,collection_name)
    return com_detection.run_community_detection(limit,collection_name)


@community_router.get("/summary", response_model=CommunitySummaryResponse)
@cache(namespace="community")
def get_summary(
    limit:int = Query(10, description="limit the number of biggest communities returned"),
    scope: str = Query("", description="scope for the community detection: <ownership>, <transaction>, <all>"),
    collection: List[str] = Query(..., description="specifies the collection for the community detection")
    ):

    """
    Retrieve the biggest communities as a summary. One node for each community indicating the number of 
    nodes inside the community. Plus the number of relationships between the different communities

    Raises:
    - HTTPException: If one of the selected collections doesn't exist
    """

    logging.info(f"get community summary with limit: {limit}, scope:{scope} and collection(s): {collection}")
    community_functionality = CommunityLogic()
    try:
        return community_functionality.get_community_sum(limit, scope, collection)
    except NotExistsException as e:
        raise HTTPException(status_code=404, detail=str(e))

@community_router.get("/single_community", response_model=NewCommunityResponse)
@cache(namespace="community")
def get_single_community(
    community_id: int = Query(0, description="Id of the community to be returned"),
    scope: str = Query("", description="scope for the community detection: <ownership>, <transaction>, <all>"),
    collection: List[str] = Query(..., description="specifies the collection for the community detection"),
    limit: int = Query(1000, description="number of most important nodes of a community to be returned")
    ):

    """
    Retrieve the most central nodes of a selected community together with the relationships between those nodes

    Raises:
    - HTTPException: If one of the selected collections or the community id doesn't exist
    """

    logging.info(f"get {limit} nodes of the community with id: {id}, scope: {scope}, for the collection(s): {collection}")
    community_functionality = CommunityLogic()
    try:
        return community_functionality.get_single_community(community_id, scope, collection, limit)
    except NotExistsException as e:
        raise HTTPException(status_code=404, detail=str(e))

