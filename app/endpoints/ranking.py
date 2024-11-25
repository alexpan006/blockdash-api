from fastapi import APIRouter, Query, HTTPException
from app.neo4j_access.ranking_functionality import RankLogic
from app.exceptions.not_exists import NotExistsException
from app.neo4j_response_model.neo4j_response_model import RankingResponse
from typing import List
import logging
from fastapi_cache.decorator import cache

"""
Author: Valentin Leuthe 
"""

ranking_router = APIRouter()

@ranking_router.get("/", response_model=RankingResponse)
@cache(namespace="ranking")
def get_ranking(
    scope: str = Query("", description="Specifies the type of ranking: Account_Transaction, Concentration_Ownership, Contribution, Ownership_Changes"),
    collection: List[str] = Query(..., description="collection name = boredapes or degods"),
    limit: int = Query(10, description="The maximum number of items to return."),
    year_from: int = Query(2024, description="The start year for the ranking filter."), 
    year_to: int = Query(2024, description="The end year for the ranking filter."),
    month_from: int = Query(1, description="The start month for the ranking filter."),
    month_to: int = Query(1, description="The end month for the ranking filter."),
    ):

    """
    Retrieve a ranking based on the provided parameters.

    Raises:
    - HTTPException: If the requested ranking does not exist, a 404 error is raised with a relevant detail message.
    """

    logging.info(f"get the ranking for {scope}")
    ranking_functionality = RankLogic()
    try:
        return ranking_functionality.get_ranking(scope, collection, limit, year_from, year_to, month_from, month_to)
    except NotExistsException as e:
        raise HTTPException(status_code=404, detail=str(e))
