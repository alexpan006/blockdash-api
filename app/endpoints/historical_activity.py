from fastapi import APIRouter, Query, HTTPException
from fastapi_cache.decorator import cache
from typing import List
import logging
from app.neo4j_response_model.neo4j_response_model import HistoryResponse, HistoryResponseCollectionDistribution
from app.neo4j_access.history_functionality import HistoryLogic

"""
Author: Valentin Leuthe 
"""

history_router = APIRouter()

year_from_description = "The start year for the temporal filter."
year_to_description = "The end year for the temporal filter."
month_from_description = "The start month for the temporal filter."
month_to_description = "The end month for the temporal filter."
collection_description = "collection name = boredapes or degods"

@history_router.get("/transaction", response_model=HistoryResponse)
@cache(namespace="history-transaction")
def get_transaction_history(
    year_from: int = Query(2024, description=year_from_description),
    year_to: int = Query(2024, description=year_to_description),
    month_from: int = Query(1, description=month_from_description),
    month_to: int = Query(1, description=month_to_description),
    collection: List[str] = Query(..., description=collection_description)
):
    
    """
    Creates a history for the number of transactions on a daily basis between the selected dates within 
    the selected communities.
    Returns a list of dates and a list of respective number of transactions on this date.
    """

    logging.info(f"""request transaction history for ({year_from}, {month_from}) until ({year_to}, {month_to})
                 considering the collection(s): {collection}""")

    history_logic = HistoryLogic()
    return history_logic.get_transaction_history(year_from, year_to, month_from, month_to, collection)

@history_router.get("/mint", response_model=HistoryResponse)
@cache(namespace="history-mint")
def get_transaction_history(
    year_from: int = Query(2024, description=year_from_description),
    year_to: int = Query(2024, description=year_to_description),
    month_from: int = Query(1, description=month_from_description),
    month_to: int = Query(1, description=month_to_description),
    collection: List[str] = Query(..., description=collection_description)
):
    
    """
    Creates a history for the number of mint events on a daily basis between the selected dates within 
    the selected communities.
    Returns a list of dates and a list of respective number of mint events on this date.
    """

    logging.info(f"""request mint history for ({year_from}, {month_from}) until ({year_to}, {month_to})
                 considering the collection(s): {collection}""")

    history_logic = HistoryLogic()
    return history_logic.get_mint_history(year_from, year_to, month_from, month_to, collection)


@history_router.get("/active_user", response_model=HistoryResponse)
@cache(namespace="history-active-user")
def get_active_user_history(
    year_from: int = Query(2024, description=year_from_description),
    year_to: int = Query(2024, description=year_to_description),
    month_from: int = Query(1, description=month_from_description),
    month_to: int = Query(1, description=month_to_description),
    collection: List[str] = Query(..., description=collection_description),
    relation_type: List[str] = Query(..., description="type of relationship: transacted and/or mint")
):
    
    """
    Creates a history of active users on the selected collection on a daily basis. With the relation_type
    it can be defined what should be considered for an account to be active: either transaction events 
    OR mint events or both together.

    Raises:
    - HTTPException: if the relation_type doesn't exist.
    """

    logging.info(f"""request active user history for ({year_from}, {month_from}) until ({year_to}, {month_to})
                 considering the collection(s): {collection} and relationship type(s): {relation_type}""")

    history_logic = HistoryLogic()
    lowercase_relation_type = [type.lower() for type in relation_type]
    # based on the selected relation_type, a different function is caled considering only the selected types
    if "transacted" in lowercase_relation_type and "mint" in lowercase_relation_type:
        logging.info("returning the active users based on transaction and mint event")
        return history_logic.get_active_users_history(year_from, year_to, month_from, month_to, collection) 
    elif "transacted" in lowercase_relation_type:
        logging.info("returning the active users based on transaction event")
        return history_logic.get_active_users_transacting(year_from, year_to, month_from, month_to, collection)
    elif "mint" in lowercase_relation_type:
        logging.info("returning the active users based on mint event")
        return history_logic.get_active_users_mint(year_from, year_to, month_from, month_to, collection)
    else:
        raise HTTPException(status_code=404, detail="relationship type wrong or doesn't exist")
    
@history_router.get("/collection_distribution", response_model=HistoryResponseCollectionDistribution)
@cache(namespace="history-collection-distribution")
def get_collection_distribution(
    year_from: int = Query(2024, description=year_from_description),
    year_to: int = Query(2024, description=year_to_description),
    month_from: int = Query(1, description=month_from_description),
    month_to: int = Query(1, description=month_to_description),
):
    
    """
    Creates a history of the distribution of the selected collection on a daily basis. 
    Raises:
    - HTTPException: if the relation_type doesn't exist.
    """

    logging.info(f"""request collection distribution history for ({year_from}, {month_from}) until ({year_to}, {month_to})""")

    history_logic = HistoryLogic()
    # based on the selected relation_type, a different function is caled considering only the selected types
    return history_logic.get_collection_distribution(year_from, year_to, month_from, month_to)
