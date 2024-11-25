from fastapi import APIRouter, Query, HTTPException
from app.neo4j_access.equality_functionality import EqualityMeasurements
from app.neo4j_response_model.neo4j_response_model import HistoryResponse
from typing import List
import logging
from fastapi_cache.decorator import cache

"""
Author: Valentin Leuthe 
"""

equality_router = APIRouter()
year_from_description = "The start year for the temporal filter."
year_to_description = "The end year for the temporal filter."
month_from_description = "The start month for the temporal filter."
month_to_description = "The end month for the temporal filter."
collection_description = "collection name = boredapes or degods"

@equality_router.get("/overall")
@cache(namespace="equality-overall")
def get_ranking(
    year_from: int = Query(2024, description=year_from_description),
    year_to: int = Query(2024, description=year_to_description),
    month_from: int = Query(1, description=month_from_description),
    month_to: int = Query(1, description=month_to_description),
    relation_type: List[str] = Query(..., description="type of relationship: transacted or mint"),
    collection: List[str] = Query(..., description=collection_description)
    ):

    """
    Calculates the overall GINI coefficient either for Transaction or Mint within the defined time period.
    (This is not possible for the owned relationship)

    Raises:
    - HTTPException: if the relation_type doesn't exist.
    """

    equality_measurement = EqualityMeasurements()
    lowercase_relation_type = [type.lower() for type in relation_type]
    if "transacted" in lowercase_relation_type:
        logging.info("returning the gini coefficient based on transaction event")
        return equality_measurement.get_gini_transaction(collection, year_from, year_to, month_from, month_to)
    elif "mint" in lowercase_relation_type:
        logging.info("returning the gini coefficient based on mint event")
        return equality_measurement.get_gini_mint(collection, year_from, year_to, month_from, month_to)
    else:
        raise HTTPException(status_code=404, detail="relationship type wrong or doesn't exist")
    

@equality_router.get("/history", response_model=HistoryResponse)
@cache(namespace="history")
def get_ranking(
    year_from: int = Query(2024, description=year_from_description),
    year_to: int = Query(2024, description=year_to_description),
    month_from: int = Query(1, description=month_from_description),
    month_to: int = Query(1, description=month_to_description),
    relation_type: List[str] = Query(..., description="type of relationship: transacted or mint or owned"),
    collection: List[str] = Query(..., description=collection_description)
    ):

    """
    Calculates the GINI coefficient for every month within the defined time frame on the selected collection. 
    With the relation_type it can be defined for which relationship type the GINI coefficient should be calculated: 
    either transaction or mint or owned.

    Raises:
    - HTTPException: if the relation_type doesn't exist.
    """

    equality_measurement = EqualityMeasurements()
    lowercase_relation_type = [type.lower() for type in relation_type]
    if "owned" in lowercase_relation_type:
        logging.info("gini history for ownership")
        return equality_measurement.get_gini_ownership_history(collection, year_from, year_to, month_from, month_to)
    elif "transacted" in lowercase_relation_type:
        logging.info("gini history for transaction events")
        return equality_measurement.get_gini_transaction_history(collection, year_from, year_to, month_from, month_to)
    elif "mint" in lowercase_relation_type:
        logging.info("gini history for mint events")
        return equality_measurement.get_gini_mint_history(collection, year_from, year_to, month_from, month_to)
    else:
        raise HTTPException(status_code=404, detail="relationship type wrong or doesn't exist")
    
@equality_router.get("/nakamoto_overall")
@cache(namespace="equality-overall")
def get_nakamoto_coefficient(
    year_from: int = Query(2024, description=year_from_description),
    year_to: int = Query(2024, description=year_to_description),
    month_from: int = Query(1, description=month_from_description),
    month_to: int = Query(1, description=month_to_description),
    relation_type: List[str] = Query(..., description="type of relationship: transacted or mint"),
    collection: List[str] = Query(..., description=collection_description)
    ):

    """
    Calculates the Nakamoto coefficient either for Transaction or Mint within the defined time period.
    (This is not possible for the owned relationship)

    Raises:
    - HTTPException: if the relation_type doesn't exist.
    """

    equality_measurement = EqualityMeasurements()
    lowercase_relation_type = [type.lower() for type in relation_type]

    if "transacted" in lowercase_relation_type:
        logging.info("returning the nakamoto coefficient based on transaction event")
        return equality_measurement.get_nakamoto_transaction(collection, year_from, year_to, month_from, month_to)
    elif "mint" in lowercase_relation_type:
        logging.info("returning the nakamoto coefficient based on mint event")
        return equality_measurement.get_nakamoto_mint(collection, year_from, year_to, month_from, month_to)
    else:
        raise HTTPException(status_code=404, detail="relationship type wrong or doesn't exist")
    

@equality_router.get("/nakamoto_history", response_model=HistoryResponse)
@cache(namespace="history")
def get_nakamoto_coefficient(
    year_from: int = Query(2024, description=year_from_description),
    year_to: int = Query(2024, description=year_to_description),
    month_from: int = Query(1, description=month_from_description),
    month_to: int = Query(1, description=month_to_description),
    relation_type: List[str] = Query(..., description="type of relationship: transacted or mint or owned"),
    collection: List[str] = Query(..., description=collection_description)
    ):

    """
    Calculates the Nakamoto coefficient for every month within the defined time frame on the selected collection.
    With the relation_type it can be defined for which relationship type the Nakamoto coefficient should be calculated:
    either transaction or mint or owned.

    Raises:
    - HTTPException: if the relation_type doesn't exist.
    """

    equality_measurement = EqualityMeasurements()
    lowercase_relation_type = [type.lower() for type in relation_type]

    if "owned" in lowercase_relation_type:
        logging.info("nakamoto history for ownership")
        return equality_measurement.get_nakamoto_ownership_history(collection, year_from, year_to, month_from, month_to)
    elif "transacted" in lowercase_relation_type:
        logging.info("nakamoto history for transaction events")
        return equality_measurement.get_nakamoto_transaction_history(collection, year_from, year_to, month_from, month_to)
    elif "mint" in lowercase_relation_type:
        logging.info("nakamoto history for mint events")
        return equality_measurement.get_nakamoto_mint_history(collection, year_from, year_to, month_from, month_to)
    else:
        raise HTTPException(status_code=404, detail="relationship type wrong or doesn't exist")