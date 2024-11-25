from app.db import get_db
from app.exceptions.not_exists import NotExistsException
from typing import List
from app.neo4j_access.utilities import Utilities
import logging

class RankLogic:

    """
    The class includes the logic for the ranking endpoint. 

    Methods:
    --------
    - get_query(self, scope: str, collection: str, limit: int, year_from: int, year_to: int, month_from: int, month_to: int)
        identifies the correct query to get the ranking that is requested
    - get_ranking(self, scope: str, collection: List[str], limit: int, year_from: int, year_to: int, month_from: int, month_to: int)
        creates the ranking based on the correct query

    Author
    ------
    Valentin Leuthe
    """

    def get_query(self, scope: str, collection: str, limit: int, year_from: int, year_to: int, month_from: int, month_to: int):
        
        """
        Based on the scope for the ranking and the collection considered, the method identifies and returns the correct
        query for the ranking. 

        Parameters:
        - scope: str 
            specifies the type of ranking: 
                "Account_Transaction" = rank owners based on transaction-volume, 
                "Concentration_Ownership" = rank owners based on their number of currently owned NFT, 
                "Contribution" = rank owners based on their contribution for a collection (how many NFT they minted), 
                "Ownership_Changes" = rank NFT based on the amount of ownership changes
        - collection: List[str]
            collection name = „boredapes“ or „degods
        - limit: int 
            The maximum number of items to return
        - year_from: int 
            The start year for the ranking filter
        - year_to: int 
            The end year for the ranking filter
        - month_from: int
            The start month for the ranking filter
        - month_to: int
            The end month for the ranking filter

        Raises:
            NotExistException: in case there doesn't exist a ranking for the choosen scope
        """

        logging.info("identify the correct query for the ranking")

        # rank owners based on transaction-volume
        if scope.upper() == "ACCOUNT_TRANSACTION":
            # in case no collection is specified, no filter on collection is applied
            if collection == "all":
                return f"""
                MATCH (a:Account)
                MATCH (a)-[r:TRANSACTED]-()
                WHERE r.transaction_timestamp IS NOT NULL 
                AND ((datetime({{epochSeconds: r.transaction_timestamp}}).year = {year_from}
                    AND datetime({{epochSeconds: r.transaction_timestamp}}).year = {year_to}
                    AND datetime({{epochSeconds: r.transaction_timestamp}}).month >= {month_from}
                    AND datetime({{epochSeconds: r.transaction_timestamp}}).month <= {month_to})
                OR 
                    (datetime({{epochSeconds: r.transaction_timestamp}}).year > {year_from}
                    AND datetime({{epochSeconds: r.transaction_timestamp}}).year < {year_to})
                OR
                    (datetime({{epochSeconds: r.transaction_timestamp}}).year = {year_from}
                    AND datetime({{epochSeconds: r.transaction_timestamp}}).year < {year_to}
                    AND datetime({{epochSeconds: r.transaction_timestamp}}).month >= {month_from})
                OR
                    (datetime({{epochSeconds: r.transaction_timestamp}}).year = {year_to}
                    AND datetime({{epochSeconds: r.transaction_timestamp}}).year > {year_from}
                    AND datetime({{epochSeconds: r.transaction_timestamp}}).month <= {month_to}))
                RETURN a.address as Identifier, count(r) AS count
                ORDER BY count DESC
                LIMIT {limit}
                """
            else:
                return f"""
                MATCH (a:Account)
                MATCH (a)-[r:TRANSACTED]-()
                WHERE r.collection_name = "{collection}"
                AND r.transaction_timestamp IS NOT NULL 
                AND ((datetime({{epochSeconds: r.transaction_timestamp}}).year = {year_from}
                    AND datetime({{epochSeconds: r.transaction_timestamp}}).year = {year_to}
                    AND datetime({{epochSeconds: r.transaction_timestamp}}).month >= {month_from}
                    AND datetime({{epochSeconds: r.transaction_timestamp}}).month <= {month_to})
                OR 
                    (datetime({{epochSeconds: r.transaction_timestamp}}).year > {year_from}
                    AND datetime({{epochSeconds: r.transaction_timestamp}}).year < {year_to})
                OR
                    (datetime({{epochSeconds: r.transaction_timestamp}}).year = {year_from}
                    AND datetime({{epochSeconds: r.transaction_timestamp}}).year < {year_to}
                    AND datetime({{epochSeconds: r.transaction_timestamp}}).month >= {month_from})
                OR
                    (datetime({{epochSeconds: r.transaction_timestamp}}).year = {year_to}
                    AND datetime({{epochSeconds: r.transaction_timestamp}}).year > {year_from}
                    AND datetime({{epochSeconds: r.transaction_timestamp}}).month <= {month_to}))
                RETURN a.address as Identifier, count(r) AS count
                ORDER BY count DESC
                LIMIT {limit}
                """
        # rank owners based on their number of currently owned NFT
        elif scope.upper() == "CONCENTRATION_OWNERSHIP":
            # in case no collection is specified, no filter on collection is applied
            if collection == "all":
                return f"""
                MATCH (a:Account)
                MATCH (a)-[r:OWNED]-(n)
                WHERE r.from IS NOT NULL
                AND r.until IS NOT NULL
                AND datetime({{epochSeconds: r.from}}).year >= {year_from}
                AND (r.currently_owned = true OR (r.currently_owned = false AND datetime({{epochSeconds: r.until}}).year <= {year_to}))
                AND datetime({{epochSeconds: r.from}}).month >= {month_from}
                AND (r.currently_owned = true OR (r.currently_owned = false AND datetime({{epochSeconds: r.until}}).month <={month_to}))
                RETURN a.address as Identifier, count(r) AS count
                ORDER BY count DESC
                LIMIT {limit}
                """
            else:
                return f"""
                MATCH (a:Account)
                MATCH (a)-[r:OWNED]-(n)
                WHERE n.collection_name = "{collection}"
                AND r.from IS NOT NULL
                AND r.until IS NOT NULL
                AND datetime({{epochSeconds: r.from}}).year >= {year_from}
                AND (r.currently_owned = true OR (r.currently_owned = false AND datetime({{epochSeconds: r.until}}).year <= {year_to}))
                AND datetime({{epochSeconds: r.from}}).month >= {month_from}
                AND (r.currently_owned = true OR (r.currently_owned = false AND datetime({{epochSeconds: r.until}}).month <={month_to}))
                RETURN a.address as Identifier, count(r) AS count
                ORDER BY count DESC
                LIMIT {limit}
                """
        # rank owners based on their contribution for a collection (how many NFT they minted)
        elif scope.upper() == "CONTRIBUTION":
            # in case no collection is specified, no filter on collection is applied
            if collection == "all":
                return f"""
                MATCH (a:Account)
                MATCH (a)-[r:MINT]-(n)
                WHERE r.date IS NOT NULL 
                AND ((datetime({{epochSeconds: r.date}}).year = {year_from}
                    AND datetime({{epochSeconds: r.date}}).year = {year_to}
                    AND datetime({{epochSeconds: r.date}}).month >= {month_from}
                    AND datetime({{epochSeconds: r.date}}).month <= {month_to})
                OR 
                    (datetime({{epochSeconds: r.date}}).year > {year_from}
                    AND datetime({{epochSeconds: r.date}}).year < {year_to})
                OR
                    (datetime({{epochSeconds: r.date}}).year = {year_from}
                    AND datetime({{epochSeconds: r.date}}).year < {year_to}
                    AND datetime({{epochSeconds: r.date}}).month >= {month_from})
                OR
                    (datetime({{epochSeconds: r.date}}).year = {year_to}
                    AND datetime({{epochSeconds: r.date}}).year > {year_from}
                    AND datetime({{epochSeconds: r.date}}).month <= {month_to}))
                RETURN a.address as Identifier, count(r) AS count
                ORDER BY count DESC
                LIMIT {limit}
                """
            else:
                return f"""
                MATCH (a:Account)
                MATCH (a)-[r:MINT]-(n)
                WHERE n.collection_name = "{collection}" 
                AND r.date IS NOT NULL 
                AND ((datetime({{epochSeconds: r.date}}).year = {year_from}
                    AND datetime({{epochSeconds: r.date}}).year = {year_to}
                    AND datetime({{epochSeconds: r.date}}).month >= {month_from}
                    AND datetime({{epochSeconds: r.date}}).month <= {month_to})
                OR 
                    (datetime({{epochSeconds: r.date}}).year > {year_from}
                    AND datetime({{epochSeconds: r.date}}).year < {year_to})
                OR
                    (datetime({{epochSeconds: r.date}}).year = {year_from}
                    AND datetime({{epochSeconds: r.date}}).year < {year_to}
                    AND datetime({{epochSeconds: r.date}}).month >= {month_from})
                OR
                    (datetime({{epochSeconds: r.date}}).year = {year_to}
                    AND datetime({{epochSeconds: r.date}}).year > {year_from}
                    AND datetime({{epochSeconds: r.date}}).month <= {month_to}))
                RETURN a.address as Identifier, count(r) AS count
                ORDER BY count DESC
                LIMIT {limit}
                """
        # rank NFT based on the amount of ownership changes
        elif scope.upper() == "OWNERSHIP_CHANGES":
            # a collection always need to be stated in case NFTs are returned (part of the key for NFT)
            if collection == "all":
                raise NotExistsException("Collection is part of the key for NFTs and a single one need to be specified!")

            return f"""
            MATCH (n:NFT)
            MATCH (n)-[r:OWNED]-()
            WHERE n.collection_name = "{collection}"
            AND r.from IS NOT NULL
            AND r.until IS NOT NULL
            AND datetime({{epochSeconds: r.from}}).year >= {year_from} 
            AND (r.currently_owned = true OR (r.currently_owned = false AND datetime({{epochSeconds: r.until}}).year <= {year_to}))
            AND datetime({{epochSeconds: r.from}}).month >= {month_from} 
            AND (r.currently_owned = true OR (r.currently_owned = false AND datetime({{epochSeconds: r.until}}).month <= {month_to}))
            RETURN n.identifier as Identifier, count(r) AS count
            ORDER BY count DESC
            LIMIT {limit}
            """
        else:
            raise NotExistsException("ranking not available") 
        

    def get_ranking(self, scope: str, collection: List[str], limit: int, year_from: int, year_to: int, month_from: int, month_to: int):
        
        """
        Method to create the ranking based on the correct query. 

        Parameters:
        - scope: str 
            specifies the type of ranking: 
                "Account_Transaction" = rank owners based on transaction-volume, 
                "Concentration_Ownership" = rank owners based on their number of currently owned NFT, 
                "Contribution" = rank owners based on their contribution for a collection (how many NFT they minted), 
                "Ownership_Changes" = rank NFT based on the amount of ownership changes
        - collection: List[str]
            collection name = „boredapes“ or „degods
        - limit: int 
            The maximum number of items to return
        - year_from: int 
            The start year for the ranking filter
        - year_to: int 
            The end year for the ranking filter
        - month_from: int
            The start month for the ranking filter
        - month_to: int
            The end month for the ranking filter
        """

        logging.info("START: creating the ranking")

        db = get_db()
        utilities = Utilities()

        collection_pro = utilities.get_collection(collection)

        rank_query = self.get_query(scope, collection_pro, limit, year_from, year_to, month_from, month_to)

        rank_query_result = db.run_query('neo4j', rank_query)
        logging.info("Ranking completed")

        # prepare the result in the form of the response model
        ranking = [
            {
            "identifier": record["Identifier"],
            "count": record["count"]
            } for record in rank_query_result]
        
        final_result = {
            "ranking": ranking
        }

        return final_result

        

        