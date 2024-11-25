from app.db import get_db
from typing import List, Dict
from app.neo4j_access.utilities import Utilities
from datetime import datetime, timedelta
import calendar

class HistoryLogic:

    """
    The class includes the logic for the endpoints which consider the temporal analysis . 

    Methods:
    --------
    - create_history_result(self, year_from: int, year_to: int, month_from: int, month_to: int, date_to_count: Dict)
        Method to create the two output arrays containing dates and the respective count
    - get_transaction_history(self, year_from: int, year_to: int, month_from: int, month_to: int, collection: List[str])
        The method creates a history of amount of transactions in a specified time period in a daily basis
    - get_mint_history(self, year_from: int, year_to: int, month_from: int, month_to: int, collection: List[str])
        The method creates a history of amount of mint events in a specified time period in a daily basis
    - get_active_users_history(self, year_from: int, year_to: int, month_from: int, month_to: int, collection: List[str])
        This method counts the number of active users on a daily basis. A user is acitve if he/she either transacted or minted
    - get_active_users_transacting(self, year_from: int, year_to: int, month_from: int, month_to: int, collection: List[str])
        This method counts the number of users that transacted each day.
    - get_active_users_mint(self, year_from: int, year_to: int, month_from: int, month_to: int, collection: List[str])
        This method counts the number of users that minted and NFT each day.

    Author
    ------
    Valentin Leuthe
    """

    def create_history_result(self, year_from: int, year_to: int, month_from: int, month_to: int, date_to_count: Dict):

        """
        Method to create the two output arrays containing dates and the respective count (e.g. number of transactions) for that day.
        Dates are on a daily basis in the format "Year - Month - Day". 
        It takes the dictonary that's returned by the cypher query containing dates and counts. The method extends the 
        query output by adding 0 for the days that are not included in the query result. This is required in order to 
        later create a plot which considers every day in the choosen time frame. In case there are no transaction, mint or own 
        events for a specific date in the database, the query result just leaves out the day and don't consider a count. 

        
        Parameters:
        - year_from: int
            The start year for the time frame
        - year_to: int
            The end year for the time frame.
        - month_from: int 
            The start month for the time frame.
        - month_to: int 
            The end month for the time frame
        - date_to_count: Dict
            query result, including days and counts 
        """

        #Define the date range
        start_date = datetime(year_from, month_from, 1)

        # Calculate end_date as the last day of the month_to
        last_day_of_month = calendar.monthrange(year_to, month_to)[1]
        end_date = datetime(year_to, month_to, last_day_of_month)

        dates = []
        counts = []

        # Iterate through each date in the specified range, to add a count = 0 for the dates that are not included in the 
        # query result
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            dates.append(date_str)
            # Check if the date is in the original results
            if date_str in date_to_count:
                counts.append(date_to_count[date_str])
            else:
                counts.append(0)
            # Move to the next day
            current_date += timedelta(days=1)

        final_result = {
            "dates": dates,
            "counts": counts
        }

        return final_result
        
    def get_transaction_history(self, year_from: int, year_to: int, month_from: int, month_to: int, collection: List[str]):

        """
        The method creates a history of amount of transactions in a specified time period in a daily basis. Within the 
        time period it returns an array with all the days and one array with the number of transactions for that specific day.
        The result is meant to be plotted in the frontend in the form of a line graph with one data-point for each day. 

        Parameters:
        - year_from: int
            The start year for the time frame
        - year_to: int
            The end year for the time frame.
        - month_from: int 
            The start month for the time frame.
        - month_to: int 
            The end month for the time frame
        - Collection: List[str]
            list of collections for which the relationships are counted 
        """

        db = get_db()
        utilities = Utilities()

        collection_processed = utilities.get_collection(collection)

        # depending whether both collections are selected or not, the collection filter is added in the query
        if collection_processed == "all":
            query = f"""
            MATCH ()-[r:TRANSACTED]->()
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
            WITH date(datetime({{epochSeconds: r.transaction_timestamp}})) AS transaction_date, COUNT(r) AS transaction_count
            RETURN transaction_date, transaction_count
            ORDER BY transaction_date
            """
        else:
            query = f"""
            MATCH ()-[r:TRANSACTED]->()
            WHERE r.collection_name = "{collection_processed}"
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
            WITH date(datetime({{epochSeconds: r.transaction_timestamp}})) AS transaction_date, COUNT(r) AS transaction_count
            RETURN transaction_date, transaction_count
            ORDER BY transaction_date
            """

        query_results = db.run_query('neo4j', query)
        # create a dictionary with date and repsective count 
        date_to_count = {str(result['transaction_date']): result['transaction_count'] for result in query_results}

        final_result = self.create_history_result(year_from, year_to, month_from, month_to, date_to_count)

        return final_result
    
    def get_mint_history(self, year_from: int, year_to: int, month_from: int, month_to: int, collection: List[str]):
        
        """
        The method creates a history of amount of mint events in a specified time period in a daily basis. Within the 
        time period it returns an array with all the days and one array with the number of mint events for that specific day.
        The result is meant to be plotted in the frontend in the form of a line graph with one data-point for each day. 

        Parameters:
        - year_from: int
            The start year for the time frame
        - year_to: int
            The end year for the time frame.
        - month_from: int 
            The start month for the time frame.
        - month_to: int 
            The end month for the time frame
        - Collection: List[str]
            list of collections for which the relationships are counted 
        """

        db = get_db()
        utilities = Utilities()

        collection_processed = utilities.get_collection(collection)

        # depending whether both collections are selected or not, the collection filter is added in the query
        if collection_processed == "all":
            query = f"""
            MATCH ()-[r:MINT]->()
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
            WITH date(datetime({{epochSeconds: r.date}})) AS mint_date, COUNT(r) AS mint_count
            RETURN mint_date, mint_count
            ORDER BY mint_date
            """
        else:
            query = f"""
            MATCH ()-[r:MINT]->(n)
            WHERE n.collection_name = "{collection_processed}" 
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
            WITH date(datetime({{epochSeconds: r.date}})) AS mint_date, COUNT(r) AS mint_count
            RETURN mint_date, mint_count
            ORDER BY mint_date
            """

        query_results = db.run_query('neo4j', query)
        date_to_count = {str(result['mint_date']): result['mint_count'] for result in query_results}

        final_result = self.create_history_result(year_from, year_to, month_from, month_to, date_to_count)

        return final_result

    def get_active_users_history(self, year_from: int, year_to: int, month_from: int, month_to: int, collection: List[str]):

        """
        This method counts the number of active users on a daily basis. A user is acitve if he/she either transacted or minted.
        Within the time period it returns an array with all the days and one array with the number of active users for that specific day.
        The result is meant to be plotted in the frontend in the form of a line graph with one data-point for each day.

        Parameters:
        - year_from: int
            The start year for the time frame
        - year_to: int
            The end year for the time frame.
        - month_from: int 
            The start month for the time frame.
        - month_to: int 
            The end month for the time frame
        - Collection: List[str]
            list of collections in which the users where active in order to be counted as active
        """

        db = get_db()
        utilities = Utilities()

        collection_processed = utilities.get_collection(collection)

        # depending whether both collections are selected or not, the collection filter is added in the query
        if collection_processed == "all": 
            query_transactions = f"""
            MATCH (a:Account)-[r:TRANSACTED]->()
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
            WITH date(datetime({{epochSeconds: r.transaction_timestamp}})) AS date, COLLECT(DISTINCT a) AS accounts
            RETURN date, SIZE(accounts) AS number
            ORDER BY date
            """
             
            query_mint = f"""
            MATCH (a:Account)-[r:MINT]->()
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
            WITH date(datetime({{epochSeconds: r.date}})) AS date, COLLECT(DISTINCT a) AS accounts
            RETURN date, SIZE(accounts) AS number
            ORDER BY date
            """
        else:
            query_transactions = f"""
            MATCH (a:Account)-[r:TRANSACTED]->()
            WHERE r.transaction_timestamp IS NOT NULL 
            AND r.collection_name = "{collection_processed}"
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
            WITH date(datetime({{epochSeconds: r.transaction_timestamp}})) AS date, COLLECT(DISTINCT a) AS accounts
            RETURN date, SIZE(accounts) AS number
            ORDER BY date
            """
             
            query_mint = f"""
            MATCH (a:Account)-[r:MINT]->(n)
            WHERE r.date IS NOT NULL 
            AND n.collection_name = "{collection_processed}" 
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
            WITH date(datetime({{epochSeconds: r.date}})) AS date, COLLECT(DISTINCT a) AS accounts
            RETURN date, SIZE(accounts) AS number
            ORDER BY date
            """

        query_results_transaction = db.run_query('neo4j', query_transactions)
        query_results_mint = db.run_query('neo4j', query_mint)

        #Define the date range
        start_date = datetime(year_from, month_from, 1)

        # Calculate end_date as the last day of the month_to
        last_day_of_month = calendar.monthrange(year_to, month_to)[1]
        end_date = datetime(year_to, month_to, last_day_of_month)

        date_to_count_transaction = {str(result['date']): result['number'] for result in query_results_transaction}
        date_to_count_mint = {str(result['date']): result['number'] for result in query_results_mint}

        dates = []
        counts = []

        # Iterate through each date in the specified range, to add a count = 0 for the dates that are not included in the 
        # query result. Additionally the counts for transaction and mint are summed up
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            dates.append(date_str)
            count_temp = 0
            # Check if the date is in the original results
            if date_str in date_to_count_transaction:
                count_temp += date_to_count_transaction[date_str]
            if date_str in date_to_count_mint:
                count_temp += date_to_count_mint[date_str]    
            counts.append(count_temp)
            # Move to the next day
            current_date += timedelta(days=1)
        
        final_result = {
                "dates": dates,
                "counts": counts
            }

        return final_result
    
    def get_active_users_transacting(self, year_from: int, year_to: int, month_from: int, month_to: int, collection: List[str]):

        """
        This method counts the number of users that transacted each day.
        Within the time period it returns an array with all the days and one array with the number of transacting users for that specific day.
        The result is meant to be plotted in the frontend in the form of a line graph with one data-point for each day.

        Parameters:
        - year_from: int
            The start year for the time frame
        - year_to: int
            The end year for the time frame.
        - month_from: int 
            The start month for the time frame.
        - month_to: int 
            The end month for the time frame
        - Collection: List[str]
            list of collections in which the users where active in order to be counted as active
        """

        db = get_db()
        utilities = Utilities()

        collection_processed = utilities.get_collection(collection)

        # depending whether both collections are selected or not, the collection filter is added in the query
        if collection_processed == "all": 
            query_transactions = f"""
            MATCH (a:Account)-[r:TRANSACTED]->()
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
            WITH date(datetime({{epochSeconds: r.transaction_timestamp}})) AS date, COLLECT(DISTINCT a) AS accounts
            RETURN date, SIZE(accounts) AS number
            ORDER BY date
            """
        else:
            query_transactions = f"""
            MATCH (a:Account)-[r:TRANSACTED]->()
            WHERE r.transaction_timestamp IS NOT NULL 
            AND r.collection_name = "{collection_processed}"
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
            WITH date(datetime({{epochSeconds: r.transaction_timestamp}})) AS date, COLLECT(DISTINCT a) AS accounts
            RETURN date, SIZE(accounts) AS number
            ORDER BY date
            """

        query_results_transaction = db.run_query('neo4j', query_transactions)
        date_to_count_transaction = {str(result['date']): result['number'] for result in query_results_transaction}

        final_result = self.create_history_result(year_from, year_to, month_from, month_to, date_to_count_transaction)

        return final_result
    
    def get_active_users_mint(self, year_from: int, year_to: int, month_from: int, month_to: int, collection: List[str]):

        """
        This method counts the number of users that minted and NFT each day.
        Within the time period it returns an array with all the days and one array with the number of minting users for that specific day.
        The result is meant to be plotted in the frontend in the form of a line graph with one data-point for each day.

        Parameters:
        - year_from: int
            The start year for the time frame
        - year_to: int
            The end year for the time frame.
        - month_from: int 
            The start month for the time frame.
        - month_to: int 
            The end month for the time frame
        - Collection: List[str]
            list of collections in which the users where active in order to be counted as active
        """

        db = get_db()
        utilities = Utilities()

        collection_processed = utilities.get_collection(collection)

        # depending whether both collections are selected or not, the collection filter is added in the query
        if collection_processed == "all": 
            query_mint = f"""
            MATCH (a:Account)-[r:MINT]->()
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
            WITH date(datetime({{epochSeconds: r.date}})) AS date, COLLECT(DISTINCT a) AS accounts
            RETURN date, SIZE(accounts) AS number
            ORDER BY date
            """
        else: 
            query_mint = f"""
            MATCH (a:Account)-[r:MINT]->(n)
            WHERE r.date IS NOT NULL 
            AND n.collection_name = "{collection_processed}" 
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
            WITH date(datetime({{epochSeconds: r.date}})) AS date, COLLECT(DISTINCT a) AS accounts
            RETURN date, SIZE(accounts) AS number
            ORDER BY date
            """

        query_results_mint = db.run_query('neo4j', query_mint)
        date_to_count_mint = {str(result['date']): result['number'] for result in query_results_mint}

        final_result = self.create_history_result(year_from, year_to, month_from, month_to, date_to_count_mint)

        return final_result
    
    def get_collection_distribution(self, year_from: int, year_to: int, month_from: int, month_to: int):
        """
        This method counts the distribution of the collections for both transacted and minted events.
        Within the time period it returns an array with all the collections and one array with the number of transactions for that specific collection.
        The result is meant to be plotted in the frontend in the form of a pie chart/area chart.
        
        Parameters:
        - year_from: int
            The start year for the time frame
        - year_to: int
            The end year for the time frame.
        - month_from: int
            The start month for the time frame.
        - month_to: int
            The end month for the time frame
        """
        db = get_db()

        # Transactions query adjusted for collection filtering
        transacted_query = f"""
            MATCH (a:Account)-[r:TRANSACTED]->()
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
            RETURN r.collection_name AS collection, COUNT(r) AS number
            """

        # Mints query adjusted for collection filtering
        mint_query = f"""
            MATCH (a:Account)-[r:MINT]->(n)
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
            RETURN n.collection_name AS collection, COUNT(r) AS number
            """

        # Execute queries
        query_results_transacted = db.run_query('neo4j', transacted_query)
        query_results_mint = db.run_query('neo4j', mint_query)

        # Merge results from both queries
        collection_counts = {}
        for result in query_results_transacted + query_results_mint:
            collection = result['collection']
            number = result['number']
            if collection in collection_counts:
                collection_counts[collection] += number
            else:
                collection_counts[collection] = number

        # Prepare final results for plotting
        collections = list(collection_counts.keys())
        counts = list(collection_counts.values())

        final_result = {
            "collections": collections,
            "counts": counts
        }

        return final_result