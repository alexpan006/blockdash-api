import logging
from app.db import get_db
from typing import List
import numpy as np
from app.neo4j_access.utilities import Utilities

class EqualityMeasurements:

    """
    The class includes the logic for the endpoints which consider the equality/inequality measurements based on 
    the GINI coefficient. 

    Methods:
    --------
    - gini_coefficient(self, counts_array)
        Calculate the Gini coefficient.
    - get_gini_transaction(self, collection: List[str], year_from: int, year_to: int, month_from: int, month_to: int)
        returns the GINI coefficient based on TRANSACTED relationship for the given time period
    - get_gini_mint(self, collection: List[str], year_from: int, year_to: int, month_from: int, month_to: int)
        returns the GINI coefficient based on MINT relationship for the given time period
    - get_gini_ownership_history(self, collection: List[str], year_from: int, year_to: int, month_from: int, month_to: int)
        returns the GINI coefficient based on OWNED relationship for the given time period on a monthly basis
    - get_gini_transaction_history(self, collection: List[str], year_from: int, year_to: int, month_from: int, month_to: int)
        returns the GINI coefficient based on TRANSACTED relationship for the given time period on a monthly basis
    - get_gini_mint_history(self, collection: List[str], year_from: int, year_to: int, month_from: int, month_to: int)
        returns the GINI coefficient based on MINT relationship for the given time period on a monthly basis

    Author
    ------
    Valentin Leuthe
    """

    # Function to calculate the Gini coefficient
    def gini_coefficient(self, counts_array):
        
        """
        Calculate the Gini coefficient.

        The Gini coefficient is a measure of statistical dispersion intended to represent 
        the asset inequality possessed within a concerned group. 
        A Gini coefficient of 0 expresses perfect equality, where all values are the same 
        (e.g., everyone has the same income). A Gini coefficient of 1 (or 100%) expresses maximal inequality.

        Parameters:
        - counts array: List[int]
            including the number of transactions, mint, owned relationships for every user that has at least one.
            Ordered in ascending order

        Returns:
            float: Gini coefficient (rounded for 4 decimals)
        """

        n = len(counts_array)

        # Calculate the Gini coefficient
        # This step takes some time 
        # adding 0.0000001 to the denominator makes sure that it not devides by 0
        gini_numerator = np.sum(np.abs(counts_array[:, None] - counts_array))
        gini_denominator = 2 * n**2 * np.mean(counts_array) + 0.0000001
    
        gini = gini_numerator / gini_denominator
        
        return gini
    
    def get_gini_transaction(self, collection: List[str], year_from: int, year_to: int, month_from: int, month_to: int):

        """
        This method returns the GINI coefficient based on TRANSACTED relationship for the given time period. 
        It gives insights into the inequality of transaction activity among Owners. 

        Parameter:
        - year_from: int
            The start year for the time period
        - year_to: int
            The end year for the time period.
        - month_from: int 
            The start month for the time period.
        - month_to: int 
            The end month for the time period
        - Collection: List[str]
            list of collections in which the users where active

        Returns:
            float: Gini coefficient (rounded for 4 decimals)
        """
         
        db = get_db()
        utilities = Utilities()

        collection_processed = utilities.get_collection(collection)

        # Query: count the number of transactions for each owner in the time period 
        # If both collections are considered the collection filter is removed
        if collection_processed == "all":
            transaction_count_query = f"""
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
            WITH a.address AS owner, COUNT(r) AS amount
            RETURN amount
            ORDER BY amount ASC
            """ 
        else:
            transaction_count_query = f"""
            MATCH (a:Account)-[r:TRANSACTED]->()
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
            WITH a.address AS owner, COUNT(r) AS amount
            RETURN amount
            ORDER BY amount ASC
            """

        query_result = db.run_query('neo4j', transaction_count_query)

        # in case the query result is empty, return a GINI of -1.0
        if query_result == []:
            return -1.0

        # Extract the transaction counts into a list
        transaction_counts = [record['amount'] for record in query_result]
        # Convert the list into a NumPy array
        transaction_counts_array = np.array(transaction_counts)

        gini = self.gini_coefficient(transaction_counts_array)
    
        return round(gini, 4)

    def get_gini_mint(self, collection: List[str], year_from: int, year_to: int, month_from: int, month_to: int):
         
        """
        This method returns the GINI coefficient based on MINT relationship for the given time period. 
        The Gini coefficient for this relationship measures the inequality in the creation of NFTs among Owners. 

        Parameter:
        - year_from: int
            The start year for the time period
        - year_to: int
            The end year for the time period.
        - month_from: int 
            The start month for the time period.
        - month_to: int 
            The end month for the time period
        - Collection: List[str]
            list of collections for which the users minted

        Returns:
            float: Gini coefficient (rounded for 4 decimals)
        """

        db = get_db()
        utilities = Utilities()

        collection_processed = utilities.get_collection(collection)

        # Query: count the number of transactions for each owner in the time period 
        # If both collections are considered the collection filter is removed
        if collection_processed == "all":
            mint_count_query = f"""
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
            WITH a.address AS owner, COUNT(r) AS amount
            RETURN amount
            ORDER BY amount ASC
            """ 
        else:
            mint_count_query = f"""
            MATCH (a:Account)-[r:MINT]->(n)
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
            WITH a.address AS owner, COUNT(r) AS amount
            RETURN amount
            ORDER BY amount ASC
            """

        query_result = db.run_query('neo4j', mint_count_query)

        # in case the query result is empty, return a GINI of -1.0
        if query_result == []:
            return -1.0

        # Extract the transaction counts into a list
        mint_counts = [record['amount'] for record in query_result]
        # Convert the list into a NumPy array
        mint_counts_array = np.array(mint_counts)

        gini = self.gini_coefficient(mint_counts_array)
    
        return round(gini, 4)
    
    def get_gini_ownership_history(self, collection: List[str], year_from: int, year_to: int, month_from: int, month_to: int):

        """
        This method returns the GINI coefficient based on OWNED relationship for the given time period on a monthly basis. 
        The Gini coefficient for this relationship gives a measure of inequality in NFT ownership.
        It is only possible on a monthly basis and not overall as we're only considering active ownerships and 
        cannot simply count all ownership relationships as they would include the inactive ones as well. 
        A more granular time frame (like weekly or daily) would be possible as well. It would increase accuracy but 
        also complexity as more GINI coefficients would need to be calculated.
        The result is meant to be plotted as a bar-chart in the frontend.

        Parameter:
        - year_from: int
            The start year for the time period
        - year_to: int
            The end year for the time period.
        - month_from: int 
            The start month for the time period.
        - month_to: int 
            The end month for the time period
        - Collection: List[str]
            list of collections to which the considered NFTs belong to 

        Returns 
            two arrays, one for dates in the format Year-Month, and one for the respective GINI coefficient for that date
        """

        db = get_db()
        utilities = Utilities()

        collection_processed = utilities.get_collection(collection)

        current_year = year_from
        current_month = month_from

        dates = []
        gini_scores = []

        # iterate of each month in the specified time frame and calculate a GINI score considering the 
        # relationships of that month
        # For an OWNED relationtion to be considered, it needs to be active in the considered month.
        # Thats the case if the ownership started in that month or before and lasted at least until 
        # this month or is still active. This is made sure by the query below 
        while (current_year < year_to) or (current_year == year_to and current_month <= month_to):

            # queries consider transactions only of the considered month.
            # if only one collection is considered, a collection filter is included
            if collection_processed == "all":
                ownership_count_query = f"""
                MATCH (a)-[r:OWNED]->(n)
                WHERE r.from IS NOT NULL
                    AND r.until IS NOT NULL
                    AND (datetime({{epochSeconds: r.from}}).year < {current_year} 
                        OR (datetime({{epochSeconds: r.from}}).year = {current_year}  AND datetime({{epochSeconds: r.from}}).month <= {current_month}))
                    AND (r.currently_owned = true 
                        OR (r.currently_owned = false AND (datetime({{epochSeconds: r.until}}).year > {current_year}  
                        OR (datetime({{epochSeconds: r.until}}).year = {current_year}  AND datetime({{epochSeconds: r.from}}).month >= {current_month}))))
                WITH a.address AS owner, COUNT(r) AS amount
                RETURN amount
                ORDER BY amount ASC
                """
            else:
                ownership_count_query = f"""
                MATCH (a)-[r:OWNED]->(n)
                WHERE n.collection_name = "{collection_processed}"
                    AND r.from IS NOT NULL
                    AND r.until IS NOT NULL
                    AND (datetime({{epochSeconds: r.from}}).year < {current_year} 
                        OR (datetime({{epochSeconds: r.from}}).year = {current_year}  AND datetime({{epochSeconds: r.from}}).month <= {current_month}))
                    AND (r.currently_owned = true 
                        OR (r.currently_owned = false AND (datetime({{epochSeconds: r.until}}).year > {current_year}  
                        OR (datetime({{epochSeconds: r.until}}).year = {current_year}  AND datetime({{epochSeconds: r.from}}).month >= {current_month}))))
                WITH a.address AS owner, COUNT(r) AS amount
                RETURN amount
                ORDER BY amount ASC
                """
            query_result = db.run_query('neo4j', ownership_count_query)
            date = str(current_year) + "," + str(current_month)

            if query_result != []:               
                # Extract the transaction counts into a list
                mint_counts = [record['amount'] for record in query_result]
                # Convert the list into a NumPy array
                mint_counts_array = np.array(mint_counts)

                gini = self.gini_coefficient(mint_counts_array)

                dates.append(date)
                gini_scores.append(round(gini, 4))

                logging.info(f"gini coef. calculated for year {current_year} and month {current_month}")
            else:
                dates.append(date)
                gini_scores.append(-1.0)

            if current_month == 12:
                current_month = 1
                current_year += 1
            else:
                current_month += 1

        final_result = {
            "dates": dates,
            "counts": gini_scores
        }
    
        return final_result
    
    def get_gini_transaction_history(self, collection: List[str], year_from: int, year_to: int, month_from: int, month_to: int):

        """
        This method returns the GINI coefficient based on TRANSACTED relationship for the given time period on a monthly basis. 
        A more granular time frame (like weekly or daily) would be possible as well. It would increase accuracy but 
        also complexity as more GINI coefficients would need to be calculated.
        The result is meant to be plotted as a bar-chart in the frontend.

        Parameter:
        - year_from: int
            The start year for the time period
        - year_to: int
            The end year for the time period.
        - month_from: int 
            The start month for the time period.
        - month_to: int 
            The end month for the time period
        - Collection: List[str]
            list of collections to which the considered NFTs belong to 

        Returns 
            two arrays, one for dates in the format Year-Month, and one for the respective GINI coefficient for that date
        """

        db = get_db()
        utilities = Utilities()

        collection_processed = utilities.get_collection(collection)

        current_year = year_from
        current_month = month_from

        dates = []
        gini_scores = []

        # iterate of each month in the specified time frame and calculate a GINI score considering the 
        # relationships of that month
        while (current_year < year_to) or (current_year == year_to and current_month <= month_to):

            # queries consider transactions only of the considered month.
            # if only one collection is considered, a collection filter is included
            if collection_processed == "all":
                transaction_count_query = f"""
                MATCH (a:Account)-[r:TRANSACTED]->()
                WHERE r.transaction_timestamp IS NOT NULL 
                    AND datetime({{epochSeconds: r.transaction_timestamp}}).year = {current_year} 
                    AND datetime({{epochSeconds: r.transaction_timestamp}}).month = {current_month} 
                WITH a.address AS owner, COUNT(r) AS amount
                RETURN amount
                ORDER BY amount ASC
                """ 
            else:
                transaction_count_query = f"""
                MATCH (a:Account)-[r:TRANSACTED]->()
                WHERE r.collection_name = "{collection_processed}"
                    AND datetime({{epochSeconds: r.transaction_timestamp}}).year = {current_year} 
                    AND datetime({{epochSeconds: r.transaction_timestamp}}).month = {current_month} 
                WITH a.address AS owner, COUNT(r) AS amount
                RETURN amount
                ORDER BY amount ASC
                """

            query_result = db.run_query('neo4j', transaction_count_query)
            date = str(current_year) + "," + str(current_month)

            if query_result != []:               
                # Extract the transaction counts into a list
                mint_counts = [record['amount'] for record in query_result]
                # Convert the list into a NumPy array
                mint_counts_array = np.array(mint_counts)

                gini = self.gini_coefficient(mint_counts_array)

                dates.append(date)
                gini_scores.append(round(gini, 4))

                logging.info(f"gini coef. calculated for year {current_year} and month {current_month}")
            else:
                dates.append(date)
                gini_scores.append(-1.0)

            if current_month == 12:
                current_month = 1
                current_year += 1
            else:
                current_month += 1

        final_result = {
            "dates": dates,
            "counts": gini_scores
        }
    
        return final_result
    
    def get_gini_mint_history(self, collection: List[str], year_from: int, year_to: int, month_from: int, month_to: int):

        """
        This method returns the GINI coefficient based on MINT relationship for the given time period on a monthly basis. 
        A more granular time frame (like weekly or daily) would be possible as well. It would increase accuracy but 
        also complexity as more GINI coefficients would need to be calculated.
        The result is meant to be plotted as a bar-chart in the frontend.

        Parameter:
        - year_from: int
            The start year for the time period
        - year_to: int
            The end year for the time period.
        - month_from: int 
            The start month for the time period.
        - month_to: int 
            The end month for the time period
        - Collection: List[str]
            list of collections to which the considered NFTs belong to 

        Returns 
            two arrays, one for dates in the format Year-Month, and one for the respective GINI coefficient for that date
        """

        db = get_db()
        utilities = Utilities()

        collection_processed = utilities.get_collection(collection)

        current_year = year_from
        current_month = month_from

        dates = []
        gini_scores = []

        # iterate of each month in the specified time frame and calculate a GINI score considering the 
        # relationships of that month
        while (current_year < year_to) or (current_year == year_to and current_month <= month_to):
            
            # queries consider mint events only of the considered month.
            # if only one collection is considered, a collection filter is included
            if collection_processed == "all":
                mint_count_query = f"""
                MATCH (a:Account)-[r:MINT]->()
                WHERE r.date IS NOT NULL 
                    AND datetime({{epochSeconds: r.date}}).year = {current_year}
                    AND datetime({{epochSeconds: r.date}}).month = {current_month} 
                WITH a.address AS owner, COUNT(r) AS amount
                RETURN amount
                ORDER BY amount ASC
                """ 
            else:
                mint_count_query = f"""
                MATCH (a:Account)-[r:MINT]->(n)
                WHERE n.collection_name = "{collection_processed}" 
                    AND r.date IS NOT NULL 
                    AND datetime({{epochSeconds: r.date}}).year = {current_year}
                    AND datetime({{epochSeconds: r.date}}).month = {current_month} 
                WITH a.address AS owner, COUNT(r) AS amount
                RETURN amount
                ORDER BY amount ASC
                """

            query_result = db.run_query('neo4j', mint_count_query)
            date = str(current_year) + "," + str(current_month)

            if query_result != []:               
                # Extract the transaction counts into a list
                mint_counts = [record['amount'] for record in query_result]
                # Convert the list into a NumPy array
                mint_counts_array = np.array(mint_counts)

                gini = self.gini_coefficient(mint_counts_array)

                dates.append(date)
                gini_scores.append(round(gini, 4))

                logging.info(f"gini coef. calculated for year {current_year} and month {current_month}")
            else:
                dates.append(date)
                gini_scores.append(-1.0)

            if current_month == 12:
                current_month = 1
                current_year += 1
            else:
                current_month += 1

        final_result = {
            "dates": dates,
            "counts": gini_scores
        }
    
        return final_result
    
    def nakamoto_coefficient(self, counts_array):
        """
        Calculate the Nakamoto coefficient, representing the minimum number of top entities required
        to control more than 50% of a resource, reflecting resource decentralization.
    
        Args:
        counts_array (List[int]): Counts of resource control by each entity.
    
        Returns:
        int: Nakamoto coefficient.
        """

        sorted_counts = np.sort(counts_array)[::-1]
        total = np.sum(sorted_counts)

        if total == 0:  # Prevent division by zero and meaningless calculations
            return 0

        cumulative_sum = np.cumsum(sorted_counts)
        nakamoto = np.argmax(cumulative_sum > 0.5 * total) + 1

        return int(nakamoto)
    
    def get_nakamoto_transaction(self, collection: List[str], year_from: int, year_to: int, month_from: int, month_to: int):
        
        """
        This method returns the Nakamoto coefficient based on TRANSACTED relationship for the given time period. 
        It gives insights into the inequality of transaction activity among Owners. 
    
        Parameter:
        - year_from: int
            The start year for the time period
        - year_to: int
            The end year for the time period.
        - month_from: int 
            The start month for the time period.
        - month_to: int 
            The end month for the time period
        - Collection: List[str]
            list of collections in which the users where active
    
        Returns:
            float: Nakamoto coefficient (rounded for 4 decimals)
        """
            
        db = get_db()
        utilities = Utilities()
    
        collection_processed = utilities.get_collection(collection)
    
        # Query: count the number of transactions for each owner in the time period 
        # If both collections are considered the collection filter is removed
        if collection_processed == "all":
            transaction_count_query = f"""
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
            WITH a.address AS owner, COUNT(r) AS amount
            RETURN amount
            ORDER BY amount ASC
            """ 
        else:
            transaction_count_query = f"""
            MATCH (a:Account)-[r:TRANSACTED]->()
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
            WITH a.address AS owner, COUNT(r) AS amount
            RETURN amount
            ORDER BY amount ASC
            """
    
        query_result = db.run_query('neo4j', transaction_count_query)

        # in case the query result is empty, return a Nakamoto of -1.0
        if query_result == []:
            return -1.0
            
        # Extract the transaction counts into a list
        transaction_counts = [record['amount'] for record in query_result]
        # Convert the list into a NumPy array
        transaction_counts_array = np.array(transaction_counts)

        nakamoto = self.nakamoto_coefficient(transaction_counts_array)

        return round(nakamoto, 4)
    
    def get_nakamoto_mint(self, collection: List[str], year_from: int, year_to: int, month_from: int, month_to: int):
                 
        """
            This method returns the Nakamoto coefficient based on MINT relationship for the given time period. 
            The Nakamoto coefficient for this relationship measures the inequality in the creation of NFTs among Owners. 
        
            Parameter:
            - year_from: int
                The start year for the time period
            - year_to: int
                The end year for the time period.
            - month_from: int 
                The start month for the time period.
            - month_to: int 
                The end month for the time period
            - Collection: List[str]
                list of collections for which the users minted
        
            Returns:
                float: Nakamoto coefficient (rounded for 4 decimals)
        """
        
        db = get_db()
        utilities = Utilities()
        
        collection_processed = utilities.get_collection(collection)
        
        # Query: count the number of transactions for each owner in the time period 
        # If both collections are considered the collection filter is removed
        if collection_processed == "all":
            mint_count_query = f"""
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
            WITH a.address AS owner, COUNT(r) AS amount
            RETURN amount
            ORDER BY amount ASC
             """ 
        else:
            mint_count_query = f"""
            MATCH (a:Account)-[r:MINT]->(n)
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
            WITH a.address AS owner, COUNT(r) AS amount
            RETURN amount
            ORDER BY amount ASC
            """
            
        query_result = db.run_query('neo4j', mint_count_query)

        # in case the query result is empty, return a Nakamoto of -1.0
        if query_result == []:
            return -1.0
                
        # Extract the transaction counts into a list
        mint_counts = [record['amount'] for record in query_result]
        # Convert the list into a NumPy array
        mint_counts_array = np.array(mint_counts)
        nakamoto = self.nakamoto_coefficient(mint_counts_array)
        return round(nakamoto, 4)
    
    def get_nakamoto_ownership_history(self, collection: List[str], year_from: int, year_to: int, month_from: int, month_to: int):
            
        """
            This method returns the Nakamoto coefficient based on OWNED relationship for the given time period on a monthly basis. 
        
            Parameter:
            - year_from: int
            The start year for the time period
            - year_to: int
            The end year for the time period.
            - month_from: int 
            The start month for the time period.
            - month_to: int 
            The end month for the time period
            - Collection: List[str]
            list of collections to which the considered NFTs belong to 
            
            Returns 
            two arrays, one for dates in the format Year-Month, and one for the respective Nakamoto coefficient for that date
        """
            
        db = get_db()
        utilities = Utilities()
            
        collection_processed = utilities.get_collection(collection)
            
        current_year = year_from
        current_month = month_from
            
        dates = []
        nakamoto_scores = []
        
        # iterate of each month in the specified time frame and calculate a Nakamoto score considering the relationships of that month
        while (current_year < year_to) or (current_year == year_to and current_month <= month_to):
        
        # queries consider transactions only of the considered month.
        # if only one collection is considered, a collection filter is included
            if collection_processed == "all":
                ownership_count_query = f"""
                MATCH (a)-[r:OWNED]->(n)
                WHERE r.from IS NOT NULL
                AND r.until IS NOT NULL
                AND (datetime({{epochSeconds: r.from}}).year < {current_year} 
                    OR (datetime({{epochSeconds: r.from}}).year = {current_year}  AND datetime({{epochSeconds: r.from}}).month <= {current_month}))
                AND (r.currently_owned = true
                    OR (r.currently_owned = false AND (datetime({{epochSeconds: r.until}}).year > {current_year}
                    OR (datetime({{epochSeconds: r.until}}).year = {current_year}  AND datetime({{epochSeconds: r.from}}).month >= {current_month}))))
                WITH a.address AS owner, COUNT(r) AS amount
                RETURN amount
                ORDER BY amount ASC
                """

            else:
                ownership_count_query = f"""
                MATCH (a)-[r:OWNED]->(n)
                WHERE n.collection_name = "{collection_processed}"
                AND r.from IS NOT NULL
                AND r.until IS NOT NULL
                AND (datetime({{epochSeconds: r.from}}).year < {current_year} 
                    OR (datetime({{epochSeconds: r.from}}).year = {current_year}  AND datetime({{epochSeconds: r.from}}).month <= {current_month}))
                AND (r.currently_owned = true
                    OR (r.currently_owned = false AND (datetime({{epochSeconds: r.until}}).year > {current_year}  
                    OR (datetime({{epochSeconds: r.until}}).year = {current_year}  AND datetime({{epochSeconds: r.from}}).month >= {current_month}))))
                WITH a.address AS owner, COUNT(r) AS amount
                RETURN amount
                ORDER BY amount ASC
                """
            query_result = db.run_query('neo4j', ownership_count_query)
            date = str(current_year) + "," + str(current_month)

            if query_result != []:
                # Extract the transaction counts into a list
                mint_counts = [record['amount'] for record in query_result]
                # Convert the list into a NumPy array
                mint_counts_array = np.array(mint_counts)

                nakamoto = self.nakamoto_coefficient(mint_counts_array)

                dates.append(date)
                nakamoto_scores.append(round(nakamoto, 4))

                logging.info(f"nakamoto coef. calculated for year {current_year} and month {current_month}")

            else:
                dates.append(date)
                nakamoto_scores.append(-1.0)

            if current_month == 12:
                current_month = 1
                current_year += 1
            else:
                current_month += 1

        final_result = {
            "dates": dates,
            "counts": nakamoto_scores
        }

        return final_result
    
    def get_nakamoto_transaction_history(self, collection: List[str], year_from: int, year_to: int, month_from: int, month_to: int):
                
        """
            This method returns the Nakamoto coefficient based on TRANSACTED relationship for the given time period on a monthly basis. 
                
            Parameter:
            - year_from: int
            The start year for the time period
            - year_to: int
            The end year for the time period.
            - month_from: int 
            The start month for the time period.
            - month_to: int 
            The end month for the time period
            - Collection: List[str]
            list of collections to which the considered NFTs belong to 
                
            Returns 
            two arrays, one for dates in the format Year-Month, and one for the respective Nakamoto coefficient for that date
        """
                
        db = get_db()
        utilities = Utilities()
                
        collection_processed = utilities.get_collection(collection)
                
        current_year = year_from
        current_month = month_from
                
        dates = []
        nakamoto_scores = []
            
        # iterate of each month in the specified time frame and calculate a Nakamoto score considering the 
        # relationships of that month
        while (current_year < year_to) or (current_year == year_to and current_month <= month_to):
            
        # queries consider transactions only of the considered month.
        # if only one collection is considered, a collection filter is included
            if collection_processed == "all":
                transaction_count_query = f"""
                MATCH (a:Account)-[r:TRANSACTED]->()
                WHERE r.transaction_timestamp IS NOT NULL 
                AND datetime({{epochSeconds: r.transaction_timestamp}}).year = {current_year} 
                AND datetime({{epochSeconds: r.transaction_timestamp}}).month = {current_month} 
                WITH a.address AS owner, COUNT(r) AS amount
                RETURN amount
                ORDER BY amount ASC
                """ 
            else:
                transaction_count_query = f"""
                MATCH (a:Account)-[r:TRANSACTED]->()
                WHERE r.collection_name = "{collection_processed}"
                AND datetime({{epochSeconds: r.transaction_timestamp}}).year = {current_year} 
                AND datetime({{epochSeconds: r.transaction_timestamp}}).month = {current_month}
                WITH a.address AS owner, COUNT(r) AS amount 
                RETURN amount
                ORDER BY amount ASC
                """

            query_result = db.run_query('neo4j', transaction_count_query)
            date = str(current_year) + "," + str(current_month)

            if query_result != []:
                # Extract the transaction counts into a list
                mint_counts = [record['amount'] for record in query_result]
                # Convert the list into a NumPy array
                mint_counts_array = np.array(mint_counts)

                nakamoto = self.nakamoto_coefficient(mint_counts_array)

                dates.append(date)
                nakamoto_scores.append(round(nakamoto, 4))

                logging.info(f"nakamoto coef. calculated for year {current_year} and month {current_month}")

            else:
                dates.append(date)
                nakamoto_scores.append(-1.0)

            if current_month == 12:
                current_month = 1
                current_year += 1
            else:
                current_month += 1

        final_result = {
            "dates": dates,
            "counts": nakamoto_scores
        }

        return final_result
    
    def get_nakamoto_mint_history(self, collection: List[str], year_from: int, year_to: int, month_from: int, month_to: int):
                    
        """
        This method returns the Nakamoto coefficient based on MINT relationship for the given time period on a monthly basis. 
                    
        Parameter:
        - year_from: int
        The start year for the time period
        - year_to: int
        The end year for the time period.
        - month_from: int 
        The start month for the time period.
        - month_to: int 
        The end month for the time period
        - Collection: List[str]
        list of collections to which the considered NFTs belong to 
                    
        Returns 
        two arrays, one for dates in the format Year-Month, and one for the respective Nakamoto coefficient for that date
        """
        db = get_db()
        utilities = Utilities()
        
        collection_processed = utilities.get_collection(collection)
                    
        current_year = year_from
        current_month = month_from
        
        dates = []
        nakamoto_scores = []
                
        # iterate of each month in the specified time frame and calculate a Nakamoto score considering the 
        # relationships of that month
        while (current_year < year_to) or (current_year == year_to and current_month <= month_to):
                
            # queries consider mint events only of the considered month.
            # if only one collection is considered, a collection filter is included
            if collection_processed == "all":
                mint_count_query = f"""
                        MATCH (a:Account)-[r:MINT]->()
                        WHERE r.date IS NOT NULL 
                        AND datetime({{epochSeconds: r.date}}).year = {current_year}
                        AND datetime({{epochSeconds: r.date}}).month = {current_month} 
                        WITH a.address AS owner, COUNT(r) AS amount
                        RETURN amount
                        ORDER BY amount ASC
                        """ 
            else:
                mint_count_query = f"""
                        MATCH (a:Account)-[r:MINT]->(n)
                        WHERE n.collection_name = "{collection_processed}" 
                        AND r.date IS NOT NULL 
                        AND datetime({{epochSeconds: r.date}}).year = {current_year}
                        AND datetime({{epochSeconds: r.date}}).month = {current_month}
                        WITH a.address AS owner, COUNT(r) AS amount
                        RETURN amount
                        ORDER BY amount ASC
                        """

            query_result = db.run_query('neo4j', mint_count_query)
            date = str(current_year) + "," + str(current_month)

            if query_result != []:
                # Extract the transaction counts into a list
                mint_counts = [record['amount'] for record in query_result]
                # Convert the list into a NumPy array
                mint_counts_array = np.array(mint_counts)

                nakamoto = self.nakamoto_coefficient(mint_counts_array)

                dates.append(date)
                nakamoto_scores.append(round(nakamoto, 4))

                logging.info(f"nakamoto coef. calculated for year {current_year} and month {current_month}")

            else:
                dates.append(date)
                nakamoto_scores.append(-1.0)
            
            if current_month == 12:
                current_month = 1
                current_year += 1
            else:
                current_month += 1

        final_result = {
            "dates": dates,
            "counts": nakamoto_scores
        }

        return final_result