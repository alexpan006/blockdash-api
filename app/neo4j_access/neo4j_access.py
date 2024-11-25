from neo4j import GraphDatabase
from dotenv import load_dotenv
load_dotenv()

class Neo4jInstance:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def run_query(self,target_db ,query, parameters=None):
        with self.driver.session(database=target_db) as session:
            result = session.run(query, parameters)
            return [record for record in result]

    def test_connection(self,target_db):
        try:
            with self.driver.session(database=target_db)  as session:
                # Running a simple query to fetch the Neo4J version
                result = session.run("RETURN 'Connection successful!' as message")
                message = result.single()[0]
                return message
        except Exception as e:
            return f"An error occurred: {e}"
    
