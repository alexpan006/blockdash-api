from app.neo4j_access.neo4j_access import Neo4jInstance
import os
import logging

"""
Author: Valentin Leuthe 
"""

db = None

def init_db():
    """
    Instanciate a database access
    """
    global db
    uri = os.getenv("DB_URL")
    username = os.getenv("DB_USR")
    password = os.getenv("DB_PWD")
    logging.info(f"Initializing DB with URI: {uri}, Username: {username}")
    db = Neo4jInstance(uri, username, password)

def get_db():
    """
    returns the database access
    """
    logging.info("Getting DB instance")
    return db