import json
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from logger.logger import logger


def json_deserializer(m: bytes):
    try: 
        return json.loads(m.decode("utf-8")) if m else None
    except Exception as e:
        logger.error("Deserialize error: %s", e)
        return None


def json_serializer(v):
    try:
        return json.dumps(v).encode("utf-8") if v else None
    except Exception as e:
        logger.error("Serialize error: %s", e)
        return None


def init_mongodb(mongo_uri: str, db_name: str, collection_name: str):
    """
    Initialize MongoDB connection and ensure database and collection exist.
    Creates them if they don't exist.
    
    Args:
        mongo_uri: MongoDB connection string
        db_name: Database name
        collection_name: Collection name
        
    Returns:
        tuple: (MongoClient, db, collection) or (None, None, None) on failure
    """
    try:
        logger.info("Attempting to connect to MongoDB: %s", mongo_uri)
        # Connect to MongoDB
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        
        # Test connection
        logger.info("Testing MongoDB connection with ping...")
        client.admin.command('ping')
        logger.info("Successfully connected to MongoDB")
        
        # Get or create database
        db = client[db_name]
        logger.info("Using database: %s", db_name)
        
        # Get or create collection
        collection = db[collection_name]
        
        # Verify collection exists by checking if it's in the list
        existing_collections = db.list_collection_names()
        logger.info("Existing collections in database: %s", existing_collections)
        
        if collection_name not in existing_collections:
            logger.info("Collection '%s' does not exist. Creating...", collection_name)
            # Create collection explicitly
            db.create_collection(collection_name)
            logger.info("Collection '%s' created successfully", collection_name)
        else:
            logger.info("Collection '%s' already exists", collection_name)
        
        logger.info("MongoDB initialization completed successfully")
        return client, db, collection
        
    except ServerSelectionTimeoutError as e:
        logger.error("Could not connect to MongoDB at %s (timeout): %s", mongo_uri, e)
        return None, None, None
    except Exception as e:
        logger.error("Failed to initialize MongoDB: %s", type(e).__name__, exc_info=True)
        return None, None, None
