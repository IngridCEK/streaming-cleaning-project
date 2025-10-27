from pymongo import MongoClient

def get_mongo(db_name: str = "streaming_demo", uri: str = "mongodb://localhost:27017/"):
    client = MongoClient(uri)
    return client[db_name]
