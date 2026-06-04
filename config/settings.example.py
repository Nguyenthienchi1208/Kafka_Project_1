

SOURCE_TOPIC = "product_view"
DESTINATION_TOPIC = "product_data"

EXTERNAL_KAFKA_CONFIG = {
    "bootstrap_servers": [
        "your_ip:9094",
        "your_ip:9194",
        "your_ip:9294",
    ],
    "security_protocol": "SASL_PLAINTEXT",  
    "sasl_mechanism": "PLAIN",
    "sasl_plain_username": "your_username",
    "sasl_plain_password": "your_password",
}

LOCAL_KAFKA_CONFIG = {
    "bootstrap_servers": [
        "localhost:9094",
        "localhost:9194",
        "localhost:9294",
    ],
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "sasl_plain_username": "your_username",
    "sasl_plain_password": "your_password",
}

MONGO_URI = "mongodb://your_username:your_password@localhost:27017/"
MONGO_DB = "kafka_db"
MONGO_COLLECTION = "product_data"
