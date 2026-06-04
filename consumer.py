import threading
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from config.settings import LOCAL_KAFKA_CONFIG, DESTINATION_TOPIC, MONGO_URI, MONGO_DB, MONGO_COLLECTION
from logger.logger import logger
from utils import json_deserializer, init_mongodb
from pymongo.errors import DuplicateKeyError

def run_consumer(stop_event=None):
    if stop_event is None:
        stop_event = threading.Event()
    
    logger.info("Consumer starting, listening to topic: %s", DESTINATION_TOPIC)
    mongo_client = None
    local_consumer = None

    try:
        logger.info("Initializing MongoDB connection...")
        mongo_client, db, collection = init_mongodb(MONGO_URI, MONGO_DB, MONGO_COLLECTION)
        
        if mongo_client is None:
            logger.error("Failed to initialize MongoDB. Consumer cannot continue.")
            return
        
        logger.info("MongoDB initialized successfully")

        logger.info("Initializing Kafka consumer for topic: %s", DESTINATION_TOPIC)
        local_consumer = KafkaConsumer(
            DESTINATION_TOPIC,
            **LOCAL_KAFKA_CONFIG,
            group_id="kafka_to_mongodb_consumer",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=5000,
            max_poll_records=100,
            value_deserializer=json_deserializer,
        )
        logger.info("Connected to local Kafka, consuming from topic: %s", DESTINATION_TOPIC)
        logger.info("Waiting for messages from Kafka topic...")

        count = 0
        batch_count = 0
        for message in local_consumer:
            batch_count += 1
            if stop_event.is_set():
                logger.info("Stop signal received, exiting consumer loop")
                break
                
            data = message.value
            if not data:
                logger.debug("Skipping empty message from partition %s offset %s", message.partition, message.offset)
                continue

            #metadata
            data["_kafka_partition"] = message.partition
            data["_kafka_offset"] = message.offset
            data["_kafka_timestamp"] = datetime.utcnow()
            try: 
                result = collection.insert_one(data)
                count += 1 
                logger.info("Stored message #%d in MongoDB with _id: %s (partition=%s, offset=%s)", 
                           count, result.inserted_id, message.partition, message.offset)
            except DuplicateKeyError:
                logger.warning("Duplicate document skipped for _id=%s", data.get("id"))
            except Exception as e:
                logger.error("Failed to insert into MongoDB: %s", e)
                
        logger.info("Consumer loop ended. Total messages processed: %d", count)
        
    except KafkaError as ke:
        logger.error("Kafka connection error: %s", ke, exc_info=True)
    except Exception as e:
        logger.error("Consumer unexpected error: %s", e, exc_info=True)
    finally:
        if local_consumer is not None: 
            try: 
                local_consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error("Error closing Kafka consumer: %s", e)
        if mongo_client:
            try:
                mongo_client.close()
                logger.info("MongoDB client closed")
            except Exception as e:
                logger.error("Error closing MongoDB client: %s", e)
        logger.info("Consumer stopped")

if __name__ == "__main__": 
    run_consumer()
            