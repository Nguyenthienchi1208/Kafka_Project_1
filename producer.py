import threading
from logger.logger import logger
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from config.settings import EXTERNAL_KAFKA_CONFIG,LOCAL_KAFKA_CONFIG,SOURCE_TOPIC, DESTINATION_TOPIC
from utils import json_deserializer, json_serializer

def run_producer(stop_event=None):
    if stop_event is None:
        stop_event = threading.Event()
    
    logger.info("Producer starting: %s -> %s", SOURCE_TOPIC, DESTINATION_TOPIC)
    external_consumer = None 
    local_producer = None 
    try:
        external_consumer = KafkaConsumer(
            SOURCE_TOPIC,
            **EXTERNAL_KAFKA_CONFIG,
            value_deserializer=json_deserializer,
        )
        logger.info("Connected to external Kafka")
        local_producer = KafkaProducer(
            **LOCAL_KAFKA_CONFIG,
            value_serializer=json_serializer,
        )
        logger.info("Connected to local Kafka")

        count = 0
        for message in external_consumer:
            if stop_event.is_set():
                logger.info("Stop signal received, exiting producer loop")
                break
                
            data = message.value
            if data is None:
                continue
            try: 
                future = local_producer.send(DESTINATION_TOPIC, value=data)
                meta = future.get(timeout=10)
                count += 1 
                logger.info(" Produced #%d to %s partition=%s offset=%s", count, DESTINATION_TOPIC, meta.partition, meta.offset)
            except KafkaError as ke:
                logger.error("Kafka send error: %s", ke)
            except Exception as e:
                logger.error("Unexpected error: %s", e)
    except KafkaError as ke:
        logger.error("Kafka connection error: %s", ke)
    except Exception as e:
        logger.error("Producer unexpected error: %s", e)
    finally:
        if external_consumer is not None:
            try: 
                external_consumer.close()
            except Exception as e: 
                pass 
        if local_producer:
            try: 
                local_producer.flush()
                local_producer.close()
            except Exception as e:
                pass
        logger.info("Producer stopped")
if __name__ == "__main__":
    run_producer()
