import threading
import signal
import sys
from logger.logger import logger
from producer import run_producer
from consumer import run_consumer

stop_event = threading.Event()


def handle_signal(signum, frame):
    logger.info("Received signal %s, initiating graceful shutdown...", signum)
    stop_event.set()  # Signal threads to stop


def main():
    logger.info("Starting Kafka Pipeline Application")
    logger.info("=" * 50)
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    # Create threads for producer and consumer
    producer_thread = threading.Thread(
        target=run_producer,
        args=(stop_event,),
        name="ProducerThread",
        daemon=False
    )
    consumer_thread = threading.Thread(
        target=run_consumer,
        args=(stop_event,),
        name="ConsumerThread",
        daemon=False
    )
    
    try:
        logger.info("Starting producer thread...")
        producer_thread.start()
        
        logger.info("Starting consumer thread...")
        consumer_thread.start()
        
        logger.info("=" * 50)
        logger.info("Pipeline is running. Press Ctrl+C to stop.")
        logger.info("Producer thread alive: %s", producer_thread.is_alive())
        logger.info("Consumer thread alive: %s", consumer_thread.is_alive())
        logger.info("=" * 50)
        
        # Wait for both threads to complete
        producer_thread.join()
        consumer_thread.join()
        
        logger.info("Both threads have completed")
        
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, initiating shutdown...")
    except Exception as e:
        logger.error("Unexpected error in main: %s", e)
    finally:
        logger.info("Kafka Pipeline Application stopped")


if __name__ == "__main__":
    main()
