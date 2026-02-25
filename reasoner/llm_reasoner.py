import json
import time
import requests
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP = "localhost:9092"
IN_TOPIC = "semantic_events"
OUT_TOPIC = "reasoning_events"
GROUP_ID = "llm-reasoner-v1"