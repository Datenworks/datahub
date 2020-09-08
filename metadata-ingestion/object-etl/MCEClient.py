from dataclasses import dataclass
from typing import Dict, List


@dataclass
class KafkaConfig:
    avsc_path = '/home/richard/datenworks/projects/datahub/metadata-events/'\
                'mxe-schemas/src/renamed/avro/com/linkedin/mxe/'\
                'MetadataChangeEvent.avsc'
    kafka_topic = 'MetadataChangeEvent_v4'
    bootstrap_server = 'localhost:9092'
    schema_registry = 'http://localhost:8081'


class MCEClient(object):
    def __init__(self, kafka_config: KafkaConfig, owner: str):
        self.kafka_config = kafka_config
        self.owner = owner

    def publish(self, schema_columns: List[Dict], upstream_arns: list = []):
        pass

    def __to_mce(self, schema):
        pass
