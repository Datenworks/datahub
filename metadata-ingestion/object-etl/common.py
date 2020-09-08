#! /usr/bin/python
import json
import time

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from dataclasses import dataclass
from sqlalchemy import create_engine
from sqlalchemy import types
from sqlalchemy.engine import reflection
from datetime import datetime
import hashlib


@dataclass
class KafkaConfig:
    avsc_path = '/home/richard/datenworks/projects/datahub/metadata-events/mxe-schemas/src/renamed/avro/com/linkedin/mxe/MetadataChangeEvent.avsc'
    kafka_topic = 'MetadataChangeEvent_v4'
    bootstrap_server = 'localhost:9092'
    schema_registry = 'http://localhost:8081'


def get_column_type(column_type):
    """
    Maps SQLAlchemy types (https://docs.sqlalchemy.org/en/13/core/type_basics.html) to corresponding schema types
    """
    avrotypes = {
        'string': ("com.linkedin.pegasus2avro.schema.StringType", {}),
        'array': ("com.linkedin.pegasus2avro.schema.ArrayType", {})
    }

    return avrotypes.get(
        column_type,
        ("com.linkedin.pegasus2avro.schema.NullType", {})
    )


def build_dataset_mce(platform, dataset_name, columns, upstreams_urns=[]):
    """
    Creates MetadataChangeEvent for the dataset.
    """
    actor, sys_time = "urn:li:corpuser:datahub", int(time.time())
    fields = []
    for column in columns:
        fields.append({
            "fieldPath": column["name"],
            "nativeDataType": repr(column["type"]),
            "type": {"type": get_column_type(column["type"])},
            "description": column.get("comment", "A field")
        })

    schema_metadata = {
        "schemaName": dataset_name,
        "platform": f"urn:li:dataPlatform:{platform}",
        "version": 2,
        "created": {"time": int(datetime.now().timestamp()), "actor": actor},
        "lastModified": {"time": int(datetime.now().timestamp()), "actor": actor},
        "hash": hashlib.sha1(json.dumps(fields).encode('utf-8')).hexdigest(),
        "platformSchema": {"tableSchema": json.dumps(fields)},
        "fields": fields
    }

    ownership = {
        "owners": [
            {
                "owner": "urn:li:corpuser:datahub",
                "type": "DATAOWNER"
            }
        ],
        "lastModified": {
            "time": 0,
            "actor": "urn:li:corpuser:datahub"
        }
    }

    upstreams = {
        "upstreams": [{"auditStamp": {"time": sys_time, "actor": actor}, "dataset": upstream_urn, "type": "COPY"} for upstream_urn in upstreams_urns]
    }

    return {
        "auditHeader": None,
        "proposedSnapshot": ("com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot", {
            "urn": f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_name},PROD)",
            "aspects": [
                ("com.linkedin.pegasus2avro.schema.SchemaMetadata", schema_metadata),
                ("com.linkedin.pegasus2avro.common.Ownership", ownership),
                ("com.linkedin.pegasus2avro.dataset.UpstreamLineage", upstreams)
            ]
        }),
        "proposedDelta": None
    }


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(
            msg.topic(), msg.partition()))


def produce_dataset_mce(mce, kafka_config):
    """
    Produces a MetadataChangeEvent to Kafka
    """
    conf = {'bootstrap.servers': kafka_config.bootstrap_server,
            'on_delivery': delivery_report,
            'schema.registry.url': kafka_config.schema_registry}
    key_schema = avro.loads('{"type": "string"}')
    record_schema = avro.load(kafka_config.avsc_path)
    producer = AvroProducer(
        conf, default_key_schema=key_schema, default_value_schema=record_schema)

    producer.produce(topic=kafka_config.kafka_topic,
                     key=mce['proposedSnapshot'][1]['urn'], value=mce)
    producer.flush()


def run(url, options, platform, kafka_config=KafkaConfig(), upstream_urns=[]):
    engine = create_engine(url, **options)
    inspector = reflection.Inspector.from_engine(engine)
    for schema in inspector.get_schema_names():
        for table in inspector.get_table_names(schema):
            if table == "cadastro":
                urns = ["urn:li:dataset:(urn:li:dataPlatform:gcs,dev-semesp-datascience.tcs_titulares,PROD)",
                        "urn:li:dataset:(urn:li:dataPlatform:gcs,dev-semesp-datascience.tcs_entidades,PROD)"]
                columns = inspector.get_columns(table, schema)
                mce = build_dataset_mce(platform, f'{schema}.{table}', columns, urns)
                produce_dataset_mce(mce, kafka_config)
            elif table == "contatos":
                urns = ["urn:li:dataset:(urn:li:dataPlatform:bigquery,stage.cadastro,PROD)"]
                columns = inspector.get_columns(table, schema)
                mce = build_dataset_mce(platform, f'{schema}.{table}', columns, urns)
                produce_dataset_mce(mce, kafka_config)
            else:
                columns = inspector.get_columns(table, schema)
                mce = build_dataset_mce(platform, f'{schema}.{table}', columns, upstream_urns)
                produce_dataset_mce(mce, kafka_config)
