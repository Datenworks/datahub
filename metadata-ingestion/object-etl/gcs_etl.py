import gcsfs
from ndjson_crawler import NdjsonCrawler
import common

gcsfs = gcsfs.GCSFileSystem()
ndjson_crawler = NdjsonCrawler()
with gcsfs.open('gs://semesp-datalake-prd-raw/tcs/titulares/dt=1996-11-04/hr=00/history') as f:
    data = f.read()

schema = ndjson_crawler.crawler(data)

mce = common.build_dataset_mce(
    'gcs',
    'dev-semesp-datascience.tcs_titulares',
    schema
)
common.produce_dataset_mce(mce, common.KafkaConfig())


with gcsfs.open('gs://semesp-datalake-prd-raw/tcs/entidades/dt=1996-11-04/hr=00/history') as f:
    data = f.read()

schema = ndjson_crawler.crawler(data)

mce = common.build_dataset_mce(
    'gcs',
    'dev-semesp-datascience.tcs_entidades',
    schema
)
common.produce_dataset_mce(mce, common.KafkaConfig())
