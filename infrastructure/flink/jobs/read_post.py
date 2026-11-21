from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
import json
from pyflink.common import Types
import unicodedata
import re




OUTPUT_TOPIC = "silver-posts"

env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(10_000)
env.disable_operator_chaining()

source = (
    KafkaSource.builder()
    .set_bootstrap_servers("kafka:9092")
    .set_topics("raw-posts")
    .set_group_id("flink-group-4")
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)
DROP_FIELDS  =  {"logo", "description", "expires_at", "created_by", "edited", "published"}

stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

sink = (
    KafkaSink.builder()
    .set_bootstrap_servers("kafka:9092")
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic(OUTPUT_TOPIC)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    .build()
)

def clean_data(raw: str) -> str:
    post = json.loads(raw)
    post["job_title"] = post["job_title"].lower()
    post["job_title"] = unicodedata.normalize("NFKD", post["job_title"])
    post["job_title"] = "".join(c for c in post["job_title"] if not unicodedata.combining(c))
    post["job_title"] = re.sub(r"[^a-z0-9+#.\s]", " ", post["job_title"])
    post["job_title"] = re.sub(r"\.(?!\w)", " ", post["job_title"])
    post["job_title"] = re.sub(r"\s+", " ", post["job_title"])
    post["job_title"] = post["job_title"].strip()
    return json.dumps(
        {key: value for key, value in post.items() if key not in DROP_FIELDS},
        ensure_ascii=False,
    )
cleaned_stream = stream.map(clean_data, output_type=Types.STRING())
cleaned_stream.sink_to(sink).name("Kafka Sink")

env.execute("read-post")