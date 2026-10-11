from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import (
    AvroSerializer,
    AvroDeserializer
)
from confluent_kafka.serialization import StringDeserializer


class SchemaRegistry:

    def __init__(
        self,
        schema_registry_url,
        schema_registry_username,
        schema_registry_password,
        key_schema_name,
        value_schema_name
    ):
        self.schema_registry_url = schema_registry_url
        self.schema_registry_username = schema_registry_username
        self.schema_registry_password = schema_registry_password

        self.key_schema_name = key_schema_name
        self.value_schema_name = value_schema_name

        self.__schema_registry_client = self._schema_registry_client()

    def _schema_registry_client(self):

        schema_registry_conf = {
            "url": f"https://{self.schema_registry_url}",
            "basic.auth.user.info":
                f"{self.schema_registry_username}:{self.schema_registry_password}"
        }

        return SchemaRegistryClient(schema_registry_conf)

    def get_key_value_schema(self):

        key_schema = (
            self.__schema_registry_client
            .get_latest_version(self.key_schema_name)
            .schema
            .schema_str
        )

        value_schema = (
            self.__schema_registry_client
            .get_latest_version(self.value_schema_name)
            .schema
            .schema_str
        )

        return key_schema, value_schema

    def get_key_value_avro_serializer(self):

        key_schema, value_schema = self.get_key_value_schema()

        key_serializer = AvroSerializer(
            self.__schema_registry_client,
            key_schema
        )

        value_serializer = AvroSerializer(
            self.__schema_registry_client,
            value_schema
        )

        return key_serializer, value_serializer

    def get_key_value_avro_deserializer(self):

        key_schema, value_schema = self.get_key_value_schema()

        key_deserializer = AvroDeserializer(
            self.__schema_registry_client,
            key_schema
        )

        value_deserializer = AvroDeserializer(
            self.__schema_registry_client,
            value_schema
        )

        string_deserializer = StringDeserializer("utf_8")

        return (
            key_deserializer,
            value_deserializer,
            string_deserializer
        )