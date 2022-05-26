from kafka import KafkaProducer

# Import JSON module to serialize data
import json

# Initialize producer variable and set parameter for JSON encode
producer = KafkaProducer(bootstrap_servers=
                         ['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Send data in JSON format
producer.send('dbzzz.inventory.airmiles', {
  "schema": {
    "type": "struct",
    "fields": [
      {
        "type": "struct",
        "fields": [
          {
            "type": "string",
            "optional": True,
            "field": "Status"
          },
          {
            "type": "double",
            "optional": True,
            "field": "Price"
          }
        ],
        "optional": True,
        "name": "dbzzz.inventory.airmiles.Value",
        "field": "before"
      },
      {
        "type": "struct",
        "fields": [
          {
            "type": "string",
            "optional": True,
            "field": "Status"
          },
          {
            "type": "double",
            "optional": True,
            "field": "Price"
          }
        ],
        "optional": True,
        "name": "dbzzz.inventory.airmiles.Value",
        "field": "after"
      },
      {
        "type": "struct",
        "fields": [
          {
            "type": "string",
            "optional": False,
            "field": "version"
          },
          {
            "type": "string",
            "optional": False,
            "field": "connector"
          },
          {
            "type": "string",
            "optional": False,
            "field": "name"
          },
          {
            "type": "int64",
            "optional": False,
            "field": "ts_ms"
          },
          {
            "type": "string",
            "optional": True,
            "name": "io.debezium.data.Enum",
            "version": 1,
            "parameters": {
              "allowed": "true,last,false"
            },
            "default": "false",
            "field": "snapshot"
          },
          {
            "type": "string",
            "optional": False,
            "field": "db"
          },
          {
            "type": "string",
            "optional": True,
            "field": "table"
          },
          {
            "type": "int64",
            "optional": False,
            "field": "server_id"
          },
          {
            "type": "string",
            "optional": True,
            "field": "gtid"
          },
          {
            "type": "string",
            "optional": False,
            "field": "file"
          },
          {
            "type": "int64",
            "optional": False,
            "field": "pos"
          },
          {
            "type": "int32",
            "optional": False,
            "field": "row"
          },
          {
            "type": "int64",
            "optional": True,
            "field": "thread"
          },
          {
            "type": "string",
            "optional": True,
            "field": "query"
          }
        ],
        "optional": False,
        "name": "io.debezium.connector.mysql.Source",
        "field": "source"
      },
      {
        "type": "string",
        "optional": False,
        "field": "op"
      },
      {
        "type": "int64",
        "optional": True,
        "field": "ts_ms"
      },
      {
        "type": "struct",
        "fields": [
          {
            "type": "string",
            "optional": False,
            "field": "id"
          },
          {
            "type": "int64",
            "optional": False,
            "field": "total_order"
          },
          {
            "type": "int64",
            "optional": False,
            "field": "data_collection_order"
          }
        ],
        "optional": True,
        "field": "transaction"
      }
    ],
    "optional": False,
    "name": "dbzzz.inventory.airmiles.Envelope"
  },
  "payload": {
    "before": None,
    "after": {
      "Status": "Gold",
      "Price": 23.399999618530273
    },
    "source": {
      "version": "1.4.2.Final-redhat-00002",
      "connector": "mysql",
      "name": "dbzzz",
      "ts_ms": 0,
      "snapshot": "true",
      "db": "inventory",
      "table": "airmiles",
      "server_id": 0,
      "gtid": None,
      "file": "mysql-bin.000028",
      "pos": 1371,
      "row": 0,
      "thread": None,
      "query": None
    },
    "op": "r",
    "ts_ms": 1653499421257,
    "transaction": None
  }

})
producer.flush()
# Print message
print("Message Sent to JSONtopic")