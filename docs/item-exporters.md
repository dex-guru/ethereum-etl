# Item exporters

Ethereum ETL supports a lot of different item exporters. An item exporter is a class that writes items to a destination.
To set up an item exporter, you need to specify the OUTPUT environment variable. You can set more that one output at a time.
The format is `exporter_name://path_to_destination,second_exporter://path_to_destination`.

Here are the item exporters that are available out of the box and how to set them up:

- CONSOLE: OUTPUT=console or OUTPUT=None
- JSON: OUTPUT=path_to_output.json
- CSV: OUTPUT=path_to_output.csv
- PROJECTS: OUTPUT=projects://path_to_google_cloud_project
- KINESIS: OUTPUT=kinesis://stream_name
- KAFKA: OUTPUT=kafka://topic
- POSTGRES: OUTPUT=postgresql://user:password@host:port/dbname
- GCS: OUTPUT=gs://bucket_name
- CLICKHOUSE: OUTPUT=clickhouse+http://host:port/dbname
- AMQP: OUTPUT=amqp://user:password@host:port/vhost or OUTPUT=rabbitmq://user:password@host:port/vhost
- ELASTICSERACH: OUTPUT=elasticsearch+http://host:port/index_name or OUTPUT=elasticsearch+https://user:password@host:9200/index_name
