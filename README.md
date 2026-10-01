# data-pipiline

- module kafka
    1. consume websocket from bluesky jetstream in json, avoiding cbor (seems that it is not supported even thought is litterally binary json) to have a schema 
    2. send it to a kafka topic
    3. probably the raw one is untouched and then i could create an avro one and have the schema so it could be used kafka streams or so to do some data preparation or even derivate enriched ones

- module flink
    1. 
