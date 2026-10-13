# data-pipiline

- module kafka
    1. consume websocket from bluesky jetstream in json, avoiding cbor (seems that it is not supported even thought is litterally binary json) to have a schema 
    2. send it to a kafka topic
    3. probably the raw one is untouched and then i could create an avro one and have the schema so it could be used kafka streams or so to do some data preparation or even derivate enriched ones

- module kafka streams
    1. consume the landing topic
    2. create a new one enriched in avro having a key (did identifier) and a value (the message)
    3. right now due to the absence of SR i will implement my own serde 

- module flink processor
    1. consume the kafka avro enriched topic
    2. convert it to a stream of words by flatMap it Monad m => m a -> (a -> m b)
    3. remove stop words (it isn't really well done, the list is small and in english)
    4. do tumbling windows of 1 minute or 30 seconds to calculate top 10 words most used
