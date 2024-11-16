use std::{sync::Arc, time::Duration};
use log::{error, trace};
use tokio::sync::Mutex;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    producer::{FutureProducer, FutureRecord},
    ClientConfig
};

pub fn create_kafka_consumer() -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9093")
        .set("group.id", "TweetProcessorConsumers")
        .create()
        .expect("Error creating consumer");

    consumer
        .subscribe(&["raw_tweets"])
        .expect("Can't subscribe to topic");

    consumer
}

pub fn create_kafka_future_producer() -> Result<Arc<Mutex<FutureProducer>>, KafkaError> {
    Ok(Arc::new(
        Mutex::new(
            ClientConfig::new()
            .set("bootstrap.servers", "localhost:9093")
            .create()?
        )
    ))
}

pub async fn produce_message(producer: &FutureProducer, output_topic: &str, processed_tweet: &String) {
    let produce_future = producer.send(
        FutureRecord::to(output_topic)
            .key("some-key")
            .payload(processed_tweet),
        Duration::from_secs(0)
    );

    match produce_future.await {
        Ok(delivery) => trace!("Sent: {:?}", delivery),
        Err((e, _)) => error!("Error: {:?}", e),
    };
}