use std::{error::Error, sync::Arc};
use futures::TryStreamExt;
use log::{error, info, trace};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use rdkafka::{
    consumer::StreamConsumer,
    producer::{FutureProducer},
    Message
};
mod kafka;

#[derive(Debug, Serialize, Deserialize)]
struct User {
    id: i32,
    name: String,
    screen_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Coordinates {
    latitude: f64,
    longitude: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Tweet {
    created_at: String,
    id: i32,
    full_text: String,
    user: User,
    retweet_count: i32,
    favourite_count: i32,
    lang: String,
    is_retweet: bool,
    geo: Option<String>,
    coordinates: Option<Coordinates>,
    place: Option<String>,
}

async fn run_async_tweet_processor() -> Result<(), Box<dyn Error>> {
    let consumer: StreamConsumer = kafka::create_kafka_consumer();

    let locked_producer: Arc<Mutex<FutureProducer>> = kafka::create_kafka_future_producer()?;

    let stream_processor = consumer.stream().try_for_each(|borrowed_message| {
        let producer = locked_producer.clone();
        let output_topic = "processed_tweets";
        async move {
            let payload = match borrowed_message.payload_view::<str>() {
                Some(Ok(payload)) => payload.to_string(),
                _ => {
                    eprintln!("Error deserializing message payload");
                    return Ok(());
                }
            };

            trace!("Consumed message: {}", payload);

            let raw_tweet: Tweet = match serde_json::from_str(&payload) {
                Ok(deserialized_tweet) => deserialized_tweet,
                Err(e) => {
                    error!("Failed to deserialize tweet. Error: {}", e);
                    return Ok(());
                }
            };

            // TBA: Pre-processing
            // TBA: Processing

            let processed_tweet = match serde_json::to_string(&raw_tweet) {
                Ok(serialized_tweet) => serialized_tweet,
                Err(e) => {
                    error!("Failed to serialize processed tweet. Error: {}", e);
                    return Ok(());
                }
            };
            let producer = producer.lock().await;
            kafka::produce_message(&producer, output_topic, &processed_tweet).await;

        Ok(())
        }
    });

    info!("Starting Tweet-Processor stream...");
    stream_processor.await.expect("Tweet-Processor stream failed");
    info!("Tweet-Processor stream terminated");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    info!("Trying to start Tweet-Processor...");
    run_async_tweet_processor().await?;
    Ok(())
}
