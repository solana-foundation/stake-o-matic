use crate::BoxResult;
use reqwest::Url;
use std::collections::HashMap;
use std::env;

const SLACK_API_URL: &str = "https://slack.com/api/";

pub fn send_slack_channel_message(message: &str) -> BoxResult<()> {
    let (token, channel_id) = get_token_and_channel()?;
    let client = reqwest::blocking::Client::new();

    let mut body: HashMap<&str, &str> = HashMap::new();
    body.insert("text", message);
    body.insert("channel", &*channel_id);

    client
        .post(
            Url::parse(SLACK_API_URL)
                .unwrap()
                .join("chat.postMessage")
                .unwrap(),
        )
        // .header("Content-type", "application/json")
        .header("Authorization", format!("Bearer {}", token))
        .json(&body)
        .send()?;
    Ok(())
}

/// Returns Slack token and channel to post in
fn get_token_and_channel() -> BoxResult<(String, String)> {
    let token = env::var("SLACK_API_TOKEN")?;
    let channel_id = env::var("SLACK_CHANNEL_ID")?;

    Ok((token, channel_id))
}
