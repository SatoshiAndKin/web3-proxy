// So the API needs to show for any given user:
// - show balance in USD
// - show deposits history (currency, amounts, transaction id)
// - show number of requests used (so we can calculate average spending over a month, burn rate for a user etc, something like "Your balance will be depleted in xx days)
// - the email address of a user if he opted in to get contacted via email
// - all the monitoring and stats but that will come from someplace else if I understand corectly?
// I wonder how we handle payment
// probably have to do manual withdrawals

use axum::{http::StatusCode, response::IntoResponse, Json};
use ethers::prelude::{Address, Bytes};
use serde::{Deserialize, Serialize};

pub async fn create_user(
    // this argument tells axum to parse the request body
    // as JSON into a `CreateUser` type
    Json(payload): Json<CreateUser>,
) -> impl IntoResponse {
    // TODO: rate limit by ip
    // TODO: insert your application logic here
    let user = User {
        id: 1337,
        eth_address: payload.eth_address,
    };

    // this will be converted into a JSON response
    // with a status code of `201 Created`
    (StatusCode::CREATED, Json(user))
}

// the input to our `create_user` handler
#[derive(Deserialize)]
pub struct CreateUser {
    eth_address: Address,
    // TODO: validation
    email: Option<String>,
    signature: Bytes,
}

// the output to our `create_user` handler
#[derive(Serialize)]
struct User {
    id: u64,
    eth_address: Address,
}
