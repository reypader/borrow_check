#[macro_use]
extern crate rocket;

use std::time::Duration;

mod acceptor;
mod accounts;
mod api;
mod books;
mod currency;
mod journal;
mod operation;

#[rocket::main]
async fn main() {
    let acceptor_handle = acceptor::spawn(10, 5, Duration::from_millis(10));
    let _ = rocket::build()
        .manage(acceptor_handle)
        .mount("/", routes![api::post_operations])
        .launch()
        .await;
}
