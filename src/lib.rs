extern crate diesel;
extern crate diesel_migrations;
extern crate dotenv;
//#![feature(map_try_insert)]
#[macro_use]
extern crate lazy_static;

mod domain;
mod repository;
mod manager;
mod store;
mod utils;

