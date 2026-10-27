use clap::Parser;
use std::net::IpAddr;

/// Server configuration. Flag names follow `redis.conf` directives where one
/// exists, so they look familiar to Redis users.
#[derive(Debug, Clone, Parser)]
#[command(version, about)]
pub struct Config {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1")]
    pub bind: IpAddr,

    /// TCP port to listen on.
    #[arg(long, default_value_t = 6379)]
    pub port: u16,
}
