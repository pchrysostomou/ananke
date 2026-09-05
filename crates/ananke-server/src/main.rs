//! `ananke-server` assembles one ananke node from the workspace crates.
//!
//! Phase 0: the `echo` subcommand runs the toy echo protocol on `RealEnv`, so the
//! code exercised under simulation in `sim/echo.rs` also runs as real processes
//! (SPEC.md §1.6).

use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::Duration;

use ananke_env::{Clock, Environment, RealEnv};
use ananke_server::echo::{self, Echo, SharedStats};
use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "ananke-server", version, about = "One ananke node")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the Phase 0 echo protocol against a set of peers.
    Echo(EchoArgs),
}

#[derive(Args)]
struct EchoArgs {
    /// Address to bind; peers see it as the sender.
    #[arg(long)]
    listen: SocketAddr,
    /// Peer addresses, comma separated.
    #[arg(long, value_delimiter = ',', required = true)]
    peers: Vec<SocketAddr>,
    /// Seconds to run before reporting and exiting; 0 runs until killed.
    #[arg(long, default_value_t = 3)]
    duration_secs: u64,
    /// Ping interval in milliseconds.
    #[arg(long, default_value_t = 20)]
    interval_ms: u64,
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Echo(args) => run_echo(args),
    }
}

/// Runs the echo node for the requested time, prints its stats, and exits non-zero if
/// the protocol invariants were violated.
fn run_echo(args: EchoArgs) -> ExitCode {
    RealEnv::run(|env| async move {
        let stats = SharedStats::default();
        let config = Echo {
            listen: args.listen,
            peers: args.peers,
            interval: Duration::from_millis(args.interval_ms),
            incarnation: 0,
        };
        let task = env.spawn("echo", echo::node(env.clone(), config, stats.clone()));
        if args.duration_secs == 0 {
            std::future::pending::<()>().await;
        }
        env.clock()
            .sleep(Duration::from_secs(args.duration_secs))
            .await;
        task.abort();

        let stats = echo::lock(&stats).clone();
        println!(
            "echo listen={} pings={} pongs={} outstanding={} unknown={} garbage={}",
            args.listen,
            stats.pings_sent,
            stats.pongs_received,
            stats.outstanding(),
            stats.unknown_pongs,
            stats.garbage
        );
        match stats.check() {
            Ok(()) => {
                println!("ok");
                ExitCode::SUCCESS
            }
            Err(violation) => {
                println!("violation: {violation}");
                ExitCode::FAILURE
            }
        }
    })
}
