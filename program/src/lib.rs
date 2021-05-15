mod entrypoint;
pub mod instruction;
pub mod processor;
pub mod state;

pub use solana_program;

mod admin {
    solana_program::declare_id!("AdminzUFhXiGmLZBCBeFQT5ZjQkZsHc2rUh28egNnMwd");
}
