mod entrypoint;
pub mod instruction;
pub mod processor;
pub mod state;

pub use solana_program;

solana_program::declare_id!("reg8X1V65CSdmrtEjMgnXZk96b9SUSQrJ8n1rP1ZMg7");

pub mod admin {
    solana_program::declare_id!("AdminzUFhXiGmLZBCBeFQT5ZjQkZsHc2rUh28egNnMwd");
}
