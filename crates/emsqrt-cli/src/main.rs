//! EM-√ CLI: Command-line interface for running pipelines.

use clap::{Parser, Subcommand};
use emsqrt_core::config::EngineConfig;
use emsqrt_exec::Engine;
use emsqrt_planner::{estimate_work, lower_to_physical, parse_yaml_pipeline, rules};
use emsqrt_te::plan_te;
use std::fs;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "emsqrt")]
#[command(about = "EM-√: External-Memory ETL Engine with hard peak-RAM guarantees", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Execute a pipeline from a YAML file
    Run {
        /// Path to the pipeline YAML file
        #[arg(short, long)]
        pipeline: PathBuf,
        
        /// Memory cap in bytes (overrides config)
        #[arg(long)]
        memory_cap: Option<usize>,
        
        /// Spill directory (overrides config)
        #[arg(long)]
        spill_dir: Option<String>,
        
        /// Maximum parallel tasks (overrides config)
        #[arg(long)]
        max_parallel: Option<usize>,
    },
    
    /// Validate a pipeline YAML file (syntax check)
    Validate {
        /// Path to the pipeline YAML file
        #[arg(short, long)]
        pipeline: PathBuf,
    },
    
    /// Show execution plan for a pipeline (EXPLAIN)
    Explain {
        /// Path to the pipeline YAML file
        #[arg(short, long)]
        pipeline: PathBuf,
        
        /// Memory cap in bytes (for planning)
        #[arg(long, default_value = "536870912")] // 512MB default
        memory_cap: usize,
    },
}

fn main() {
    let cli = Cli::parse();
    
    match cli.command {
        Commands::Run {
            pipeline,
            memory_cap,
            spill_dir,
            max_parallel,
        } => {
            if let Err(e) = run_pipeline(&pipeline, memory_cap, spill_dir, max_parallel) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        Commands::Validate { pipeline } => {
            if let Err(e) = validate_pipeline(&pipeline) {
                eprintln!("Validation failed: {}", e);
                std::process::exit(1);
            }
            println!("✓ Pipeline is valid");
        }
        Commands::Explain {
            pipeline,
            memory_cap,
        } => {
            if let Err(e) = explain_pipeline(&pipeline, memory_cap) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }
}

fn run_pipeline(
    pipeline_path: &PathBuf,
    memory_cap: Option<usize>,
    spill_dir: Option<String>,
    max_parallel: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Read YAML file
    let yaml_content = fs::read_to_string(pipeline_path)?;
    
    // Parse pipeline
    let logical_plan = parse_yaml_pipeline(&yaml_content)?;
    
    // Optimize
    let optimized = rules::optimize(logical_plan);
    
    // Lower to physical plan
    let phys_prog = lower_to_physical(&optimized);
    
    // Estimate work
    let work = estimate_work(&optimized, None);
    
    // Create config
    let mut config = EngineConfig::from_env();
    if let Some(cap) = memory_cap {
        config.mem_cap_bytes = cap;
    }
    if let Some(dir) = spill_dir {
        config.spill_dir = dir;
    }
    if let Some(parallel) = max_parallel {
        config.max_parallel_tasks = parallel;
    }
    
    // Plan TE execution
    let te = plan_te(&phys_prog.plan, &work, config.mem_cap_bytes)
        .map_err(|e| format!("TE planning failed: {}", e))?;
    
    // Execute
    let mut engine = Engine::new(config);
    let manifest = engine.run(&phys_prog, &te)?;
    
    println!("✓ Pipeline executed successfully");
    println!("  Duration: {}ms", manifest.finished_ms - manifest.started_ms);
    println!("  Plan hash: {}", manifest.plan_hash);
    
    Ok(())
}

fn validate_pipeline(pipeline_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let yaml_content = fs::read_to_string(pipeline_path)?;
    let _logical_plan = parse_yaml_pipeline(&yaml_content)?;
    Ok(())
}

fn explain_pipeline(
    pipeline_path: &PathBuf,
    memory_cap: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let yaml_content = fs::read_to_string(pipeline_path)?;
    let logical_plan = parse_yaml_pipeline(&yaml_content)?;
    let optimized = rules::optimize(logical_plan);
    let phys_prog = lower_to_physical(&optimized);
    let work = estimate_work(&optimized, None);
    let te = plan_te(&phys_prog.plan, &work, memory_cap)
        .map_err(|e| format!("TE planning failed: {}", e))?;
    
    println!("Pipeline Execution Plan");
    println!("======================");
    println!();
    println!("Memory Cap: {} bytes ({:.2} MB)", memory_cap, memory_cap as f64 / 1_048_576.0);
    println!();
    println!("Work Estimate:");
    println!("  Total Rows: {}", work.total_rows);
    println!("  Total Bytes: {} ({:.2} MB)", work.total_bytes, work.total_bytes as f64 / 1_048_576.0);
    println!("  Max Fan-in: {}", work.max_fan_in);
    println!();
    println!("TE Plan:");
    println!("  Block Size: {} rows per block", te.block_size.rows_per_block);
    println!("  Total Blocks: {}", te.order.len());
    if let Some(max_frontier) = te.max_frontier_hint {
        println!("  Max Frontier: {} blocks", max_frontier);
    }
    println!();
    println!("Block Execution Order:");
    for (i, block) in te.order.iter().enumerate() {
        println!("  {}. Block {} (Op {}) - {} dependencies", 
                 i + 1, block.id.get(), block.op.get(), block.deps.len());
    }
    
    Ok(())
}

