use emsqrt_core::schema::{Field, DataType, Schema};
use emsqrt_core::dag::LogicalPlan as L;
use emsqrt_core::config::EngineConfig;
use emsqrt_planner::{rules, lower_to_physical, estimate_work};
use emsqrt_te::plan_te;
use emsqrt_exec::Engine;
use std::fs;
use std::io::Write;

#[test]
fn e2e_smoke() {
    // Create a temporary directory and CSV file for the test
    let temp_dir = "/tmp/emsqrt-e2e-smoke";
    fs::create_dir_all(temp_dir).expect("Failed to create temp dir");
    
    let input_file = format!("{}/dev.csv", temp_dir);
    let output_file = format!("{}/out.csv", temp_dir);
    
    // Create a simple CSV file with headers
    let mut file = fs::File::create(&input_file).expect("Failed to create input file");
    writeln!(file, "ts,uid").unwrap();
    writeln!(file, "2023-01-01,alice").unwrap();
    writeln!(file, "2023-01-02,bob").unwrap();
    
    // Logical: scan→project→sink
    let schema = Schema::new(vec![
        Field::new("ts", DataType::Utf8, false),
        Field::new("uid", DataType::Utf8, false),
    ]);
    let lp = L::Scan { source: format!("file://{}", input_file), schema: schema.clone() };
    let lp = L::Project { input: Box::new(lp), columns: vec!["ts".into()] };
    let lp = L::Sink { input: Box::new(lp), destination: format!("file://{}", output_file), format: "csv".into() };
    let lp = rules::optimize(lp);
    let phys_prog = lower_to_physical(&lp);
    let work = estimate_work(&lp, None);
    let te = plan_te(&phys_prog.plan, &work, 64 * 1024 * 1024).unwrap();
    
    let mut config = EngineConfig::default();
    config.spill_dir = format!("{}/spill", temp_dir);
    let mut eng = Engine::new(config);
    let manifest = eng.run(&phys_prog, &te).unwrap();
    assert!(manifest.started_ms <= manifest.finished_ms);
    
    // Cleanup
    let _ = fs::remove_dir_all(temp_dir);
}

