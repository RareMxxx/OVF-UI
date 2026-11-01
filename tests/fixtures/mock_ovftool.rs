//! Test fixture only. Never distributed with the application.
use std::io::Write;
fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|a| a == "--version") {
        println!("VMware ovftool 4.6 — TEST FIXTURE");
        return;
    }
    if args.iter().any(|a| a == "--verifyOnly") {
        println!("The manifest validates (fixture)");
        return;
    }
    println!("PROGRESS");
    for p in [0, 25, 50, 75, 100] {
        println!("+ {p}");
        std::io::stdout().flush().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    println!("RESULT\n+ SUCCESS");
}
