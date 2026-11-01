#[cfg(feature = "test-fixtures")]
fn main() -> eframe::Result {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let mut store = ovf_dock::store::Store::open().unwrap();
    store.directory = temp.path().into();
    let manager = ovf_dock::jobs::JobManager::new(store, rt.handle().clone());
    struct Preview {
        manager: ovf_dock::jobs::JobManager,
        queue: bool,
    }
    impl eframe::App for Preview {
        fn update(&mut self, ctx: &eframe::egui::Context, _: &mut eframe::Frame) {
            ovf_dock::ui::visual_fixture(ctx, &self.manager, self.queue);
        }
    }
    eframe::run_native(
        "OVF UI · UI test",
        eframe::NativeOptions {
            viewport: eframe::egui::ViewportBuilder::default().with_inner_size([920.0, 800.0]),
            ..Default::default()
        },
        Box::new(move |cc| {
            if std::env::args().any(|s| s == "--workspace-preview") {
                return Ok(Box::new(ovf_dock::ui::DockApp::new(
                    cc,
                    rt.handle().clone(),
                )));
            }
            Ok(Box::new(Preview {
                manager,
                queue: std::env::args().any(|s| s == "--queue"),
            }))
        }),
    )
}
#[cfg(not(feature = "test-fixtures"))]
fn main() {
    eprintln!("Use --features test-fixtures");
}
