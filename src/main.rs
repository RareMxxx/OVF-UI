#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() -> eframe::Result {
    let _instance = match ovf_dock::instance::InstanceGuard::acquire() {
        Ok(guard) => guard,
        Err(error) => {
            rfd::MessageDialog::new()
                .set_title("OVF UI уже открыт")
                .set_description(format!("{error:#}"))
                .set_level(rfd::MessageLevel::Info)
                .show();
            return Ok(());
        }
    };
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Не удалось запустить обработчик задач");
    let handle = runtime.handle().clone();
    let options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_inner_size([1280.0, 820.0])
            .with_min_inner_size([1040.0, 680.0])
            .with_icon(app_icon())
            .with_title("OVF UI — VMware Cloud Director"),
        ..Default::default()
    };
    let result = eframe::run_native(
        "OVF UI",
        options,
        Box::new(move |cc| Ok(Box::new(ovf_dock::ui::DockApp::new(cc, handle)))),
    );
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
    result
}

fn app_icon() -> eframe::egui::IconData {
    let mut rgba = vec![0; 64 * 64 * 4];
    for y in 0..64 {
        for x in 0..64 {
            let filled = (6..43).contains(&x) && (6..43).contains(&y);
            let outline = (24..60).contains(&x)
                && (24..60).contains(&y)
                && (!(28..56).contains(&x) || !(28..56).contains(&y));
            if filled || outline {
                let i = (y * 64 + x) * 4;
                rgba[i..i + 4].copy_from_slice(&[235, 48, 67, 255]);
            }
        }
    }
    eframe::egui::IconData {
        rgba,
        width: 64,
        height: 64,
    }
}
