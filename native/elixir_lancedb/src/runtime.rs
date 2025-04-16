use once_cell::sync::OnceCell;
use tokio::runtime::{Builder, Runtime};
static RUNTIME: OnceCell<Runtime> = OnceCell::new();

pub fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create ElixirLanceDB runtime")
    })
}
