use anyhow::{Context, Result};
use rusqlite::Connection;
use std::{
    fs::{File, OpenOptions},
    path::Path,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct Db(Arc<Mutex<Connection>>);
impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Connection::open(path)?;
        db.busy_timeout(std::time::Duration::from_secs(5))?;
        db.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;",
        )?;
        Ok(Self(Arc::new(Mutex::new(db))))
    }
    pub async fn call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let db = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = db
                .lock()
                .map_err(|_| anyhow::anyhow!("database worker panicked"))?;
            f(&mut connection)
        })
        .await
        .context("database worker failed")?
    }
}
pub fn try_lock(path: &Path) -> Result<Option<File>> {
    std::fs::create_dir_all(path.parent().context("lock has no parent")?)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    match fs2::FileExt::try_lock_exclusive(&file) {
        Ok(()) => Ok(Some(file)),
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
        Err(e) => Err(e.into()),
    }
}
