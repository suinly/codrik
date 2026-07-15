use std::{
    fs::{self, File, OpenOptions},
    io,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use fs2::FileExt;

pub struct InstanceLock {
    file: File,
    path: PathBuf,
}

impl InstanceLock {
    pub fn acquire(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        validate_lock_parent(path)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(path)
            .with_context(|| format!("failed to open runtime lock {}", path.display()))?;
        let metadata = file.metadata()?;
        let effective = unsafe { libc::geteuid() };
        if !metadata.is_file() || metadata.uid() != effective || metadata.mode() & 0o022 != 0 {
            bail!(
                "runtime lock is not a safe owner-only regular file: {}",
                path.display()
            );
        }
        file.try_lock_exclusive()
            .with_context(|| format!("another runtime owns lock {}", path.display()))?;
        Ok(Self {
            file,
            path: path.to_owned(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn remove_stale_socket(&self, socket: impl AsRef<Path>) -> Result<()> {
        let socket = socket.as_ref();
        match fs::symlink_metadata(socket) {
            Ok(metadata) => {
                let kind = metadata.file_type();
                if !kind.is_socket() {
                    bail!(
                        "refusing to remove non-socket stale path {}",
                        socket.display()
                    );
                }
                fs::remove_file(socket)
                    .with_context(|| format!("failed to remove stale socket {}", socket.display()))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error)
                .with_context(|| format!("failed to inspect stale socket {}", socket.display())),
        }
    }
}

fn validate_lock_parent(path: &Path) -> Result<()> {
    let parent = path.parent().context("runtime lock path has no parent")?;
    let metadata = fs::symlink_metadata(parent)
        .with_context(|| format!("failed to inspect lock parent {}", parent.display()))?;
    let effective = unsafe { libc::geteuid() };
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != effective
        || metadata.mode() & 0o022 != 0
    {
        bail!("unsafe runtime lock parent: {}", parent.display());
    }
    Ok(())
}

impl Drop for InstanceLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

use std::os::unix::fs::FileTypeExt;

#[cfg(test)]
mod tests {
    use std::fs;

    use anyhow::Result;

    use super::InstanceLock;

    #[test]
    fn lock_is_exclusive_and_only_owner_removes_stale_socket() -> Result<()> {
        let root = std::path::PathBuf::from("/tmp").join(format!("cl-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        let lock_path = root.join("runtime.lock");
        let socket_path = root.join("codrik.sock");
        let stale = std::os::unix::net::UnixListener::bind(&socket_path)?;
        drop(stale);

        let owner = InstanceLock::acquire(&lock_path)?;
        assert!(InstanceLock::acquire(&lock_path).is_err());
        assert!(socket_path.exists());
        owner.remove_stale_socket(&socket_path)?;
        assert!(!socket_path.exists());
        drop(owner);
        assert!(InstanceLock::acquire(&lock_path).is_ok());
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn stale_cleanup_rejects_a_regular_file() -> Result<()> {
        let root = std::path::PathBuf::from("/tmp").join(format!("cl-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        let owner = InstanceLock::acquire(root.join("runtime.lock"))?;
        let victim = root.join("codrik.sock");
        fs::write(&victim, b"not a socket")?;
        assert!(owner.remove_stale_socket(&victim).is_err());
        assert_eq!(fs::read(&victim)?, b"not a socket");
        drop(owner);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn lock_rejects_a_group_writable_parent() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let root = std::path::PathBuf::from("/tmp").join(format!("cl-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o770))?;
        assert!(InstanceLock::acquire(root.join("runtime.lock")).is_err());
        fs::remove_dir(root)?;
        Ok(())
    }
}
