use std::{
    ffi::CString,
    fs::File,
    io,
    mem::MaybeUninit,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::ffi::OsStrExt,
        unix::fs::MetadataExt,
    },
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use fs2::FileExt;

pub struct InstanceLock {
    file: File,
    directory: File,
    path: PathBuf,
    socket_path: PathBuf,
    socket_name: CString,
}

impl InstanceLock {
    pub fn acquire(path: impl AsRef<Path>, socket: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let socket = socket.as_ref();
        let parent = path.parent().context("runtime lock path has no parent")?;
        crate::runtime::ipc::security::validate_secure_directory(parent)?;
        if socket.parent() != Some(parent) {
            bail!("runtime socket must be a direct child of the locked directory");
        }
        let lock_name = child_name(path, "runtime lock")?;
        let socket_name = child_name(socket, "runtime socket")?;
        if lock_name == socket_name {
            bail!("runtime lock and socket names must differ");
        }
        let directory = open_directory(parent)?;
        validate_protected_directory(&directory, parent)?;
        let descriptor = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                lock_name.as_ptr(),
                libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0o600,
            )
        };
        if descriptor < 0 {
            return Err(io::Error::last_os_error())
                .with_context(|| format!("failed to open runtime lock {}", path.display()));
        }
        let file = unsafe { File::from_raw_fd(descriptor) };
        let metadata = file.metadata()?;
        let effective = unsafe { libc::geteuid() };
        if !metadata.is_file() || metadata.uid() != effective || metadata.mode() & 0o777 != 0o600 {
            bail!(
                "runtime lock must be an effective-UID-owned mode-0600 regular file: {}",
                path.display()
            );
        }
        file.try_lock_exclusive()
            .with_context(|| format!("another runtime owns lock {}", path.display()))?;
        Ok(Self {
            file,
            directory,
            path: path.to_owned(),
            socket_path: socket.to_owned(),
            socket_name,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub fn remove_stale_socket(&self) -> Result<()> {
        let mut status = MaybeUninit::<libc::stat>::uninit();
        let result = unsafe {
            libc::fstatat(
                self.directory.as_raw_fd(),
                self.socket_name.as_ptr(),
                status.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::NotFound {
                return Ok(());
            }
            return Err(error).with_context(|| {
                format!(
                    "failed to inspect stale socket {}",
                    self.socket_path.display()
                )
            });
        }
        let status = unsafe { status.assume_init() };
        let effective = unsafe { libc::geteuid() };
        if status.st_mode & libc::S_IFMT != libc::S_IFSOCK || status.st_uid != effective {
            bail!(
                "refusing to remove unsafe stale socket {}",
                self.socket_path.display()
            );
        }
        if unsafe { libc::unlinkat(self.directory.as_raw_fd(), self.socket_name.as_ptr(), 0) } < 0 {
            return Err(io::Error::last_os_error()).with_context(|| {
                format!(
                    "failed to remove stale socket {}",
                    self.socket_path.display()
                )
            });
        }
        Ok(())
    }
}

fn child_name(path: &Path, description: &str) -> Result<CString> {
    let name = path
        .file_name()
        .with_context(|| format!("{description} has no file name"))?;
    CString::new(name.as_bytes()).with_context(|| format!("{description} contains a NUL byte"))
}

fn open_directory(path: &Path) -> Result<File> {
    let path_bytes = CString::new(path.as_os_str().as_bytes())
        .context("runtime directory contains a NUL byte")?;
    let descriptor = unsafe {
        libc::open(
            path_bytes.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if descriptor < 0 {
        return Err(io::Error::last_os_error()).with_context(|| {
            format!(
                "failed to open protected runtime directory {}",
                path.display()
            )
        });
    }
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

fn validate_protected_directory(directory: &File, path: &Path) -> Result<()> {
    let metadata = directory.metadata()?;
    let effective = unsafe { libc::geteuid() };
    if !metadata.is_dir() || metadata.uid() != effective || metadata.mode() & 0o777 != 0o700 {
        bail!(
            "protected runtime directory must be effective-UID-owned mode 0700: {}",
            path.display()
        );
    }
    Ok(())
}

impl Drop for InstanceLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::PermissionsExt};

    use anyhow::Result;

    use super::InstanceLock;

    fn short_temp() -> &'static std::path::Path {
        #[cfg(target_os = "macos")]
        return std::path::Path::new("/private/tmp");
        #[cfg(target_os = "linux")]
        return std::path::Path::new("/tmp");
    }

    #[test]
    fn lock_is_exclusive_and_only_owner_removes_stale_socket() -> Result<()> {
        let root = short_temp().join(format!("cl-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        let lock_path = root.join("runtime.lock");
        let socket_path = root.join("codrik.sock");
        let stale = std::os::unix::net::UnixListener::bind(&socket_path)?;
        drop(stale);

        let owner = InstanceLock::acquire(&lock_path, &socket_path)?;
        assert!(InstanceLock::acquire(&lock_path, &socket_path).is_err());
        assert!(socket_path.exists());
        owner.remove_stale_socket()?;
        assert!(!socket_path.exists());
        drop(owner);
        assert!(InstanceLock::acquire(&lock_path, &socket_path).is_ok());
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn stale_cleanup_rejects_a_regular_file() -> Result<()> {
        let root = short_temp().join(format!("cl-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        let victim = root.join("codrik.sock");
        let owner = InstanceLock::acquire(root.join("runtime.lock"), &victim)?;
        fs::write(&victim, b"not a socket")?;
        assert!(owner.remove_stale_socket().is_err());
        assert_eq!(fs::read(&victim)?, b"not a socket");
        drop(owner);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn lock_rejects_a_group_writable_parent() -> Result<()> {
        let root = short_temp().join(format!("cl-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o770))?;
        assert!(
            InstanceLock::acquire(root.join("runtime.lock"), root.join("codrik.sock")).is_err()
        );
        fs::remove_dir(root)?;
        Ok(())
    }

    #[test]
    fn existing_lock_must_be_exactly_0600() -> Result<()> {
        let root = short_temp().join(format!("cl-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        let lock = root.join("runtime.lock");
        fs::write(&lock, b"")?;
        fs::set_permissions(&lock, fs::Permissions::from_mode(0o640))?;
        assert!(InstanceLock::acquire(&lock, root.join("codrik.sock")).is_err());
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn cleanup_is_bound_to_the_configured_socket() -> Result<()> {
        let root = short_temp().join(format!("cl-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        let intended = root.join("codrik.sock");
        let unrelated = root.join("other.sock");
        drop(std::os::unix::net::UnixListener::bind(&intended)?);
        drop(std::os::unix::net::UnixListener::bind(&unrelated)?);
        let owner = InstanceLock::acquire(root.join("runtime.lock"), &intended)?;
        owner.remove_stale_socket()?;
        assert!(!intended.exists());
        assert!(unrelated.exists());
        drop(owner);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn cleanup_uses_locked_directory_after_parent_path_swap() -> Result<()> {
        let root = short_temp().join(format!("cl-{}", uuid::Uuid::new_v4()));
        let moved = root.with_extension("moved");
        fs::create_dir(&root)?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        let socket = root.join("codrik.sock");
        drop(std::os::unix::net::UnixListener::bind(&socket)?);
        let owner = InstanceLock::acquire(root.join("runtime.lock"), &socket)?;
        fs::rename(&root, &moved)?;
        fs::create_dir(&root)?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        drop(std::os::unix::net::UnixListener::bind(&socket)?);

        owner.remove_stale_socket()?;

        assert!(socket.exists());
        assert!(!moved.join("codrik.sock").exists());
        drop(owner);
        fs::remove_dir_all(root)?;
        fs::remove_dir_all(moved)?;
        Ok(())
    }
}
