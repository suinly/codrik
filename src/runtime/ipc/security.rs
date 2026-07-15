use std::{
    fs, io,
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, PermissionsExt},
    },
    path::Path,
    sync::{Mutex, OnceLock},
};

use anyhow::{Context, Result, bail};
use tokio::net::{UnixListener, UnixStream};

pub trait PeerCredentials: Send + Sync {
    fn peer_uid(&self, stream: &UnixStream) -> io::Result<u32>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct OsPeerCredentials;

#[cfg(target_os = "linux")]
impl PeerCredentials for OsPeerCredentials {
    fn peer_uid(&self, stream: &UnixStream) -> io::Result<u32> {
        let mut credentials = libc::ucred {
            pid: 0,
            uid: 0,
            gid: 0,
        };
        let mut length = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
        let status = unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut credentials as *mut libc::ucred).cast(),
                &mut length,
            )
        };
        if status == 0 {
            Ok(credentials.uid)
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

#[cfg(target_os = "macos")]
impl PeerCredentials for OsPeerCredentials {
    fn peer_uid(&self, stream: &UnixStream) -> io::Result<u32> {
        let mut uid = 0;
        let mut gid = 0;
        let status = unsafe { libc::getpeereid(stream.as_raw_fd(), &mut uid, &mut gid) };
        if status == 0 {
            Ok(uid)
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("local IPC is supported only on Linux and macOS");

pub struct AuthorizedUnixStream(UnixStream);

impl AuthorizedUnixStream {
    pub fn authorize(stream: UnixStream, credentials: &dyn PeerCredentials) -> Result<Self> {
        let peer = credentials
            .peer_uid(&stream)
            .context("failed to read Unix peer credentials")?;
        let effective = unsafe { libc::geteuid() };
        if peer != effective {
            bail!("Unix peer UID {peer} does not match daemon effective UID {effective}");
        }
        Ok(Self(stream))
    }

    pub fn into_inner(self) -> UnixStream {
        self.0
    }
}

pub fn create_secure_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => validate_secure_directory(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir(path).with_context(|| format!("failed to create {}", path.display()))?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
            validate_secure_directory(path)
        }
        Err(error) => Err(error).with_context(|| format!("failed to inspect {}", path.display())),
    }
}

pub fn validate_secure_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path).with_context(|| {
        format!(
            "failed to inspect security-sensitive path {}",
            path.display()
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!(
            "security-sensitive path is not a real directory: {}",
            path.display()
        );
    }
    let effective = unsafe { libc::geteuid() };
    if metadata.uid() != effective {
        bail!(
            "security-sensitive path {} is owned by UID {}, expected {effective}",
            path.display(),
            metadata.uid()
        );
    }
    if metadata.mode() & 0o022 != 0 {
        bail!(
            "security-sensitive path is group/world writable: {}",
            path.display()
        );
    }
    Ok(())
}

pub fn bind_secure_listener(path: &Path) -> Result<UnixListener> {
    let parent = path.parent().context("socket path has no parent")?;
    validate_secure_directory(parent)?;
    let guard = umask_guard().lock().expect("umask mutex poisoned");
    let old = unsafe { libc::umask(0o077) };
    let bound = std::os::unix::net::UnixListener::bind(path);
    unsafe { libc::umask(old) };
    drop(guard);
    let listener = bound.with_context(|| format!("failed to bind socket {}", path.display()))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o777 != 0o600
    {
        let _ = fs::remove_file(path);
        bail!(
            "bound socket failed ownership or permission validation: {}",
            path.display()
        );
    }
    listener.set_nonblocking(true)?;
    UnixListener::from_std(listener).context("failed to create asynchronous Unix listener")
}

fn umask_guard() -> &'static Mutex<()> {
    static UMASK: OnceLock<Mutex<()>> = OnceLock::new();
    UMASK.get_or_init(|| Mutex::new(()))
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        os::unix::fs::{MetadataExt, PermissionsExt, symlink},
    };

    use anyhow::Result;
    use tokio::net::UnixStream;

    use super::{
        AuthorizedUnixStream, PeerCredentials, bind_secure_listener, create_secure_directory,
        validate_secure_directory,
    };

    struct Credentials(u32);

    impl PeerCredentials for Credentials {
        fn peer_uid(&self, _stream: &UnixStream) -> std::io::Result<u32> {
            Ok(self.0)
        }
    }

    #[test]
    fn directory_is_0700_and_writable_or_symlink_path_is_rejected() -> Result<()> {
        let root = std::env::temp_dir().join(format!("codrik-security-{}", uuid::Uuid::new_v4()));
        create_secure_directory(&root)?;
        assert_eq!(fs::symlink_metadata(&root)?.mode() & 0o777, 0o700);
        validate_secure_directory(&root)?;

        fs::set_permissions(&root, fs::Permissions::from_mode(0o770))?;
        assert!(validate_secure_directory(&root).is_err());
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        let linked = root.with_extension("link");
        symlink(&root, &linked)?;
        assert!(validate_secure_directory(&linked).is_err());
        fs::remove_file(linked)?;
        fs::remove_dir(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn peer_uid_is_checked_before_authorization() -> Result<()> {
        let (ours, _) = UnixStream::pair()?;
        let uid = unsafe { libc::geteuid() };
        assert!(AuthorizedUnixStream::authorize(ours, &Credentials(uid)).is_ok());
        let (theirs, _) = UnixStream::pair()?;
        assert!(
            AuthorizedUnixStream::authorize(theirs, &Credentials(uid.wrapping_add(1))).is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn bound_socket_is_0600_before_use() -> Result<()> {
        let root = std::path::PathBuf::from("/tmp").join(format!("cs-{}", uuid::Uuid::new_v4()));
        create_secure_directory(&root)?;
        let socket = root.join("codrik.sock");
        let listener = bind_secure_listener(&socket)?;
        assert_eq!(fs::symlink_metadata(&socket)?.mode() & 0o777, 0o600);
        drop(listener);
        fs::remove_file(socket)?;
        fs::remove_dir(root)?;
        Ok(())
    }
}
