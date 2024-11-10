use std::os::fd::AsFd;

use anyhow::Result;
use rustix::ioctl;

use super::FsVerityHashValue;

// See /usr/include/linux/fsverity.h
#[repr(C)]
pub struct FsVerityEnableArg {
    version: u32,
    hash_algorithm: u32,
    block_size: u32,
    salt_size: u32,
    salt_ptr: u64,
    sig_size: u32,
    __reserved1: u32,
    sig_ptr: u64,
    __reserved2: [u64; 11],
}

// #define FS_IOC_ENABLE_VERITY    _IOW('f', 133, struct fsverity_enable_arg)
type FsIocEnableVerity = ioctl::WriteOpcode<b'f', 133, FsVerityEnableArg>;

pub fn fs_ioc_enable_verity<F: AsFd, H: FsVerityHashValue>(fd: F) -> Result<()> {
    unsafe {
        ioctl::ioctl(
            fd,
            ioctl::Setter::<FsIocEnableVerity, FsVerityEnableArg>::new(FsVerityEnableArg {
                version: 1,
                hash_algorithm: H::ALGORITHM as u32,
                block_size: 4096,
                salt_size: 0,
                salt_ptr: 0,
                sig_size: 0,
                __reserved1: 0,
                sig_ptr: 0,
                __reserved2: [0; 11],
            }),
        )?;
    }

    Ok(())
}

#[repr(C)]
pub struct FsVerityDigest<F> {
    digest_algorithm: u16,
    digest_size: u16,
    digest: F,
}

// #define FS_IOC_MEASURE_VERITY   _IORW('f', 134, struct fsverity_digest)
type FsIocMeasureVerity = ioctl::ReadWriteOpcode<b'f', 134, FsVerityDigest<()>>;

pub fn fs_ioc_measure_verity<F: AsFd, H: FsVerityHashValue>(fd: F) -> Result<H> {
    let digest_size = std::mem::size_of::<H>() as u16;
    let digest_algorithm = H::ALGORITHM as u16;

    let mut digest = FsVerityDigest::<H> {
        digest_algorithm,
        digest_size,
        digest: H::EMPTY,
    };

    unsafe {
        ioctl::ioctl(
            fd,
            ioctl::Updater::<FsIocMeasureVerity, FsVerityDigest<H>>::new(&mut digest),
        )?;
    }

    if digest.digest_algorithm != digest_algorithm || digest.digest_size != digest_size {
        Err(std::io::Error::from(std::io::ErrorKind::InvalidData))?
    } else {
        Ok(digest.digest)
    }
}
