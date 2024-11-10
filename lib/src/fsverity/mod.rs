pub mod digest;
pub mod ioctl;

pub trait FsVerityHashValue {
    const ALGORITHM: u8;
    const EMPTY: Self;
}

pub type Sha256HashValue = [u8; 32];

impl FsVerityHashValue for Sha256HashValue {
    const ALGORITHM: u8 = 1;
    const EMPTY: Self = [0; 32];
}

pub type Sha512HashValue = [u8; 64];

impl FsVerityHashValue for Sha512HashValue {
    const ALGORITHM: u8 = 2;
    const EMPTY: Self = [0; 64];
}
