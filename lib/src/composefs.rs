//! # Creating composefs images
//!
//! This code wraps `mkcomposefs` from the composefs project.

use std::ffi::OsString;
use std::fmt::Display;
use std::fmt::Write as WriteFmt;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{anyhow, Result};

struct Xattr {
    key: Vec<u8>,
    value: Vec<u8>,
}
type Xattrs = Vec<Xattr>;

struct Mtime {
    sec: u64,
    nsec: u64,
}

struct Entry {
    path: Vec<u8>,
    uid: u32,
    gid: u32,
    mode: u32,
    mtime: Mtime,
    item: Item,
    xattrs: Xattrs,
}

enum RegularContent {
    Inline(Vec<u8>),
    External { path: Vec<u8>, digest: String },
}

enum Item {
    Regular {
        size: u64,
        nlink: u32,
        content: RegularContent,
    },
    Device {
        nlink: u32,
        rdev: u32,
    },
    Symlink {
        nlink: u32,
        target: PathBuf,
    },
    Hardlink {
        target: PathBuf,
    },
    Directory {},
}

fn unescape(s: &str) -> Result<Vec<u8>> {
    let mut it = s.chars();
    let mut r = Vec::new();
    while let Some(c) = it.next() {
        if !c.is_ascii() {
            dbg!(c);
            let mut b = [0; 4];
            c.encode_utf8(&mut b);
            r.extend(b);
            continue;
        }
        if c != '\\' {
            r.push(c as u8);
            continue;
        }
        let c = it.next().ok_or_else(|| anyhow!("Unterminated escape"))?;
        let c = match c {
            '\\' => b'\\',
            'n' => b'\n',
            'r' => b'\r',
            't' => b'\t',
            'x' => {
                let mut s = String::new();
                s.push(
                    it.next()
                        .ok_or_else(|| anyhow!("Unterminated hex escape"))?,
                );
                s.push(
                    it.next()
                        .ok_or_else(|| anyhow!("Unterminated hex escape"))?,
                );
                let v = u8::from_str_radix(&s, 16)?;
                v
            }
            o => anyhow::bail!("Invalid escape {o}"),
        };
        r.push(c);
    }
    Ok(r)
}

fn escape(s: &[u8]) -> String {
    let mut r = String::new();
    for c in s.iter().copied() {
        if c == b'\\' {
            r.push_str(r"\\");
            continue;
        }
        if c.is_ascii_graphic() {
            r.push(c as char);
        } else {
            match c {
                b'\n' => r.push_str(r"\n"),
                b'\t' => r.push_str(r"\t"),
                o => {
                    write!(r, "\\x{:x}", o).unwrap();
                }
            }
        }
    }
    r
}

fn optional_str(s: &str) -> Option<&str> {
    match s {
        "-" => None,
        o => Some(o),
    }
}

impl FromStr for Mtime {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let (sec, nsec) = s
            .split_once('.')
            .ok_or_else(|| anyhow!("Missing . in mtime"))?;
        Ok(Self {
            sec: u64::from_str(sec)?,
            nsec: u64::from_str(nsec)?,
        })
    }
}

impl FromStr for Xattr {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let (key, value) = s
            .split_once('=')
            .ok_or_else(|| anyhow!("Missing = in xattrs"))?;
        let key = unescape(key)?;
        let value = unescape(value)?;
        Ok(Self { key, value })
    }
}

impl std::str::FromStr for Entry {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let mut components = s.split(' ');
        let mut next = |name: &str| components.next().ok_or_else(|| anyhow!("Missing {name}"));
        let path = next("path")?;
        let path = unescape(path)?;
        let size = u64::from_str(next("size")?)?;
        let modeval = next("mode")?;
        let (is_hardlink, mode) = if let Some((_, rest)) = modeval.split_once('@') {
            (true, u32::from_str_radix(rest, 8)?)
        } else {
            (false, u32::from_str_radix(modeval, 8)?)
        };
        let nlink = u32::from_str(next("nlink")?)?;
        let uid = u32::from_str(next("uid")?)?;
        let gid = u32::from_str(next("gid")?)?;
        let rdev = u32::from_str(next("rdev")?)?;
        let mtime = Mtime::from_str(next("mtime")?)?;
        let payload = optional_str(next("payload")?);
        let content = optional_str(next("content")?);
        let digest = optional_str(next("digest")?);
        let xattrs = components
            .map(Xattr::from_str)
            .collect::<Result<Vec<_>>>()?;

        let item = if is_hardlink {
            let target = OsString::from_vec(unescape(
                payload.ok_or_else(|| anyhow!("Missing payload"))?,
            )?)
            .into();
            Item::Hardlink { target }
        } else {
            match libc::S_IFMT & mode {
                libc::S_IFREG => {
                    let inline_content = content.map(unescape).transpose()?;
                    let external = match (digest, payload) {
                        (None, None) => None,
                        (Some(digest), Some(payload)) => Some((digest, unescape(payload)?)),
                        _ => anyhow::bail!("Must specify digest and payload together"),
                    };
                    let content = match (inline_content, external) {
                        (None, None) => anyhow::bail!("Missing content or digest"),
                        (None, Some((digest, path))) => RegularContent::External {
                            path,
                            digest: digest.to_owned(),
                        },
                        (Some(inline), None) => RegularContent::Inline(inline),
                        (Some(_), Some(_)) => anyhow::bail!("Cannot specify content and digest"),
                    };
                    Item::Regular {
                        size,
                        nlink,
                        content,
                    }
                }
                libc::S_IFLNK => {
                    let target = OsString::from_vec(unescape(
                        payload.ok_or_else(|| anyhow!("Missing payload"))?,
                    )?)
                    .into();
                    Item::Symlink { nlink, target }
                }
                libc::S_IFCHR | libc::S_IFBLK => Item::Device { nlink, rdev },
                libc::S_IFDIR => Item::Directory {},
                o => {
                    anyhow::bail!("Unhandled mode {o:o}")
                }
            }
        };
        Ok(Entry {
            path,
            uid,
            gid,
            mode,
            mtime,
            item,
            xattrs,
        })
    }
}

impl Item {
    pub(crate) fn size(&self) -> u64 {
        match self {
            Item::Regular { size, .. } => *size,
            _ => 0,
        }
    }

    pub(crate) fn nlink(&self) -> u32 {
        match self {
            Item::Regular { nlink, .. } => *nlink,
            Item::Device { nlink, .. } => *nlink,
            Item::Symlink { nlink, .. } => *nlink,
            _ => 0,
        }
    }

    pub(crate) fn rdev(&self) -> u32 {
        match self {
            Item::Device { rdev, .. } => *rdev,
            _ => 0,
        }
    }
}

impl Display for Mtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.sec, self.nsec)
    }
}

impl Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", escape(&self.path))?;
        write!(
            f,
            " {} {:o} {} {} {} {} {} ",
            self.item.size(),
            self.mode,
            self.item.nlink(),
            self.uid,
            self.gid,
            self.item.rdev(),
            self.mtime,
        )?;
        match &self.item {
            Item::Regular { content, .. } => match content {
                RegularContent::Inline(data) => {
                    let escaped = escape(&data);
                    write!(f, "- {escaped} -")?;
                }
                RegularContent::External { path, digest } => {
                    let path_escaped = escape(path);
                    write!(f, "{path_escaped} - {digest}")?;
                }
            },
            Item::Hardlink { target, .. } | Item::Symlink { target, .. } => {
                let escaped = escape(target.as_os_str().as_bytes());
                write!(f, "{escaped} - -")?;
            }
            _ => {
                write!(f, "- -")?;
            }
        }
        for xattr in self.xattrs.iter() {
            write!(f, " {}={}", escape(&xattr.key), escape(&xattr.value))?;
        }
        std::fmt::Result::Ok(())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Context;

    use super::*;

    #[test]
    fn test_escaping() {
        let idents = ["abc0123/-"];
        for ident in idents {
            assert_eq!(escape(ident.as_bytes()), ident);
        }
        for ident in ["/a‐dir"] {
            assert_eq!(unescape(ident).unwrap(), ident.as_bytes());
        }
        assert_eq!(escape(b" "), r"\x20");
        assert_eq!(unescape(r"\x20").unwrap(), b" ");
    }

    #[test]
    fn test_parse() {
        const CONTENT: &str = include_str!("fixtures/composefs-example.txt");
        let entries = CONTENT
            .lines()
            .enumerate()
            .map(|(i, line)| Entry::from_str(line).with_context(|| format!("Line {i}")))
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(entries[2].uid, 1000);
        assert_eq!(entries[2].mtime.sec, 1674041780);
        assert_eq!(entries[2].mtime.nsec, 601887980);
        assert_eq!(entries[2].path, b"/a-dir");
        for entry in entries {
            println!("{entry}");
        }
    }
}
