use anyhow::Result;
use arrayvec::ArrayVec;
use chrono::{TimeZone, Utc};
use std::collections::{HashSet, BTreeMap};
use std::convert::{TryFrom, TryInto};
use std::io;
use std::net::Ipv4Addr;

struct MlgStream<R: io::BufRead> {
    reader: R,
}
impl<R: io::BufRead> Iterator for MlgStream<R> {
    type Item = ([u8; 4], [u8; 4]);

    fn next(&mut self) -> Option<Self::Item> {
        let mut record = [0u8; 8];

        match self.reader.read_exact(&mut record) {
            Ok(_) => {
                let dt_bytes: [u8; 4] = record[0..4].try_into().unwrap();
                let ip_bytes: [u8; 4] = record[4..8].try_into().unwrap();
                Some((dt_bytes, ip_bytes))
            }
            Err(_) => None,
        }
    }
}

///
/// Converts apache-like logs:
/// ```text
/// 213.180.203.32 - - [31/Aug/2020:00:00:39 +0000] "GET ...
/// ```
/// To a binary stream of `(timestamp, ipv4)` tuples:
/// ```text
/// 0x5f4c3da7 0xd5b4cb20
/// ```
///
pub fn log2mlg<R: io::BufRead, W: io::Write>(reader: R, mut writer: W) -> Result<()> {
    for line in reader.lines() {
        match parse_log_line(line?) {
            Ok((date, ip)) => {
                writer.write_all(&date)?;
                writer.write_all(&ip)?;
            }
            Err(e) => {
                eprintln!("{}", e);
            }
        }
    }
    Ok(())
}

/// ```
/// let line = "213.180.203.32 - - [31/Aug/2020:00:00:39 +0000] \"GET ...".to_string();
/// assert_eq!(
///     minilog::parse_log_line(line).unwrap(),
///     ([95, 76, 61, 167], [213, 180, 203, 32])
/// )
/// ```
pub fn parse_log_line(line: String) -> Result<([u8; 4], [u8; 4])> {
    let parts: ArrayVec<_, 6> = line.splitn(6, ' ').collect();

    let ipstr = parts.get(0).unwrap();
    let datestr = format!("{} {}", parts.get(3).unwrap(), parts.get(4).unwrap());

    let ip: Ipv4Addr = ipstr.parse()?;
    let date = chrono::DateTime::parse_from_str(&datestr, "[%d/%b/%Y:%H:%M:%S %z]")?;

    Ok(((date.timestamp() as i32).to_be_bytes(), ip.octets()))
}

///
/// Takes a stream of `(timestamp, ipv4)` tuples and prints out a
/// summary of `YYYY-mm-dd: <number of unique IPs>`
///
pub fn mlg2dau<R: io::BufRead>(reader: R) -> Result<()> {
    let mut days = BTreeMap::new();
    let stream = MlgStream { reader };

    for (dt_bytes, ip_bytes) in stream.into_iter() {
        let ip = Ipv4Addr::try_from(ip_bytes)?;
        let timestamp = i32::from_be_bytes(dt_bytes);
        let day = timestamp - (timestamp % 86400);

        (*days.entry(day).or_insert(HashSet::new())).insert(ip);
    }

    for (k, v) in days {
        let dt = Utc.timestamp(k as i64, 0);
        let day = dt.format("%Y-%m-%d").to_string();
        println!("{}: {}", day, v.len());
    }

    Ok(())
}

///
/// Takes a stream of `(timestamp, ipv4)` tuples and prints out a
/// summary of `YYYY-mm: <number of unique IPs>`. Note that this
/// uses calendar months, as opposed to 28-day or 30-day windows.
///
pub fn mlg2mau<R: io::BufRead>(reader: R) -> Result<()> {
    let mut days = BTreeMap::new();
    let mut months = BTreeMap::new();
    let stream = MlgStream { reader };

    // Months aren't fixed lengths, so we start by counting days
    for (dt_bytes, ip_bytes) in stream.into_iter() {
        let ip = Ipv4Addr::try_from(ip_bytes)?;
        let timestamp = i32::from_be_bytes(dt_bytes);
        let day = timestamp - (timestamp % 86400);

        (*days.entry(day).or_insert(HashSet::new())).insert(ip);
    }

    // Then merge days into months
    for (k, v) in days {
        let dt = Utc.timestamp(k as i64, 0);
        let month = dt.format("%Y-%m").to_string();
        (*months.entry(month).or_insert(HashSet::new())).extend(v);
    }

    // Then print months
    for (k, v) in months {
        println!("{}: {}", k, v.len());
    }

    Ok(())
}

///
/// Takes a stream of `(timestamp, ipv4)` tuples and prints out all
/// the unique IPs
///
pub fn mlg2uniq<R: io::BufRead>(reader: R) -> Result<()> {
    let mut ips = HashSet::new();
    let stream = MlgStream { reader };

    for (_dt_bytes, ip_bytes) in stream.into_iter() {
        let ip = Ipv4Addr::try_from(ip_bytes)?;
        ips.insert(ip);
    }

    for ip in ips {
        println!("{}", ip);
    }

    Ok(())
}
