//! Versioned, configuration-independent TREV runs prepared from validated rows.
//! Cursor JSON is kept separately and decoded only at a returned batch boundary.
use crate::{
    stream::{Batch, Cursor},
    Error, Result,
};

pub const WIDTH: usize = 89;
const MAGIC: &[u8; 8] = b"TREVPR02";

pub struct Prepared {
    pub width: usize,
    pub count: usize,
    pub runs: usize,
    pub minimum_timestamp: i64,
    pub previous: Option<Cursor>,
    pub first: Option<Cursor>,
    pub last: Option<Cursor>,
    pub records: usize,
    pub cursors: usize,
}

pub fn encode(
    batch: &Batch,
    cursor: impl Fn(Option<&Cursor>) -> Result<Vec<u8>>,
) -> Result<Vec<u8>> {
    let mut records = Vec::new();
    let mut cursors = Vec::new();
    let mut i = 0;
    let mut minimum = i64::MAX;
    while i < batch.rows.len() {
        let first = &batch.rows[i];
        let mut end = i + 1;
        let mut buy = 0u128;
        let mut sell = 0u128;
        minimum = minimum.min(first.cursor.timestamp);
        while end < batch.rows.len() {
            let row = &batch.rows[end];
            if row.segment_start || (row.price, row.spec) != (first.price, first.spec) {
                break;
            }
            let units = if row.buy { &mut buy } else { &mut sell };
            *units = units
                .checked_add(u128::from(row.quantity))
                .ok_or(Error::Overflow)?;
            minimum = minimum.min(row.cursor.timestamp);
            end += 1;
        }
        let last = &batch.rows[end - 1];
        for value in [
            first.price as u64,
            first.spec,
            first.cursor.timestamp as u64,
            first.quantity,
        ] {
            records.extend_from_slice(&value.to_le_bytes());
        }
        records.push(u8::from(first.buy) | (u8::from(first.segment_start) << 1));
        records.extend_from_slice(
            &u32::try_from(end - i)
                .map_err(|_| Error::Overflow)?
                .to_le_bytes(),
        );
        for value in [
            batch.rows[(i + 1).min(end - 1)].cursor.timestamp,
            last.cursor.timestamp,
        ] {
            records.extend_from_slice(&value.to_le_bytes());
        }
        records.extend_from_slice(&buy.to_le_bytes());
        records.extend_from_slice(&sell.to_le_bytes());
        records.extend_from_slice(
            &u32::try_from(cursors.len())
                .map_err(|_| Error::Overflow)?
                .to_le_bytes(),
        );
        blob(&mut cursors, &cursor(Some(&last.cursor))?)?;
        i = end;
    }
    let mut bytes = MAGIC.to_vec();
    bytes.extend_from_slice(
        &u32::try_from(batch.rows.len())
            .map_err(|_| Error::Overflow)?
            .to_le_bytes(),
    );
    bytes.extend_from_slice(
        &u32::try_from(records.len() / WIDTH)
            .map_err(|_| Error::Overflow)?
            .to_le_bytes(),
    );
    bytes.extend_from_slice(&minimum.to_le_bytes());
    blob(&mut bytes, &cursor(batch.previous.as_ref())?)?;
    blob(&mut bytes, &cursor(batch.rows.first().map(|r| &r.cursor))?)?;
    blob(&mut bytes, &cursor(batch.rows.last().map(|r| &r.cursor))?)?;
    bytes.extend(records);
    bytes.extend(cursors);
    Ok(bytes)
}

fn blob(out: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    out.extend_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| Error::Overflow)?
            .to_le_bytes(),
    );
    out.extend_from_slice(value);
    Ok(())
}

pub fn u32_at(data: &[u8], at: usize) -> Result<u32> {
    Ok(u32::from_le_bytes(
        data.get(at..at + 4)
            .ok_or(Error::Data("truncated prepared tape"))?
            .try_into()
            .unwrap(),
    ))
}
pub fn u64_at(data: &[u8], at: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(
        data.get(at..at + 8)
            .ok_or(Error::Data("truncated prepared tape"))?
            .try_into()
            .unwrap(),
    ))
}
pub fn u128_at(data: &[u8], at: usize) -> Result<u128> {
    Ok(u128::from_le_bytes(
        data.get(at..at + 16)
            .ok_or(Error::Data("truncated prepared tape"))?
            .try_into()
            .unwrap(),
    ))
}
pub fn read_blob<'a>(data: &'a [u8], offset: &mut usize) -> Result<&'a str> {
    let length = u32_at(data, *offset)? as usize;
    *offset += 4;
    let value = data
        .get(*offset..*offset + length)
        .ok_or(Error::Data("truncated prepared cursor"))?;
    *offset += length;
    std::str::from_utf8(value).map_err(|_| Error::Data("invalid prepared cursor"))
}

impl Prepared {
    pub fn new(bytes: &[u8], cursor: impl Fn(&str) -> Result<Option<Cursor>>) -> Result<Self> {
        let width = match bytes.get(..8) {
            Some(b"TREVPR02") => WIDTH,
            Some(b"TREVPR03") => 40,
            _ => return Err(Error::Data("invalid prepared tape version")),
        };
        let count = u32_at(&bytes, 8)? as usize;
        let runs = u32_at(&bytes, 12)? as usize;
        let minimum_timestamp = u64_at(&bytes, 16)? as i64;
        let mut records = 24;
        let previous = cursor(read_blob(&bytes, &mut records)?)?;
        let first = cursor(read_blob(&bytes, &mut records)?)?;
        let last = cursor(read_blob(&bytes, &mut records)?)?;
        let cursors = records
            .checked_add(runs.checked_mul(width).ok_or(Error::Overflow)?)
            .ok_or(Error::Overflow)?;
        if cursors > bytes.len() || (runs == 0) != (count == 0) {
            return Err(Error::Data("invalid prepared tape bounds"));
        }
        Ok(Self {
            width,
            count,
            runs,
            minimum_timestamp,
            previous,
            first,
            last,
            records,
            cursors,
        })
    }
    pub fn cursor<'a>(
        &self,
        bytes: &'a [u8],
        separate: Option<&'a [u8]>,
        index: usize,
    ) -> Result<&'a str> {
        if index >= self.runs {
            return Err(Error::Data("invalid prepared run"));
        }
        if bytes.len() == self.cursors && separate.is_none() {
            return Err(Error::PreparedCursors);
        }
        let offset = u32_at(bytes, self.records + (index + 1) * self.width - 4)? as usize;
        let (data, mut at) = if let Some(separate) = separate {
            (separate, offset)
        } else {
            (bytes, self.cursors + offset)
        };
        read_blob(data, &mut at)
    }

    pub fn run(&self, data: &[u8], index: usize) -> Result<Run> {
        let at = self.records + index * self.width;
        if self.width == 40 {
            let count_flags = u32_at(data, at + 16)?;
            let timestamp = |offset| {
                (self.minimum_timestamp as u64)
                    .checked_add(u64::from(u32_at(data, at + offset)?))
                    .ok_or(Error::Overflow)
            };
            Ok(Run {
                price: i64::from(u32_at(data, at)?),
                spec: u64::from(u32_at(data, at + 4)?),
                timestamp: timestamp(8)?,
                quantity: u64::from(u32_at(data, at + 12)?),
                count: count_flags >> 2,
                flags: (count_flags & 3) as u8,
                first: timestamp(20)?,
                last: timestamp(24)?,
                buy: u128::from(u32_at(data, at + 28)?),
                sell: u128::from(u32_at(data, at + 32)?),
            })
        } else {
            Ok(Run {
                price: u64_at(data, at)? as i64,
                spec: u64_at(data, at + 8)?,
                timestamp: u64_at(data, at + 16)?,
                quantity: u64_at(data, at + 24)?,
                flags: data[at + 32],
                count: u32_at(data, at + 33)?,
                first: u64_at(data, at + 37)?,
                last: u64_at(data, at + 45)?,
                buy: u128_at(data, at + 53)?,
                sell: u128_at(data, at + 69)?,
            })
        }
    }

    pub fn compact(&self, data: &[u8]) -> Result<Vec<u8>> {
        if self.width != WIDTH {
            return Ok(data.to_vec());
        }
        let mut records = Vec::with_capacity(self.runs * 40);
        for i in 0..self.runs {
            let r = self.run(data, i)?;
            let minimum = self.minimum_timestamp as u64;
            if r.count >= 1 << 30 || r.timestamp < minimum || r.first < minimum || r.last < minimum
            {
                return Ok(data.to_vec());
            }
            for value in [
                r.price as u128,
                u128::from(r.spec),
                u128::from(r.timestamp - minimum),
                u128::from(r.quantity),
                u128::from((r.count << 2) | u32::from(r.flags)),
                u128::from(r.first - minimum),
                u128::from(r.last - minimum),
                r.buy,
                r.sell,
                u128::from(u32_at(data, self.records + (i + 1) * WIDTH - 4)?),
            ] {
                let Ok(value) = u32::try_from(value) else {
                    return Ok(data.to_vec());
                };
                records.extend_from_slice(&value.to_le_bytes());
            }
        }
        let mut result = data[..self.records].to_vec();
        result[..8].copy_from_slice(b"TREVPR03");
        result.extend(records);
        result.extend_from_slice(&data[self.cursors..]);
        Ok(result)
    }
}

pub struct Run {
    pub price: i64,
    pub spec: u64,
    pub timestamp: u64,
    pub quantity: u64,
    pub flags: u8,
    pub count: u32,
    pub first: u64,
    pub last: u64,
    pub buy: u128,
    pub sell: u128,
}
