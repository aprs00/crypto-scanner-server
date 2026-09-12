//! ClickHouse RowBinary for the existing nine-column projector SELECT.
use crate::{Error, Result};
use std::cmp::Ordering;
use std::collections::HashMap;

// Exchange execution IDs normally fit here (including 36-byte UUIDs). Keeping
// them in the row avoids one allocator round trip for every decoded trade.
// Longer IDs remain lossless and use the same owned bytes as before.
#[derive(Clone, Debug)]
pub struct Identity(IdentityBytes);

#[derive(Clone, Debug)]
enum IdentityBytes {
    Inline { length: u8, bytes: [u8; 40] },
    Heap(Vec<u8>),
}

impl From<&str> for Identity {
    fn from(value: &str) -> Self {
        let value = value.as_bytes();
        if value.len() <= 40 {
            let mut bytes = [0; 40];
            bytes[..value.len()].copy_from_slice(value);
            Self(IdentityBytes::Inline {
                length: value.len() as u8,
                bytes,
            })
        } else {
            Self(IdentityBytes::Heap(value.to_vec()))
        }
    }
}

impl Identity {
    fn bytes(&self) -> &[u8] {
        match &self.0 {
            IdentityBytes::Inline { length, bytes } => &bytes[..usize::from(*length)],
            IdentityBytes::Heap(bytes) => bytes,
        }
    }

    pub fn as_str(&self) -> Result<&str> {
        std::str::from_utf8(self.bytes()).map_err(|_| Error::Data("invalid execution identity"))
    }
}

impl PartialEq for Identity {
    fn eq(&self, other: &Self) -> bool {
        self.bytes() == other.bytes()
    }
}
impl Eq for Identity {}
impl PartialOrd for Identity {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Identity {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bytes().cmp(other.bytes())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Cursor {
    pub partition: Option<(u64, u8)>, // reconciled day and archive/live kind
    pub epoch: u32,
    pub sequence: u64,
    pub batch: u32,
    pub timestamp: i64,
    pub id: Identity,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Execution {
    pub cursor: Cursor,
    pub price: i64,
    pub quantity: u64,
    pub buy: bool,
    pub spec: u64,
    pub segment_start: bool,
}

#[derive(Default)]
pub struct Batch {
    pub rows: Vec<Execution>,
    pub previous: Option<Cursor>,
}

pub struct Decoder {
    data: Vec<u8>,
    offset: usize,
    divisors: HashMap<u64, (u64, u64)>,
    previous: Option<Execution>,
    after: Option<Cursor>,
    through: Option<Cursor>,
    pub done: bool,
}

impl Decoder {
    pub fn new(after: Option<Cursor>, through: Option<Cursor>) -> Self {
        Self {
            data: Vec::new(),
            offset: 0,
            divisors: HashMap::new(),
            previous: None,
            after,
            through,
            done: false,
        }
    }

    pub fn set_divisor(&mut self, spec: u64, price: u64, quantity: u64) {
        self.divisors.insert(spec, (price, quantity));
    }

    pub fn feed(&mut self, data: &[u8]) {
        if self.offset > 0 {
            self.data.drain(..self.offset);
            self.offset = 0;
        }
        self.data.extend_from_slice(data);
    }

    pub fn finish(&self) -> Result<()> {
        if !self.done && self.offset != self.data.len() {
            return Err(Error::Data("truncated RowBinary execution"));
        }
        Ok(())
    }

    pub fn take(&mut self, limit: usize, partition: Option<(u64, u8)>) -> Result<Batch> {
        if limit == 0 {
            return Err(Error::Data("batch size must be positive"));
        }
        let initial = self
            .previous
            .as_ref()
            .map(|e| &e.cursor)
            .or(self.after.as_ref());
        let mut batch = Batch {
            rows: Vec::with_capacity(limit.min((self.data.len() - self.offset) / 51)),
            previous: initial.cloned(),
        };
        let mut offset = self.offset;
        let mut previous = self.previous.as_ref();
        let mut done = self.done;
        // Commit decoder state only after the entire returned batch validates.
        // Loading a previously unseen specification can safely retry this call.
        while !done && batch.rows.len() < limit {
            let Some((mut row, end)) = decode_row(&self.data, offset, partition, &self.divisors)?
            else {
                break;
            };
            offset = end;
            if let Some(through) = &self.through {
                if row.cursor > *through {
                    if partition.is_none() {
                        return Err(Error::Data(
                            "projector execution advanced beyond its through cursor",
                        ));
                    }
                    done = true;
                    break;
                }
            }
            if let Some(after) = &self.after {
                if row.cursor <= *after {
                    if partition.is_some() && row.cursor == *after {
                        continue;
                    }
                    return Err(Error::Data(
                        "projector execution did not advance after its cursor",
                    ));
                }
            }
            let last = batch.rows.last().or(previous);
            if let Some(last) = last {
                if row.cursor < last.cursor {
                    return Err(Error::Data(
                        "projector executions are not in global source order",
                    ));
                }
                if row.cursor == last.cursor {
                    // Segment flags are derived, not execution identity.
                    if (row.price, row.quantity, row.buy, row.spec)
                        != (last.price, last.quantity, last.buy, last.spec)
                    {
                        return Err(Error::Data("conflicting duplicate replay cursor"));
                    }
                    continue;
                }
            }
            let previous_cursor = last.map(|e| &e.cursor).or(self.after.as_ref());
            if let Some(old) = previous_cursor {
                row.segment_start = match (old.partition, partition) {
                    (Some((_, 1)), Some((_, 1))) | (None, None) => row.cursor.epoch != old.epoch,
                    _ => false, // archive/live seams are verified by the existing Python query
                };
            }
            batch.rows.push(row);
            previous = None;
        }
        if let Some(last) = batch.rows.last() {
            self.previous = Some(last.clone());
        }
        self.offset = offset;
        self.done = done;
        Ok(batch)
    }
}

fn decode_row(
    data: &[u8],
    start: usize,
    partition: Option<(u64, u8)>,
    divisors: &HashMap<u64, (u64, u64)>,
) -> Result<Option<(Execution, usize)>> {
    // u32, u64, u32, i64, String(varuint length + UTF-8), i64, u64, Bool, u64
    if data.len() - start < 25 {
        return Ok(None);
    }
    let mut offset = start + 24;
    let mut length = 0u64;
    let mut shift = 0;
    loop {
        let Some(&byte) = data.get(offset) else {
            return Ok(None);
        };
        offset += 1;
        if shift == 63 && byte > 1 {
            return Err(Error::Data("invalid RowBinary string length"));
        }
        length |= u64::from(byte & 127) << shift;
        if byte & 128 == 0 {
            break;
        }
        shift += 7;
        if shift > 63 {
            return Err(Error::Data("invalid RowBinary string length"));
        }
    }
    let length =
        usize::try_from(length).map_err(|_| Error::Data("invalid RowBinary string length"))?;
    let tail = offset
        .checked_add(length)
        .ok_or(Error::Data("invalid RowBinary string length"))?;
    let end = tail
        .checked_add(25)
        .ok_or(Error::Data("invalid RowBinary string length"))?;
    if end > data.len() {
        return Ok(None);
    }
    let id = std::str::from_utf8(&data[offset..tail])
        .map_err(|_| Error::Data("projector execution identity is not UTF-8"))?;
    let epoch = u32::from_le_bytes(
        data[start..start + 4]
            .try_into()
            .map_err(|_| Error::Data("invalid row"))?,
    );
    let sequence = read_u64(data, start + 4);
    let index = u32::from_le_bytes(
        data[start + 12..start + 16]
            .try_into()
            .map_err(|_| Error::Data("invalid row"))?,
    );
    let timestamp = read_u64(data, start + 16) as i64;
    let price = read_u64(data, tail) as i64;
    let quantity = read_u64(data, tail + 8);
    let buy = data[tail + 16];
    let spec = read_u64(data, tail + 17);
    if epoch == 0 || timestamp <= 0 || price <= 0 || quantity == 0 || spec == 0 {
        return Err(Error::Data(
            "projector execution contains an invalid native integer",
        ));
    }
    if id.is_empty() {
        return Err(Error::Data("projector execution identity is incomplete"));
    }
    if buy > 1 {
        return Err(Error::Data("projector execution side is not boolean"));
    }
    let (price_divisor, quantity_divisor) = *divisors.get(&spec).ok_or(Error::UnknownSpec(spec))?;
    if price_divisor == 0
        || quantity_divisor == 0
        || price as u64 % price_divisor != 0
        || quantity % quantity_divisor != 0
    {
        return Err(Error::Data(
            "execution atoms are not exactly divisible for instrument specification",
        ));
    }
    Ok(Some((
        Execution {
            cursor: Cursor {
                partition,
                epoch,
                sequence,
                batch: index,
                timestamp,
                id: id.into(),
            },
            price: (price as u64 / price_divisor) as i64,
            quantity: quantity / quantity_divisor,
            buy: buy == 1,
            spec,
            segment_start: false,
        },
        end,
    )))
}

fn read_u64(data: &[u8], offset: usize) -> u64 {
    // Called only after the complete fixed-size record tail/header was checked.
    let mut value = [0; 8];
    value.copy_from_slice(&data[offset..offset + 8]);
    u64::from_le_bytes(value)
}
