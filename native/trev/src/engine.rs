use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

// Keys are normalized integer prices. Multiplication distributes neighboring
// prices over the table and its fingerprint bits without hashing byte strings.
#[derive(Default, Clone, Debug)]
pub struct PriceHasher(u64);

impl Hasher for PriceHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = (self.0.rotate_left(5) ^ u64::from(*byte)).wrapping_mul(0x9e3779b97f4a7c15);
        }
    }
    fn write_i64(&mut self, price: i64) {
        self.0 = (price as u64).wrapping_mul(0x9e3779b97f4a7c15);
    }
}

type Levels = HashMap<i64, (u128, u128), BuildHasherDefault<PriceHasher>>;

pub type Order = [u64; 2];

#[derive(Default)]
pub struct Tail {
    pub first: u64,
    pub last: u64,
    pub count: u64,
    pub buy: u128,
    pub sell: u128,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Active {
    pub open_tick: i64,
    pub last_tick: i64,
    pub high_tick: i64,
    pub low_tick: i64,
    pub break_before: bool,
    pub buy_units: u128,
    pub sell_units: u128,
    pub trade_count: u64,
    #[serde(with = "levels_json")]
    pub levels: Levels,
    pub first_order: Option<Order>,
    pub last_order: Option<Order>,
}

// Checkpoints use sorted [price, buy, sell] arrays, not a JSON object/map.
mod levels_json {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        levels: &Levels,
        s: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut ordered = levels
            .iter()
            .map(|(p, (b, q))| (*p, *b, *q))
            .collect::<Vec<_>>();
        ordered.sort_unstable_by_key(|level| level.0);
        ordered.serialize(s)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> std::result::Result<Levels, D::Error> {
        Ok(Vec::<(i64, u128, u128)>::deserialize(d)?
            .into_iter()
            .map(|(p, b, s)| (p, (b, s)))
            .collect())
    }
}

impl Active {
    fn empty(price: i64, gap: bool) -> Self {
        Self {
            open_tick: price,
            last_tick: price,
            high_tick: price,
            low_tick: price,
            break_before: gap,
            buy_units: 0,
            sell_units: 0,
            trade_count: 0,
            levels: Levels::default(),
            first_order: None,
            last_order: None,
        }
    }

    fn add(&mut self, order: Order, price: i64, quantity: u64, buy: bool) -> Result<()> {
        self.add_run(order, price, quantity, buy, &Tail::default())
    }

    fn add_run(
        &mut self,
        order: Order,
        price: i64,
        quantity: u64,
        buy: bool,
        tail: &Tail,
    ) -> Result<()> {
        let buy_units = tail
            .buy
            .checked_add(if buy { u128::from(quantity) } else { 0 })
            .ok_or(Error::Overflow)?;
        let sell_units = tail
            .sell
            .checked_add(if buy { 0 } else { u128::from(quantity) })
            .ok_or(Error::Overflow)?;
        let values = self.levels.entry(price).or_insert((0, 0));
        values.0 = values.0.checked_add(buy_units).ok_or(Error::Overflow)?;
        values.1 = values.1.checked_add(sell_units).ok_or(Error::Overflow)?;
        self.buy_units = self
            .buy_units
            .checked_add(buy_units)
            .ok_or(Error::Overflow)?;
        self.sell_units = self
            .sell_units
            .checked_add(sell_units)
            .ok_or(Error::Overflow)?;
        self.trade_count = self
            .trade_count
            .checked_add(1)
            .and_then(|v| v.checked_add(tail.count))
            .ok_or(Error::Overflow)?;
        self.last_tick = price;
        self.high_tick = self.high_tick.max(price);
        self.low_tick = self.low_tick.min(price);
        self.first_order.get_or_insert(order);
        self.last_order = Some(if tail.count == 0 {
            order
        } else {
            [
                tail.last,
                order[1].checked_add(tail.count).ok_or(Error::Overflow)?,
            ]
        });
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Unset,
    Up,
    Down,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct State {
    pub direction: Direction,
    pub next_bar_index: u64,
    pub break_before_next: bool,
    pub spec_id: Option<u64>,
    pub last_order: Option<Order>,
    pub active: Option<Active>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            direction: Direction::Unset,
            next_bar_index: 0,
            break_before_next: false,
            spec_id: None,
            last_order: None,
            active: None,
        }
    }
}

#[derive(Debug)]
pub struct Bar {
    pub index: u64,
    pub spec: u64,
    pub gap: bool,
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
    pub buy: u128,
    pub sell: u128,
    pub count: u64,
    pub opened: u64,
    pub ended: u64,
    pub levels: Vec<(i64, (u128, u128))>,
}

#[derive(Clone)]
pub struct Engine {
    pub state: State,
    continuation: i64,
    reversal: i64,
    max_expansion: i64,
    max_levels: usize,
    pub tick_count: Option<u64>,
}

impl Engine {
    /// Add executions after the first trade of an unchanged-price run. The
    /// first trade is always pushed normally because it can close a boundary.
    pub fn push_tail(
        &mut self,
        price: i64,
        first: u64,
        last: u64,
        count: u64,
        buy: u128,
        sell: u128,
    ) -> Result<()> {
        if count == 0 {
            return Ok(());
        }
        let previous = self
            .state
            .last_order
            .ok_or(Error::Data("missing run origin"))?;
        let first_order = [first, previous[1].checked_add(1).ok_or(Error::Overflow)?];
        let last_order = [last, previous[1].checked_add(count).ok_or(Error::Overflow)?];
        let active = self
            .state
            .active
            .as_mut()
            .ok_or(Error::Data("missing run active bar"))?;
        let level = active.levels.entry(price).or_insert((0, 0));
        level.0 = level.0.checked_add(buy).ok_or(Error::Overflow)?;
        level.1 = level.1.checked_add(sell).ok_or(Error::Overflow)?;
        active.buy_units = active.buy_units.checked_add(buy).ok_or(Error::Overflow)?;
        active.sell_units = active.sell_units.checked_add(sell).ok_or(Error::Overflow)?;
        active.trade_count = active
            .trade_count
            .checked_add(count)
            .ok_or(Error::Overflow)?;
        active.first_order.get_or_insert(first_order);
        active.last_order = Some(last_order);
        active.last_tick = price;
        active.high_tick = active.high_tick.max(price);
        active.low_tick = active.low_tick.min(price);
        if active.levels.len() > self.max_levels {
            return Err(Error::Density);
        }
        self.state.last_order = Some(last_order);
        Ok(())
    }

    pub fn new(
        continuation: i64,
        reversal: i64,
        max_expansion: i64,
        max_levels: usize,
    ) -> Result<Self> {
        if continuation <= 0
            || reversal <= 0
            || !(1..=500).contains(&max_expansion)
            || max_levels == 0
        {
            return Err(Error::Data("invalid TREV configuration"));
        }
        Ok(Self {
            state: State::default(),
            continuation,
            reversal,
            max_expansion,
            max_levels,
            tick_count: None,
        })
    }

    fn close(&mut self, price: i64, trigger: Option<Order>, out: &mut Vec<Bar>) -> Result<()> {
        let active = self
            .state
            .active
            .take()
            .ok_or(Error::Data("missing active bar"))?;
        if active.levels.len() > self.max_levels {
            return Err(Error::Density);
        }
        let opened = active
            .first_order
            .or(trigger)
            .or(active.last_order)
            .ok_or(Error::Data("TREV bar has no event-time anchor"))?[0];
        let ended = trigger
            .or(active.last_order)
            .or(active.first_order)
            .ok_or(Error::Data("TREV bar has no event-time anchor"))?[0];
        let index = self.state.next_bar_index;
        self.state.next_bar_index = index.checked_add(1).ok_or(Error::Overflow)?;
        // Updates use hash lookup; closed bars and checkpoints retain their
        // canonical ascending price order. Flat closed levels also avoid tree
        // allocation and traversal in the columnar output serializer.
        let mut levels: Vec<_> = active.levels.into_iter().collect();
        levels.sort_unstable_by_key(|level| level.0);
        out.push(Bar {
            index,
            spec: self
                .state
                .spec_id
                .ok_or(Error::Data("missing specification"))?,
            gap: active.break_before,
            open: active.open_tick,
            high: active.high_tick.max(active.open_tick).max(price),
            low: active.low_tick.min(active.open_tick).min(price),
            close: price,
            buy: active.buy_units,
            sell: active.sell_units,
            count: active.trade_count,
            opened,
            ended,
            levels,
        });
        Ok(())
    }

    pub fn truncate(&mut self, out: &mut Vec<Bar>) -> Result<()> {
        if let Some(active) = &self.state.active {
            if active.trade_count > 0 {
                self.close(active.last_tick, None, out)?;
            }
        }
        self.state.active = None;
        self.state.spec_id = None;
        self.state.direction = Direction::Unset;
        self.state.break_before_next = true;
        Ok(())
    }

    pub fn push(
        &mut self,
        timestamp: u64,
        price: i64,
        quantity: u64,
        buy: bool,
        spec: u64,
        out: &mut Vec<Bar>,
    ) -> Result<()> {
        self.push_run(timestamp, price, quantity, buy, spec, out, &Tail::default())
    }

    pub fn push_run(
        &mut self,
        timestamp: u64,
        price: i64,
        quantity: u64,
        buy: bool,
        spec: u64,
        out: &mut Vec<Bar>,
        tail: &Tail,
    ) -> Result<()> {
        let rank = match self.state.last_order {
            Some(order) => order[1].checked_add(1).ok_or(Error::Overflow)?,
            None => 0,
        };
        let order = [timestamp, rank];
        if self.state.spec_id.is_some_and(|old| old != spec) {
            self.truncate(out)?;
        }
        if let Some(count) = self.tick_count {
            if tail.count != 0 {
                return Err(Error::Data("Tick requires individual executions"));
            }
            let active = self
                .state
                .active
                .get_or_insert_with(|| Active::empty(price, self.state.break_before_next));
            self.state.break_before_next = false;
            active.add(order, price, quantity, buy)?;
            self.state.spec_id = Some(spec);
            self.state.last_order = Some(order);
            if active.trade_count == count {
                self.close(price, None, out)?;
            }
            return Ok(());
        }
        if self.state.active.is_none() {
            self.state.active = Some(Active::empty(price, self.state.break_before_next));
            self.state.break_before_next = false;
        } else {
            let active = self
                .state
                .active
                .as_ref()
                .ok_or(Error::Data("missing active bar"))?;
            let direction = match self.state.direction {
                Direction::Unset if price > active.open_tick => Direction::Up,
                Direction::Unset if price < active.open_tick => Direction::Down,
                _ => self.state.direction.clone(),
            };
            // Positive i64 prices imply this distance cannot overflow. Thresholds
            // are computed wide so even very large user configurations are exact.
            let distance = i128::from(price) - i128::from(active.open_tick);
            let c = i128::from(self.continuation);
            let r = i128::from(self.reversal);
            let (count, first, step, next) = match direction {
                Direction::Up if distance >= c => (distance / c, c, c, Direction::Up),
                Direction::Up if distance <= -r => {
                    (1 + (-distance - r) / c, -r, -c, Direction::Down)
                }
                Direction::Down if distance <= -c => (-distance / c, -c, -c, Direction::Down),
                Direction::Down if distance >= r => (1 + (distance - r) / c, r, c, Direction::Up),
                _ => (0, 0, 0, direction.clone()),
            };
            if count > i128::from(self.max_expansion) {
                return Err(Error::Expansion);
            }
            self.state.direction = direction;
            let open = i128::from(active.open_tick);
            let mut final_price = None;
            for index in 0..count {
                let close =
                    i64::try_from(open + first + index * step).map_err(|_| Error::Overflow)?;
                if index + 1 == count && close == price {
                    self.state
                        .active
                        .as_mut()
                        .ok_or(Error::Data("missing active bar"))?
                        .add(order, price, quantity, buy)?;
                }
                self.close(close, Some(order), out)?;
                self.state.active = Some(Active::empty(close, false));
                self.state.direction = next.clone();
                final_price = Some(close);
            }
            if final_price == Some(price) {
                self.state.spec_id = Some(spec);
                self.state.last_order = Some(order);
                return self.push_tail(
                    price, tail.first, tail.last, tail.count, tail.buy, tail.sell,
                );
            }
        }
        let active = self
            .state
            .active
            .as_mut()
            .ok_or(Error::Data("missing active bar"))?;
        active.add_run(order, price, quantity, buy, tail)?;
        if active.levels.len() > self.max_levels {
            return Err(Error::Density);
        }
        self.state.spec_id = Some(spec);
        self.state.last_order = active.last_order;
        Ok(())
    }
}
