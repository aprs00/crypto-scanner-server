use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type Order = [u64; 2];

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
    pub levels: BTreeMap<i64, (u128, u128)>,
    pub first_order: Option<Order>,
    pub last_order: Option<Order>,
}

// Checkpoints use sorted [price, buy, sell] arrays, not a JSON object/map.
mod levels_json {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        levels: &BTreeMap<i64, (u128, u128)>,
        s: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        levels
            .iter()
            .map(|(p, (b, q))| (*p, *b, *q))
            .collect::<Vec<_>>()
            .serialize(s)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> std::result::Result<BTreeMap<i64, (u128, u128)>, D::Error> {
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
            levels: BTreeMap::new(),
            first_order: None,
            last_order: None,
        }
    }

    fn add(&mut self, order: Order, price: i64, quantity: u64, buy: bool) -> Result<()> {
        let quantity = u128::from(quantity);
        let values = self.levels.entry(price).or_insert((0, 0));
        if buy {
            values.0 = values.0.checked_add(quantity).ok_or(Error::Overflow)?;
            self.buy_units = self
                .buy_units
                .checked_add(quantity)
                .ok_or(Error::Overflow)?;
        } else {
            values.1 = values.1.checked_add(quantity).ok_or(Error::Overflow)?;
            self.sell_units = self
                .sell_units
                .checked_add(quantity)
                .ok_or(Error::Overflow)?;
        }
        self.trade_count = self.trade_count.checked_add(1).ok_or(Error::Overflow)?;
        self.last_tick = price;
        self.high_tick = self.high_tick.max(price);
        self.low_tick = self.low_tick.min(price);
        self.first_order.get_or_insert(order);
        self.last_order = Some(order);
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
    pub levels: BTreeMap<i64, (u128, u128)>,
}

#[derive(Clone)]
pub struct Engine {
    pub state: State,
    continuation: i64,
    reversal: i64,
    max_expansion: i64,
    max_levels: usize,
}

impl Engine {
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
            levels: active.levels,
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
        let rank = match self.state.last_order {
            Some(order) => order[1].checked_add(1).ok_or(Error::Overflow)?,
            None => 0,
        };
        let order = [timestamp, rank];
        if self.state.spec_id.is_some_and(|old| old != spec) {
            self.truncate(out)?;
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
                return Ok(());
            }
        }
        let active = self
            .state
            .active
            .as_mut()
            .ok_or(Error::Data("missing active bar"))?;
        active.add(order, price, quantity, buy)?;
        if active.levels.len() > self.max_levels {
            return Err(Error::Density);
        }
        self.state.spec_id = Some(spec);
        self.state.last_order = Some(order);
        Ok(())
    }
}
