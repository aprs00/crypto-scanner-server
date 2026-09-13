use crate::{
    engine::{Bar, Engine, State},
    prepared::{self, Prepared},
    stream::{Batch, Cursor, Decoder},
    Error,
};
use pyo3::{
    create_exception,
    exceptions::{PyOverflowError, PyValueError},
    prelude::*,
    types::{PyBytes, PyTuple},
};
use std::{fs::File, ops::Range, sync::Arc};

create_exception!(_trev_native, DataError, PyValueError);
create_exception!(_trev_native, UnknownSpec, PyValueError);
create_exception!(_trev_native, PreparedFallback, PyValueError);
create_exception!(_trev_native, PreparedCursorRequired, PyValueError);

fn error(error: Error) -> PyErr {
    match error {
        Error::Overflow => PyOverflowError::new_err("native TREV integer range exceeded"),
        Error::Expansion => PyValueError::new_err("TREV_EXPANSION_LIMIT"),
        Error::Density => PyValueError::new_err("CHART_BAR_TOO_DENSE"),
        Error::Data(message) => DataError::new_err(message),
        Error::UnknownSpec(id) => UnknownSpec::new_err(id),
        Error::PreparedCursors => PreparedCursorRequired::new_err("prepared cursor data required"),
    }
}

fn cursor_json(value: Option<&str>) -> PyResult<Option<Cursor>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let values: Vec<serde_json::Value> =
        serde_json::from_str(value).map_err(|_| PyValueError::new_err("invalid replay cursor"))?;
    let base = match values.len() {
        5 => 0,
        8 if values[0].as_i64() == Some(-2) => 3,
        _ => return Err(PyValueError::new_err("invalid replay cursor")),
    };
    let unsigned = |i: usize| {
        values[i]
            .as_u64()
            .ok_or_else(|| PyOverflowError::new_err("invalid cursor integer"))
    };
    let partition = if base == 3 {
        let kind = unsigned(2)?;
        if kind > 1 {
            return Err(PyValueError::new_err("invalid replay cursor kind"));
        }
        Some((unsigned(1)?, kind as u8))
    } else {
        None
    };
    Ok(Some(Cursor {
        partition,
        epoch: u32::try_from(unsigned(base)?)
            .map_err(|_| PyOverflowError::new_err("cursor epoch overflow"))?,
        sequence: unsigned(base + 1)?,
        batch: u32::try_from(unsigned(base + 2)?)
            .map_err(|_| PyOverflowError::new_err("cursor batch overflow"))?,
        timestamp: values[base + 3]
            .as_i64()
            .ok_or_else(|| PyOverflowError::new_err("cursor timestamp overflow"))?,
        id: values[base + 4]
            .as_str()
            .ok_or_else(|| PyValueError::new_err("invalid cursor identity"))?
            .into(),
    }))
}

fn cursor_object(py: Python<'_>, cursor: Option<&Cursor>) -> PyResult<PyObject> {
    let Some(cursor) = cursor else {
        return Ok(py.None());
    };
    let mut values = Vec::<PyObject>::new();
    if let Some((day, kind)) = cursor.partition {
        values.extend([
            (-2i64).into_pyobject(py)?.into_any().unbind(),
            day.into_pyobject(py)?.into_any().unbind(),
            kind.into_pyobject(py)?.into_any().unbind(),
        ]);
    }
    values.extend([
        cursor.epoch.into_pyobject(py)?.into_any().unbind(),
        cursor.sequence.into_pyobject(py)?.into_any().unbind(),
        cursor.batch.into_pyobject(py)?.into_any().unbind(),
        cursor.timestamp.into_pyobject(py)?.into_any().unbind(),
        cursor
            .id
            .as_str()
            .map_err(error)?
            .into_pyobject(py)?
            .into_any()
            .unbind(),
    ]);
    Ok(PyTuple::new(py, values)?.into_any().unbind())
}

#[pyclass(name = "ExecutionBatch", frozen)]
struct PyBatch {
    inner: Batch,
}

#[pymethods]
impl PyBatch {
    fn __len__(&self) -> usize {
        self.inner.rows.len()
    }
    #[staticmethod]
    fn combine(batches: Vec<PyRef<'_, PyBatch>>) -> PyResult<Self> {
        let mut inner = Batch {
            rows: Vec::with_capacity(batches.iter().map(|b| b.inner.rows.len()).sum()),
            previous: batches.first().and_then(|b| b.inner.previous.clone()),
        };
        for batch in batches {
            if let Some(last) = inner.rows.last() {
                if Some(&last.cursor) != batch.inner.previous.as_ref() {
                    return Err(error(Error::Data("noncontiguous native fallback batches")));
                }
            }
            inner.rows.extend(batch.inner.rows.iter().cloned());
        }
        Ok(Self { inner })
    }
    fn run_count(&self) -> usize {
        self.inner
            .rows
            .iter()
            .enumerate()
            .filter(|(i, row)| {
                *i == 0 || row.segment_start || {
                    let previous = &self.inner.rows[*i - 1];
                    (row.price, row.spec) != (previous.price, previous.spec)
                }
            })
            .count()
    }
    fn timestamp_bounds(&self) -> Option<(i64, i64)> {
        self.inner
            .rows
            .iter()
            .map(|r| r.cursor.timestamp)
            .fold(None, |bounds, value| {
                Some(bounds.map_or((value, value), |(low, high): (i64, i64)| {
                    (low.min(value), high.max(value))
                }))
            })
    }
    fn prepared_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = py
            .allow_threads(|| {
                prepared::encode(&self.inner, |cursor| {
                    let Some(cursor) = cursor else {
                        return Ok(b"null".to_vec());
                    };
                    let mut values = Vec::new();
                    if let Some((day, kind)) = cursor.partition {
                        values.extend([
                            serde_json::json!(-2),
                            serde_json::json!(day),
                            serde_json::json!(kind),
                        ]);
                    }
                    values.extend([
                        serde_json::json!(cursor.epoch),
                        serde_json::json!(cursor.sequence),
                        serde_json::json!(cursor.batch),
                        serde_json::json!(cursor.timestamp),
                        serde_json::json!(cursor.id.as_str()?),
                    ]);
                    serde_json::to_vec(&values).map_err(|_| Error::Data("invalid prepared cursor"))
                })
            })
            .map_err(error)?;
        Ok(PyBytes::new(py, &bytes))
    }
    #[getter]
    fn first_cursor(&self, py: Python<'_>) -> PyResult<PyObject> {
        cursor_object(py, self.inner.rows.first().map(|r| &r.cursor))
    }
    #[getter]
    fn last_cursor(&self, py: Python<'_>) -> PyResult<PyObject> {
        cursor_object(py, self.inner.rows.last().map(|r| &r.cursor))
    }
    #[getter]
    fn previous_cursor(&self, py: Python<'_>) -> PyResult<PyObject> {
        cursor_object(py, self.inner.previous.as_ref())
    }
    /// Only exceptional integer fallback materializes Python trades.
    fn executions(&self, py: Python<'_>, start: usize) -> PyResult<Vec<PyObject>> {
        if start > self.inner.rows.len() {
            return Err(PyValueError::new_err("invalid batch offset"));
        }
        self.inner.rows[start..]
            .iter()
            .map(|row| {
                let c = &row.cursor;
                let replay = if c.partition.is_some() {
                    cursor_object(py, Some(c))?
                } else {
                    py.None()
                };
                Ok((
                    c.epoch,
                    c.sequence,
                    c.batch,
                    c.timestamp,
                    c.id.as_str().map_err(error)?,
                    row.price,
                    row.quantity,
                    row.buy,
                    row.spec,
                    replay,
                    row.segment_start,
                )
                    .into_pyobject(py)?
                    .into_any()
                    .unbind())
            })
            .collect()
    }
}

#[pyclass(name = "PreparedBatch")]
struct PyPrepared {
    inner: Prepared,
    data: PreparedData,
    cursors: Option<Py<PyBytes>>,
}

enum PreparedData {
    Bytes(Py<PyBytes>),
    Mapped(Arc<memmap2::Mmap>, Range<usize>),
}
impl PreparedData {
    fn bytes<'a>(&'a self, py: Python<'a>) -> &'a [u8] {
        match self {
            Self::Bytes(data) => data.bind(py).as_bytes(),
            Self::Mapped(data, range) => &data[range.clone()],
        }
    }
}

#[pyclass(name = "PreparedTape", frozen)]
struct PyTape {
    data: Arc<memmap2::Mmap>,
}
#[pymethods]
impl PyTape {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let file = File::open(path)?;
        // Cache files have immutable published prefixes. Writers append to new
        // offsets and never truncate or overwrite a published file.
        let data = unsafe { memmap2::MmapOptions::new().map(&file)? };
        Ok(Self {
            data: Arc::new(data),
        })
    }
    #[pyo3(signature = (offset, length, checksum=None))]
    fn batch(
        &self,
        py: Python<'_>,
        offset: usize,
        length: usize,
        checksum: Option<u64>,
    ) -> PyResult<PyPrepared> {
        let end = offset
            .checked_add(length)
            .ok_or_else(|| error(Error::Overflow))?;
        let data = self
            .data
            .get(offset..end)
            .ok_or_else(|| error(Error::Data("truncated prepared mapping")))?;
        if checksum.is_some_and(|expected| {
            py.allow_threads(|| xxhash_rust::xxh3::xxh3_64(data)) != expected
        }) {
            return Err(error(Error::Data("prepared tape checksum mismatch")));
        }
        let inner = Prepared::new(data, prepared_cursor).map_err(error)?;
        Ok(PyPrepared {
            inner,
            data: PreparedData::Mapped(self.data.clone(), offset..end),
            cursors: None,
        })
    }
}

fn prepared_cursor(value: &str) -> crate::Result<Option<Cursor>> {
    cursor_json(if value == "null" { None } else { Some(value) })
        .map_err(|_| Error::Data("invalid prepared cursor"))
}

#[pymethods]
impl PyPrepared {
    #[new]
    fn new(py: Python<'_>, data: Py<PyBytes>) -> PyResult<Self> {
        let inner = Prepared::new(data.bind(py).as_bytes(), prepared_cursor).map_err(error)?;
        Ok(Self {
            inner,
            data: PreparedData::Bytes(data),
            cursors: None,
        })
    }
    fn __len__(&self) -> usize {
        self.inner.count
    }
    #[getter]
    fn records_end(&self) -> usize {
        self.inner.cursors
    }
    fn compact_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let data = self.data.bytes(py);
        let bytes = py
            .allow_threads(|| self.inner.compact(data))
            .map_err(error)?;
        Ok(PyBytes::new(py, &bytes))
    }
    fn checksum(&self, py: Python<'_>) -> u64 {
        let data = self.data.bytes(py);
        py.allow_threads(|| xxhash_rust::xxh3::xxh3_64(data))
    }
    fn load_cursors(&mut self, py: Python<'_>, data: Py<PyBytes>) -> PyResult<()> {
        if self.cursors.is_some() || self.data.bytes(py).len() != self.inner.cursors {
            return Err(PyValueError::new_err("prepared cursors are already loaded"));
        }
        self.cursors = Some(data);
        Ok(())
    }
    #[getter]
    fn previous_cursor(&self, py: Python<'_>) -> PyResult<PyObject> {
        cursor_object(py, self.inner.previous.as_ref())
    }
    #[getter]
    fn first_cursor(&self, py: Python<'_>) -> PyResult<PyObject> {
        cursor_object(py, self.inner.first.as_ref())
    }
    #[getter]
    fn last_cursor(&self, py: Python<'_>) -> PyResult<PyObject> {
        cursor_object(py, self.inner.last.as_ref())
    }
}

#[pyclass(name = "RowBinaryDecoder")]
struct PyDecoder {
    inner: Decoder,
}

#[pymethods]
impl PyDecoder {
    #[new]
    #[pyo3(signature = (after=None, through=None))]
    fn new(after: Option<&str>, through: Option<&str>) -> PyResult<Self> {
        Ok(Self {
            inner: Decoder::new(cursor_json(after)?, cursor_json(through)?),
        })
    }
    fn set_divisor(&mut self, spec: u64, price: u64, quantity: u64) {
        self.inner.set_divisor(spec, price, quantity);
    }
    fn feed(&mut self, data: &[u8]) {
        self.inner.feed(data);
    }
    #[pyo3(signature = (limit, partition=None))]
    fn take(
        &mut self,
        py: Python<'_>,
        limit: usize,
        partition: Option<(u64, u8)>,
    ) -> PyResult<PyBatch> {
        if partition.is_some_and(|(_, kind)| kind > 1) {
            return Err(PyValueError::new_err("invalid replay partition"));
        }
        py.allow_threads(|| self.inner.take(limit, partition))
            .map(|inner| PyBatch { inner })
            .map_err(error)
    }
    fn finish(&self) -> PyResult<()> {
        self.inner.finish().map_err(error)
    }
    #[getter]
    fn done(&self) -> bool {
        self.inner.done
    }
}

#[pyclass(name = "ClosedBars", frozen)]
struct PyBars {
    bars: Vec<Bar>,
}

#[pymethods]
impl PyBars {
    fn __len__(&self) -> usize {
        self.bars.len()
    }
    #[getter]
    fn level_count(&self) -> usize {
        self.bars.iter().map(|b| b.levels.len()).sum()
    }
    fn timestamps(&self) -> Vec<(u64, u64)> {
        self.bars
            .iter()
            .map(|bar| (bar.opened, bar.ended.max(bar.opened)))
            .collect()
    }
    #[staticmethod]
    fn native_batches<'py>(
        py: Python<'py>,
        batches: Vec<PyRef<'_, PyBars>>,
        series: u64,
        timestamps: Vec<(i64, i64)>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let bars: Vec<&Bar> = batches.iter().flat_map(|batch| batch.bars.iter()).collect();
        let level_count: usize = bars.iter().map(|bar| bar.levels.len()).sum();
        if timestamps.len() != bars.len() {
            return Err(PyValueError::new_err("invalid bar timestamp count"));
        }
        let bytes = py
            .allow_threads(|| {
                let mut output = Vec::with_capacity(700 + bars.len() * 121 + level_count * 24);
                fn varint(out: &mut Vec<u8>, mut value: usize) {
                    while value >= 128 {
                        out.push((value as u8 & 127) | 128);
                        value >>= 7;
                    }
                    out.push(value as u8);
                }
                fn string(out: &mut Vec<u8>, value: &str) {
                    varint(out, value.len());
                    out.extend_from_slice(value.as_bytes());
                }
                let columns = [
                    ("series_id", "UInt64"),
                    ("bar_index", "UInt64"),
                    ("opened_at", "DateTime64(6, 'UTC')"),
                    ("ended_at", "DateTime64(6, 'UTC')"),
                    ("instrument_spec_id", "UInt64"),
                    ("open_tick", "Int64"),
                    ("high_tick", "Int64"),
                    ("low_tick", "Int64"),
                    ("close_tick", "Int64"),
                    ("buy_units", "UInt64"),
                    ("sell_units", "UInt64"),
                    ("trade_count", "UInt64"),
                    ("break_before", "Bool"),
                    ("level_ticks", "Array(Int64)"),
                    ("level_buy_units", "Array(UInt64)"),
                    ("level_sell_units", "Array(UInt64)"),
                ];
                varint(&mut output, columns.len());
                varint(&mut output, bars.len());
                for (column, (name, kind)) in columns.iter().enumerate() {
                    string(&mut output, name);
                    string(&mut output, kind);
                    if column >= 13 {
                        let mut offset = 0u64;
                        for bar in &bars {
                            offset = offset
                                .checked_add(bar.levels.len() as u64)
                                .ok_or(Error::Overflow)?;
                            output.extend_from_slice(&offset.to_le_bytes());
                        }
                        for bar in &bars {
                            for (price, (buy, sell)) in &bar.levels {
                                let value = match column {
                                    13 => *price as u64,
                                    14 => u64::try_from(*buy).map_err(|_| Error::Overflow)?,
                                    _ => u64::try_from(*sell).map_err(|_| Error::Overflow)?,
                                };
                                output.extend_from_slice(&value.to_le_bytes());
                            }
                        }
                        continue;
                    }
                    for (index, bar) in bars.iter().enumerate() {
                        if column == 12 {
                            output.push(u8::from(bar.gap));
                            continue;
                        }
                        let value = match column {
                            0 => series,
                            1 => bar.index,
                            2 => timestamps[index].0 as u64,
                            3 => timestamps[index].1 as u64,
                            4 => bar.spec,
                            5 => bar.open as u64,
                            6 => bar.high as u64,
                            7 => bar.low as u64,
                            8 => bar.close as u64,
                            9 => u64::try_from(bar.buy).map_err(|_| Error::Overflow)?,
                            10 => u64::try_from(bar.sell).map_err(|_| Error::Overflow)?,
                            _ => bar.count,
                        };
                        output.extend_from_slice(&value.to_le_bytes());
                    }
                }
                Ok::<_, Error>(output)
            })
            .map_err(error)?;
        Ok(PyBytes::new(py, &bytes))
    }
    /// CHART_BAR_COLUMNS order, except timestamps remain integer microseconds.
    fn rows(&self, py: Python<'_>, series: u64) -> PyResult<Vec<PyObject>> {
        self.bars
            .iter()
            .map(|bar| {
                let mut values = Vec::<PyObject>::new();
                macro_rules! value {
                    ($value:expr) => {
                        values.push($value.into_pyobject(py)?.into_any().unbind());
                    };
                }
                value!(series);
                value!(bar.index);
                value!(bar.opened);
                value!(bar.ended.max(bar.opened));
                value!(bar.spec);
                value!(bar.open);
                value!(bar.high);
                value!(bar.low);
                value!(bar.close);
                value!(bar.buy);
                value!(bar.sell);
                value!(bar.count);
                values.push(bar.gap.into_pyobject(py)?.to_owned().into_any().unbind());
                value!(bar.levels.iter().map(|(p, _)| *p).collect::<Vec<_>>());
                value!(bar.levels.iter().map(|(_, (b, _))| *b).collect::<Vec<_>>());
                value!(bar.levels.iter().map(|(_, (_, s))| *s).collect::<Vec<_>>());
                Ok(PyTuple::new(py, values)?.into_any().unbind())
            })
            .collect()
    }
    /// Stable exact comparison format used by the standalone 57M benchmark.
    fn canonical_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let mut bytes = Vec::new();
        for bar in &self.bars {
            for value in [
                i128::from(bar.index),
                i128::from(bar.spec),
                i128::from(bar.gap),
                i128::from(bar.open),
                i128::from(bar.high),
                i128::from(bar.low),
                i128::from(bar.close),
                i128::try_from(bar.buy).map_err(|_| error(Error::Overflow))?,
                i128::try_from(bar.sell).map_err(|_| error(Error::Overflow))?,
                i128::from(bar.count),
                i128::from(bar.opened),
                i128::from(bar.ended),
                bar.levels.len() as i128,
            ] {
                bytes.extend_from_slice(
                    &i64::try_from(value)
                        .map_err(|_| error(Error::Overflow))?
                        .to_le_bytes(),
                );
            }
            for (price, (buy, sell)) in &bar.levels {
                bytes.extend_from_slice(&price.to_le_bytes());
                bytes.extend_from_slice(
                    &i64::try_from(*buy)
                        .map_err(|_| error(Error::Overflow))?
                        .to_le_bytes(),
                );
                bytes.extend_from_slice(
                    &i64::try_from(*sell)
                        .map_err(|_| error(Error::Overflow))?
                        .to_le_bytes(),
                );
            }
        }
        Ok(PyBytes::new(py, &bytes))
    }
}

#[pyclass(name = "TrevBuilder")]
struct PyBuilder {
    engine: Engine,
    cursor: Option<Cursor>,
    anchor: i64,
}

#[pymethods]
impl PyBuilder {
    #[new]
    #[pyo3(signature = (continuation, reversal, max_expansion, max_levels, anchor, snapshot, cursor=None, tick=None))]
    fn new(
        continuation: i64,
        reversal: i64,
        max_expansion: i64,
        max_levels: usize,
        anchor: i64,
        snapshot: &str,
        cursor: Option<&str>,
        tick: Option<u64>,
    ) -> PyResult<Self> {
        let mut engine =
            Engine::new(continuation, reversal, max_expansion, max_levels).map_err(error)?;
        if tick.is_some_and(|count| count == 0 || count > 4000) {
            return Err(PyValueError::new_err("invalid Tick configuration"));
        }
        engine.tick_count = tick;
        // The Python adapter validates the engine state before crossing FFI.
        // A valid Python integer outside native capacity selects exact fallback.
        engine.state =
            serde_json::from_str::<State>(snapshot).map_err(|_| error(Error::Overflow))?;
        Ok(Self {
            engine,
            cursor: cursor_json(cursor)?,
            anchor,
        })
    }
    fn snapshot_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.engine.state).map_err(|e| PyValueError::new_err(e.to_string()))
    }
    #[getter]
    fn cursor(&self, py: Python<'_>) -> PyResult<PyObject> {
        cursor_object(py, self.cursor.as_ref())
    }
    #[pyo3(signature = (batch, start, max_bars, seam_break=false))]
    fn process_prepared(
        &mut self,
        py: Python<'_>,
        batch: &PyPrepared,
        start: usize,
        max_bars: usize,
        seam_break: bool,
    ) -> PyResult<(usize, PyBars)> {
        if self.engine.tick_count.is_some() {
            return Err(PreparedFallback::new_err(
                "Tick requires individual executions",
            ));
        }
        let data = batch.data.bytes(py);
        let cursor_data = batch
            .cursors
            .as_ref()
            .map(|value| value.bind(py).as_bytes());
        let batch = &batch.inner;
        if start > batch.count || max_bars == 0 {
            return Err(PyValueError::new_err("invalid batch bounds"));
        }
        if batch.minimum_timestamp < self.anchor {
            return Err(PreparedFallback::new_err(
                "prepared batch crosses the chart anchor",
            ));
        }
        let mut consumed = 0;
        let mut run = 0;
        while run < batch.runs && consumed < start {
            consumed += batch.run(data, run).map_err(error)?.count as usize;
            run += 1;
        }
        if consumed != start {
            return Err(PreparedFallback::new_err("prepared offset is inside a run"));
        }
        let previous = if run == 0 {
            batch.previous.clone()
        } else {
            prepared_cursor(batch.cursor(data, cursor_data, run - 1).map_err(error)?)
                .map_err(error)?
        };
        if previous != self.cursor {
            return Err(PreparedFallback::new_err(
                "prepared batch has a different resume cursor",
            ));
        }
        py.allow_threads(|| {
            let mut engine = self.engine.clone();
            let mut out = Vec::new();
            while run < batch.runs {
                let prepared::Run {
                    price,
                    spec,
                    timestamp,
                    quantity,
                    flags,
                    count,
                    first,
                    last,
                    buy,
                    sell,
                } = batch.run(data, run)?;
                if price <= 0 || spec == 0 || quantity == 0 || count == 0 || flags > 3 {
                    return Err(Error::Data("invalid prepared run"));
                }
                if (flags & 2 != 0 || (run == 0 && seam_break))
                    && engine.state.last_order.is_some()
                    && (run > 0 || self.cursor.is_some())
                {
                    engine.truncate(&mut out)?;
                }
                engine.push_run(
                    timestamp,
                    price,
                    quantity,
                    flags & 1 != 0,
                    spec,
                    &mut out,
                    &crate::engine::Tail {
                        first,
                        last,
                        count: u64::from(count - 1),
                        buy,
                        sell,
                    },
                )?;
                consumed = consumed
                    .checked_add(count as usize)
                    .ok_or(Error::Overflow)?;
                run += 1;
                if out.len() >= max_bars {
                    break;
                }
            }
            if consumed > batch.count || (run == batch.runs && consumed != batch.count) {
                return Err(Error::Data("invalid prepared execution count"));
            }
            let cursor = if run == batch.runs {
                batch.last.clone()
            } else if run > 0 {
                prepared_cursor(batch.cursor(data, cursor_data, run - 1)?)?
            } else {
                previous
            };
            self.engine = engine;
            self.cursor = cursor;
            Ok((consumed, PyBars { bars: out }))
        })
        .map_err(error)
    }
    #[pyo3(signature = (batch, start, max_bars, seam_break=false))]
    fn process(
        &mut self,
        py: Python<'_>,
        batch: &PyBatch,
        start: usize,
        max_bars: usize,
        seam_break: bool,
    ) -> PyResult<(usize, PyBars)> {
        if start > batch.inner.rows.len() || max_bars == 0 {
            return Err(PyValueError::new_err("invalid batch bounds"));
        }
        py.allow_threads(|| {
            // Small active-state copy makes failures atomic at the FFI boundary.
            let mut engine = self.engine.clone();
            let mut previous = self.cursor.as_ref();
            let mut out = Vec::new();
            let mut end = start;
            for (index, row) in batch.inner.rows.iter().enumerate().skip(start) {
                end = index + 1;
                if previous.is_some_and(|cursor| row.cursor <= *cursor) {
                    continue;
                }
                if row.cursor.timestamp >= self.anchor {
                    let broken = if row.cursor.partition.is_some() {
                        row.segment_start || (index == 0 && seam_break)
                    } else {
                        previous.is_some_and(|cursor| cursor.epoch != row.cursor.epoch)
                    };
                    if broken && engine.state.last_order.is_some() && previous.is_some() {
                        engine.truncate(&mut out)?;
                    }
                    engine.push(
                        row.cursor.timestamp as u64,
                        row.price,
                        row.quantity,
                        row.buy,
                        row.spec,
                        &mut out,
                    )?;
                }
                previous = Some(&row.cursor);
                if out.len() >= max_bars {
                    break;
                }
            }
            let cursor = previous.cloned();
            self.engine = engine;
            self.cursor = cursor;
            Ok((end, PyBars { bars: out }))
        })
        .map_err(error)
    }
}

#[pymodule]
fn _trev_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyBatch>()?;
    m.add_class::<PyDecoder>()?;
    m.add_class::<PyBuilder>()?;
    m.add_class::<PyBars>()?;
    m.add_class::<PyPrepared>()?;
    m.add_class::<PyTape>()?;
    m.add("DataError", m.py().get_type::<DataError>())?;
    m.add("UnknownSpec", m.py().get_type::<UnknownSpec>())?;
    m.add("PreparedFallback", m.py().get_type::<PreparedFallback>())?;
    m.add(
        "PreparedCursorRequired",
        m.py().get_type::<PreparedCursorRequired>(),
    )?;
    Ok(())
}
