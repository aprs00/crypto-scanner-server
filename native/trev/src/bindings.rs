use crate::{
    engine::{Bar, Engine, State},
    stream::{Batch, Cursor, Decoder},
    Error,
};
use pyo3::{
    create_exception,
    exceptions::{PyOverflowError, PyValueError},
    prelude::*,
    types::{PyBytes, PyTuple},
};

create_exception!(_trev_native, DataError, PyValueError);
create_exception!(_trev_native, UnknownSpec, PyValueError);

fn error(error: Error) -> PyErr {
    match error {
        Error::Overflow => PyOverflowError::new_err("native TREV integer range exceeded"),
        Error::Expansion => PyValueError::new_err("TREV_EXPANSION_LIMIT"),
        Error::Density => PyValueError::new_err("CHART_BAR_TOO_DENSE"),
        Error::Data(message) => DataError::new_err(message),
        Error::UnknownSpec(id) => UnknownSpec::new_err(id),
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
    /// Only Tick and exceptional integer fallback materialize Python trades.
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
                value!(bar.levels.keys().copied().collect::<Vec<_>>());
                value!(bar.levels.values().map(|(b, _)| *b).collect::<Vec<_>>());
                value!(bar.levels.values().map(|(_, s)| *s).collect::<Vec<_>>());
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
    #[pyo3(signature = (continuation, reversal, max_expansion, max_levels, anchor, snapshot, cursor=None))]
    fn new(
        continuation: i64,
        reversal: i64,
        max_expansion: i64,
        max_levels: usize,
        anchor: i64,
        snapshot: &str,
        cursor: Option<&str>,
    ) -> PyResult<Self> {
        let mut engine =
            Engine::new(continuation, reversal, max_expansion, max_levels).map_err(error)?;
        // The Python adapter validates with TrevEngine.restore before crossing FFI.
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
    m.add("DataError", m.py().get_type::<DataError>())?;
    m.add("UnknownSpec", m.py().get_type::<UnknownSpec>())?;
    Ok(())
}
