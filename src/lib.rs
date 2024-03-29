extern crate pyo3;
pub mod utils;
pub mod fuzzdex;

use std::collections::HashSet;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::exceptions::PyRuntimeError;

use crate::fuzzdex::{seeker, query};

type FastHash = ahash::RandomState;

pub enum FuzzDex {
    /// While being build.
    Indexer(fuzzdex::Indexer),
    /// When finished and queryable.
    Index(seeker::Index)
}

#[pyclass(name="FuzzDex")]
pub struct PyFuzzDex {
    index: FuzzDex,
}

impl PyFuzzDex {
    /// Helper to easily get index or create an exception.
    fn get_index(&self) -> Result<&seeker::Index, PyErr> {
        match &self.index {
            FuzzDex::Indexer(_) =>
                Err(PyErr::new::<PyRuntimeError, _>("Index is not yet finished.")),
            FuzzDex::Index(index) =>
                Ok(index)
        }
    }
}

/// Python wrapper for fuzzdex proper.
#[pymethods]
impl PyFuzzDex {
    #[new]
    fn new() -> PyResult<Self> {
        let fuzzdex = PyFuzzDex {
            index: FuzzDex::Indexer(fuzzdex::Indexer::new())
        };
        Ok(fuzzdex)
    }

    fn add_phrase(&mut self, phrase: &str, phrase_idx: usize,
                  constraints: HashSet<usize, FastHash>) -> PyResult<()> {
        let constraints: Option<&HashSet<usize, FastHash>> = if constraints.is_empty() {
            None
        } else {
            Some(&constraints)
        };

        match &mut self.index {
            FuzzDex::Indexer(indexer) => {
                indexer.add_phrase(phrase, phrase_idx, constraints)
                    .map_err(|_|
                             PyErr::new::<PyRuntimeError, _>("Duplicate phrase index."))
            }
            FuzzDex::Index(_) => {
                Err(PyErr::new::<PyRuntimeError, _>("Index is already finished."))
            }
        }
    }

    /// Finish indexing and move into searchable index with a given internal cache size.
    fn finish(&mut self, cache_size: Option<usize>) -> PyResult<()> {
        let cache_size = cache_size.unwrap_or(2000);
        if cache_size == 0 {
            return Err(PyErr::new::<PyRuntimeError, _>("Cache size must be at least 1"))
        }
        match &mut self.index {
            FuzzDex::Indexer(indexer) => {
                let indexer = std::mem::take(indexer);
                self.index = FuzzDex::Index(indexer.finish_with_cache(cache_size));
                Ok(())
            }
            FuzzDex::Index(_) => {
                Err(PyErr::new::<PyRuntimeError, _>("Index is already finished."))
            }
        }
    }

    /// Query index using given criterions.
    fn cache_stats(&self, py: Python) -> PyResult<PyObject> {
        let index = self.get_index()?;
        let stats = index.cache_stats();
        let pystats = PyDict::new(py);
        pystats.set_item("hits", stats.hits)?;
        pystats.set_item("misses", stats.misses)?;
        pystats.set_item("inserts", stats.inserts)?;
        pystats.set_item("size", stats.size)?;
        Ok(pystats.into())
    }

    /// Query index using given criterions.
    #[allow(clippy::too_many_arguments)]
    fn search<'py>(&self, py: Python<'py>,
                   must: Vec<&str>, should: Vec<&str>,
                   constraint: Option<usize>, limit: Option<usize>,
                   max_distance: Option<usize>,
                   scan_cutoff: Option<f32>) -> PyResult<PyObject> {
        let index = self.get_index()?;
        if must.len() != 1 {
            return Err(PyErr::new::<PyRuntimeError, _>("Exactly one `must token` is supported."));
        }
        let query = query::Query::new(&must, &should)
            .constraint(constraint)
            .max_distance(max_distance)
            .limit(limit)
            .scan_cutoff(scan_cutoff.unwrap_or(0.3));

        let search_results = py.allow_threads(
            move || {
                index.search(&query)
            });
        let pyresults = search_results.iter()
            .map(|result| {
                let pyresult = PyDict::new(py);
                pyresult.set_item("origin", result.origin).unwrap();
                pyresult.set_item("index", result.index).unwrap();
                pyresult.set_item("token", result.token).unwrap();
                pyresult.set_item("distance", result.distance).unwrap();
                pyresult.set_item("score", result.score).unwrap();
                pyresult.set_item("should_score", result.should_score).unwrap();
                pyresult
            });

        let list = PyList::new(py, pyresults);
        Ok(list.into())
    }
}

/// Helper to calculate levenshtein distance from Python without additional libs.
#[pyfunction]
fn distance(side_a: &str, side_b: &str) -> PyResult<usize> {
    Ok(utils::distance(side_a, side_b))
}

/// Python access to internal trigramizer.
#[pyfunction]
fn trigramize(token: &str) -> PyResult<Vec<String>> {
    Ok(utils::trigramize(token))
}

/// Python access to the internal tokenizer.
#[pyfunction]
fn tokenize(phrase: &str, min_length: Option<usize>) -> PyResult<Vec<String>> {
    let min_length = min_length.unwrap_or(2);
    Ok(utils::tokenize(phrase, min_length))
}

#[pymodule]
#[pyo3(name="fuzzdex")]
fn pyfuzzdex(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add("__doc__", "FUZZy inDEX in Rust")?;
    m.add_class::<PyFuzzDex>()?;
    m.add_function(wrap_pyfunction!(distance, m)?)?;
    m.add_function(wrap_pyfunction!(trigramize, m)?)?;
    m.add_function(wrap_pyfunction!(tokenize, m)?)?;
    Ok(())
}
