extern crate pyo3;
pub mod utils;
pub mod fuzzdex;

use std::collections::HashSet;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::exceptions;

use crate::fuzzdex::{seeker, query};

type FastHash = ahash::RandomState;

#[pyclass(name="FuzzDex")]
pub struct PyFuzzDex {
    /* Will become None after creation of Index */
    indexer: Option<fuzzdex::Indexer>,
    index: Option<seeker::Index>,
}

/// Python wrapper for fuzzdex proper.
#[pymethods]
impl PyFuzzDex {
    #[new]
    fn new() -> PyResult<Self> {
        let fuzzdex = PyFuzzDex {
            indexer: Some(fuzzdex::Indexer::new()),
            index: None,
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

        if let Some(indexer) = &mut self.indexer {
            indexer.add_phrase(phrase, phrase_idx, constraints);
            Ok(())
        } else {
            Err(PyErr::new::<exceptions::PyRuntimeError, _>("Index is already finished."))
        }
    }

    fn finish(&mut self) -> PyResult<()> {
        if let Some(indexer) = self.indexer.take() {
            self.index = Some(indexer.finish());
            Ok(())
        } else {
            Err(PyErr::new::<exceptions::PyRuntimeError, _>("Index is already finished."))
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn search<'py>(&self, py: Python<'py>,
                   must: &str, should: Vec<&str>,
                   constraint: Option<usize>, limit: Option<usize>,
                   max_distance: Option<usize>,
                   scan_cutoff: Option<f32>) -> PyResult<PyObject> {
        match &self.index {
            None => {
                Err(PyErr::new::<exceptions::PyRuntimeError, _>("Index is not yet finished."))
            },
            Some(index) => {
                let query = query::Query::new(must, &should)
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
fn fuzzdex(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add("__doc__", "FUZZy inDEX in Rust")?;
    m.add_class::<PyFuzzDex>()?;
    m.add_function(wrap_pyfunction!(distance, m)?)?;
    m.add_function(wrap_pyfunction!(trigramize, m)?)?;
    m.add_function(wrap_pyfunction!(tokenize, m)?)?;
    Ok(())
}
