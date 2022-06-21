extern crate pyo3;
pub mod fuzzdex;
use std::collections::HashSet;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::exceptions;

use unicode_segmentation::UnicodeSegmentation;

type FastHash = ahash::RandomState;

#[pyclass]
pub struct FuzzDex {
    /* Will become None after creation of IndexReady */
    index: Option<fuzzdex::Index>,
    index_ready: Option<fuzzdex::IndexReady>,
}

// Python wrapper for fuzzdex proper.
#[pymethods]
impl FuzzDex {
    #[new]
    fn new() -> PyResult<Self> {
        let fuzzdex = FuzzDex {
            index: Some(fuzzdex::Index::new()),
            index_ready: None,
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

        if let Some(index) = &mut self.index {
            index.add_phrase(phrase, phrase_idx, constraints);
            Ok(())
        } else {
            Err(PyErr::new::<exceptions::PyRuntimeError, _>("Index is already finished."))
        }
    }

    fn finish(&mut self) -> PyResult<()> {
        if let Some(index) = self.index.take() {
            self.index_ready = Some(index.finish());
            Ok(())
        } else {
            Err(PyErr::new::<exceptions::PyRuntimeError, _>("Index is already finished."))
        }
    }

    fn search<'py>(&mut self, py: Python<'py>,
                  must: &str, should: Vec<&str>,
                  constraint: Option<usize>, limit: Option<usize>,
                  max_distance: Option<usize>) -> PyResult<PyObject> {
        match &mut self.index_ready {
            None => {
                Err(PyErr::new::<exceptions::PyRuntimeError, _>("Index is not yet finished."))
            },
            Some(index) => {
                let query = fuzzdex::Query::new(must, &should)
                    .constraint(constraint)
                    .max_distance(max_distance)
                    .limit(limit);

                let search_results = index.search(&query);
                /*
                // TODO: Use allow_threads, but protect access because of cache.
                // Otherwise RuntimeError: AlreadyBorrowed happens (see python tests)
                let search_results = py.allow_threads(
                    move || {
                        let mut index = index.lock().unwrap();
                        index.search(&query)
                    });
                */
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

/* Helper to calculate levenhstein distance from Python without additional libs */
#[pyfunction]
fn distance(side_a: &str, side_b: &str) -> PyResult<usize> {
    let graphemes_a = side_a.graphemes(true).collect::<Vec<&str>>();
    let graphemes_b = side_b.graphemes(true).collect::<Vec<&str>>();
    let (distance, _) = levenshtein_diff::distance(&graphemes_a, &graphemes_b);
    Ok(distance)
}

#[pymodule]
fn fuzzdex(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add("__doc__", "FUZZy inDEX in Rust")?;
    m.add_class::<FuzzDex>()?;
    m.add_function(wrap_pyfunction!(distance, m)?)?;
    Ok(())
}
