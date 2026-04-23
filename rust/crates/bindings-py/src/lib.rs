#![allow(unsafe_op_in_unsafe_fn)]

use std::sync::Mutex;

use nexus_state::{CacheEntry, PartialHitPlan, QueryKey, ReuseKey};
use nxradixtree_core::RadixTree;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyclass]
struct PyRustPlanner {
    inner: Mutex<RadixTree>,
}

#[pymethods]
impl PyRustPlanner {
    #[new]
    fn new() -> Self {
        Self {
            inner: Mutex::new(RadixTree::default()),
        }
    }

    fn insert(&self, reuse_key_json: &str, entry_json: &str) -> PyResult<()> {
        let reuse_key: ReuseKey = parse_json(reuse_key_json)?;
        let entry: CacheEntry = parse_json(entry_json)?;
        let mut planner = self
            .inner
            .lock()
            .map_err(|_| PyValueError::new_err("planner mutex poisoned"))?;
        planner.insert(reuse_key, entry);
        Ok(())
    }

    fn lookup(&self, query_json: &str) -> PyResult<Option<String>> {
        let query: QueryKey = parse_json(query_json)?;
        let planner = self
            .inner
            .lock()
            .map_err(|_| PyValueError::new_err("planner mutex poisoned"))?;
        planner
            .lookup(&query)
            .map(|result| serde_json::to_string(&result))
            .transpose()
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }

    fn plan_partial_hit(&self, query_json: &str) -> PyResult<Option<String>> {
        let query: QueryKey = parse_json(query_json)?;
        let planner = self
            .inner
            .lock()
            .map_err(|_| PyValueError::new_err("planner mutex poisoned"))?;
        planner
            .plan_partial_hit(&query)
            .map(|plan: PartialHitPlan| serde_json::to_string(&plan))
            .transpose()
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }
}

#[pymodule]
fn nexuskv_planner_native(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyRustPlanner>()?;
    Ok(())
}

fn parse_json<T>(payload: &str) -> PyResult<T>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_str(payload).map_err(|error| PyValueError::new_err(error.to_string()))
}
