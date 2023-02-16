use pyo3::{
    prelude::{pyfunction, pymodule},
    types::PyModule,
    wrap_pyfunction, PyResult, Python,
};

#[pyfunction]
fn tick_upper(price: i32, is_kosdaq: bool) -> i32 {
    todo!()
}

#[pymodule]
fn util(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(tick_upper))?;
    Ok(())
}
