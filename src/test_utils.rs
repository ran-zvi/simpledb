use std::panic;

pub fn run_test<T>(
    test: T,
    setup: Option<Box<dyn FnOnce() -> ()>>,
    teardown: Option<Box<dyn FnOnce() -> ()>>,
) -> ()
where
    T: FnOnce() -> () + panic::UnwindSafe,
{
    match setup {
        Some(func) => func(),
        None => (),
    };

    let result = panic::catch_unwind(|| test());

    match teardown {
        Some(func) => func(),
        None => (),
    };

    assert!(result.is_ok());
}
