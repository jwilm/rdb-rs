macro_rules! unwrap_or_panic {
    ($expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(err) => panic!("Error: {:?}", err),
        }
    };
}
