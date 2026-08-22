use super::*;

#[test]
fn parses_openraft_snapshot_size_syntax() {
    assert_eq!("3MiB".parse::<Byte>().unwrap().as_u64(), 3 * 1_048_576);
    assert_eq!("5.3 KB".parse::<Byte>().unwrap().as_u64(), 5_300);
    assert!("3bogus".parse::<Byte>().is_err());
}
