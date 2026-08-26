use super::*;

#[test]
fn high_cardinality_subjects_remain_indexed() {
    let too_many_subjects = (0..=4_096)
        .map(|id| (format!("events/{id}"), id as u64))
        .collect::<Vec<_>>();
    let index = build_index(&too_many_subjects, 1);
    assert_eq!(index.dictionary.len(), too_many_subjects.len());
    assert!(indexed_offsets(&index, "events/4096").unwrap().used_index);

    let too_many_postings = (0..=65_536)
        .map(|offset| ("events/shared".to_string(), offset as u64))
        .collect::<Vec<_>>();
    let index = build_index(&too_many_postings, 1);
    let query = indexed_offsets(&index, "events/*").unwrap();
    assert!(query.used_index);
    assert_eq!(query.offsets.len(), too_many_postings.len());
}
