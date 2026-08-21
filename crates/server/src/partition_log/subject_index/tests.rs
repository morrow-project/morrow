use super::*;

#[test]
fn cardinality_and_posting_budgets_disable_the_optional_index() {
    let too_many_subjects = (0..=MAX_INDEX_SUBJECTS)
        .map(|id| (format!("events.{id}"), id as u64))
        .collect::<Vec<_>>();
    assert!(build_index(&too_many_subjects, 1).is_none());

    let too_many_postings = (0..=MAX_INDEX_POSTINGS)
        .map(|offset| ("events.shared".to_string(), offset as u64))
        .collect::<Vec<_>>();
    assert!(build_index(&too_many_postings, 1).is_none());
}
