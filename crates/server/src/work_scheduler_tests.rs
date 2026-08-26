use super::work_scheduler::*;

#[test]
fn control_and_foreground_budgets_are_independent() {
    let mut scheduler = WorkScheduler::new([
        (
            WorkClass::Control,
            WorkBudget {
                max_records: 1,
                max_bytes: 10,
                max_concurrency: 1,
            },
        ),
        (
            WorkClass::Foreground,
            WorkBudget {
                max_records: 1,
                max_bytes: 10,
                max_concurrency: 1,
            },
        ),
    ]);
    assert!(scheduler.try_reserve(WorkClass::Control, 1, 10));
    assert!(scheduler.try_reserve(WorkClass::Foreground, 1, 10));
    assert!(!scheduler.try_reserve(WorkClass::Foreground, 1, 1));
    assert_eq!(scheduler.usage(WorkClass::Foreground).rejected, 1);
    scheduler.release(WorkClass::Foreground, 1, 10);
    assert!(scheduler.try_reserve(WorkClass::Foreground, 1, 1));
}

#[test]
fn retention_budget_saturates_and_releases() {
    let mut scheduler = WorkScheduler::new([(
        WorkClass::Retention,
        WorkBudget {
            max_records: u64::MAX,
            max_bytes: u64::MAX,
            max_concurrency: 1,
        },
    )]);
    assert!(scheduler.try_reserve(WorkClass::Retention, 0, 0));
    assert!(!scheduler.try_reserve(WorkClass::Retention, 0, 0));
    assert_eq!(scheduler.usage(WorkClass::Retention).rejected, 1);
    scheduler.release(WorkClass::Retention, 0, 0);
    assert!(scheduler.try_reserve(WorkClass::Retention, 0, 0));
}
