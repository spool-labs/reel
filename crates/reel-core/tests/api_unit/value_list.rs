use reel_core::value_list::*;

// answers come back in order, and clear keeps nothing
#[test]
fn answers() {
    let mut list = ValueList::new();

    list.push_found(b"first");
    list.push_missing();
    list.push_corrupt();
    list.push_found(b"");

    assert_eq!(list.len(), 4);
    assert_eq!(list.answer(0), Some(ValueAnswer::Found(b"first")));
    assert_eq!(list.answer(1), Some(ValueAnswer::Missing));
    assert_eq!(list.answer(2), Some(ValueAnswer::Corrupt));
    assert_eq!(list.answer(3), Some(ValueAnswer::Found(b"")));
    assert_eq!(list.answer(4), None);

    list.clear();
    assert!(list.is_empty());
}
