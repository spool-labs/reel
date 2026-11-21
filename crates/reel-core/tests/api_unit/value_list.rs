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

// a long value swaps into the list with no copy and moves out the same way, a short one is copied, and a cleared list keeps the
// long values' room for the next reads to fill
#[test]
fn long_values_keep_their_buffers() {
    let mut list = ValueList::new();
    let long = vec![7_u8; OWNED_VALUE_BYTES + 1];
    let mut buffer = long.clone();
    let address = buffer.as_ptr();
    list.push_filled(&mut buffer);
    assert!(buffer.is_empty(), "the read's buffer kept the long value");
    let mut short = b"short".to_vec();
    list.push_filled(&mut short);
    assert_eq!(short, b"short", "a short value left its buffer");
    list.push_missing();

    assert_eq!(list.answer(0), Some(ValueAnswer::Found(&long)));
    assert_eq!(list.answer(1), Some(ValueAnswer::Found(b"short")));
    assert_eq!(list.answer(2), Some(ValueAnswer::Missing));
    let taken = list.take_answer(0);
    assert!(matches!(&taken, Some(TakenAnswer::Found(value)) if value.as_ptr() == address && *value == long), "the long value was copied");
    assert_eq!(list.take_answer(1), Some(TakenAnswer::Found(b"short".to_vec())));
    assert_eq!(list.take_answer(2), Some(TakenAnswer::Missing));

    let mut again = vec![1_u8; OWNED_VALUE_BYTES + 1];
    list.push_filled(&mut again);
    let kept = list.answer(3).map(|answer| matches!(answer, ValueAnswer::Found(value) if value.len() == OWNED_VALUE_BYTES + 1));
    assert_eq!(kept, Some(true));
    list.clear();
    let mut next = vec![2_u8; OWNED_VALUE_BYTES + 1];
    list.push_filled(&mut next);
    assert!(next.capacity() > OWNED_VALUE_BYTES, "a cleared list gave back no room for the next read");
}
