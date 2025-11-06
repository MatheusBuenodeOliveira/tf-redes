import pytest

from router import parse_advert, parse_text_message


def test_parse_advert_empty():
    assert parse_advert("") == []


def test_parse_advert_single():
    s = "*192.168.1.2;1"
    out = parse_advert(s)
    assert out == [("192.168.1.2", 1)]


def test_parse_advert_multiple():
    s = "*192.168.1.2;1*10.0.0.5;3"
    out = parse_advert(s)
    assert ("192.168.1.2", 1) in out
    assert ("10.0.0.5", 3) in out


def test_parse_text_message_ok():
    s = "!192.168.1.5;192.168.1.2;Oi tudo bem?"
    orig, dest, text = parse_text_message(s)
    assert orig == "192.168.1.5"
    assert dest == "192.168.1.2"
    assert text == "Oi tudo bem?"


def test_parse_text_message_bad():
    with pytest.raises(ValueError):
        parse_text_message("not_a_message")

    with pytest.raises(ValueError):
        parse_text_message("!bad;format")
