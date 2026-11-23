import pytest

from frt.classification import CATEGORIES, is_party_far_right


@pytest.mark.parametrize(
    ("position", "ideology", "expected"),
    [
        ("Far-right", "Right-wing populism", True),
        ("Centre-right", "National conservatism", False),
        ("far right", "", True),
        ("Centre-left", "Social democracy", False),
        ("", "", False),
        (float("nan"), float("nan"), False),
    ],
)
def test_is_party_far_right(position, ideology, expected):
    assert is_party_far_right(position, ideology, CATEGORIES) is expected


def test_national_conservatism_alone_is_not_far_right():
    assert is_party_far_right("Right-wing", "National conservatism", CATEGORIES) is False


def test_ideology_used_only_without_position():
    assert is_party_far_right("", "Far-right; Nationalism", CATEGORIES) is True
    assert is_party_far_right("Centre-right", "Far-right", CATEGORIES) is False


def test_deny_list_overrides_text():
    assert is_party_far_right("Right-wing to far-right", "", CATEGORIES, "HU", "Jobbik") is False
    assert is_party_far_right("Right-wing to far-right", "", CATEGORIES, "HU", "Other") is True
