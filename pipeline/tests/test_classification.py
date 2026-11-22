import pytest

from frt.classification import CATEGORIES, is_party_far_right


@pytest.mark.parametrize(
    ("position", "ideology", "expected"),
    [
        ("Far-right", "Right-wing populism", True),
        ("Centre-right", "National conservatism", True),
        ("far right", "", True),
        ("Centre-left", "Social democracy", False),
        ("", "", False),
        (float("nan"), float("nan"), False),
    ],
)
def test_is_party_far_right(position, ideology, expected):
    assert is_party_far_right(position, ideology, CATEGORIES) is expected
