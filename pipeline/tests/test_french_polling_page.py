import pandas as pd

from frt.scraping.polling import is_election_result, parse_date
from frt.scraping.sources import get_party_aliases

URL = "https://fr.wikipedia.org/wiki/%C3%89lections_f%C3%A9d%C3%A9rales_suisses_de_2027"


def test_parses_end_of_a_french_date_range():
    date, _ = parse_date("31 oct. - 17 novembre 2025", URL)
    assert date == pd.Timestamp(2025, 11, 17)


def test_parses_french_first_of_the_month():
    date, _ = parse_date("18 mai - 1 er juin 2026", URL)
    assert date == pd.Timestamp(2026, 6, 1)


def test_recognises_election_result_rows():
    assert is_election_result("Dernières élections [ 43 ]")
    assert is_election_result("2023 federal election")
    assert not is_election_result("YouGov")


def test_maps_french_party_names_to_existing_ones():
    aliases = get_party_aliases("Switzerland")
    assert aliases["Union démocratique du centre"] == "Swiss People's Party"
    assert get_party_aliases("Germany") == {}
