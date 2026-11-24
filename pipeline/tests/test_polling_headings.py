from frt.scraping.polling import clean_section_title


def test_strips_english_edit_link():
    assert clean_section_title("2025[edit]") == "2025"


def test_strips_turkish_edit_links():
    assert clean_section_title("2025[değiştir|kaynağı değiştir]") == "2025"


def test_leaves_plain_headings_alone():
    assert clean_section_title("Party vote") == "Party vote"
