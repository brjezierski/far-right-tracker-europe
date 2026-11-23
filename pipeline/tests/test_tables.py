from bs4 import BeautifulSoup

from frt.scraping.tables import parse_all_tables_from_soup

HTML = """
<table class="wikitable">
  <tr><th>Fieldwork date</th><th>Pollster</th><th><a href="/wiki/Party_A">Party A</a></th>
      <th><a href="/wiki/Party_B">Party B</a></th></tr>
  <tr><td>1 Jan 2024</td><td>Poll Co</td><td>30%</td><td>20%</td></tr>
  <tr><td>8 Jan 2024</td><td>Poll Co</td><td>31%</td><td>19%</td></tr>
</table>
"""


def test_parse_all_tables_returns_one_dataframe_with_a_row_per_poll():
    tables = parse_all_tables_from_soup(BeautifulSoup(HTML, "lxml"))
    assert len(tables) == 1
    assert len(tables[0]) == 2
