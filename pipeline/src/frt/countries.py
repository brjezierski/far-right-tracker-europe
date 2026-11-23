from __future__ import annotations

import logging

import pycountry

logger = logging.getLogger(__name__)


# Function to get ISO alpha-2 code for a country name
def get_country_iso_code(country_name):
    try:
        # Search for the country by name
        country = pycountry.countries.get(name=country_name)
        if country:
            return country.alpha_2  # Return the ISO alpha-2 code
        else:
            additional_countries = {
                "Turkey": "TR",
                "Russia": "RU",
                "Czech Republic": "CZ",
                "Kosovo": "XK",
                "Moldova": "MD",
                "Macedonia": "MK",
            }
            return additional_countries.get(country_name, f"Country '{country_name}' not found.")
    except Exception as e:
        return f"An error occurred: {e}"
