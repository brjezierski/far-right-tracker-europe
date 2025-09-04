import re
import json
from pathlib import Path
from datetime import datetime
from typing import Any, Dict
import pycountry


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
            return additional_countries.get(
                country_name, f"Country '{country_name}' not found."
            )
    except Exception as e:
        return f"An error occurred: {e}"


ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
PIPELINE_DIR = ROOT / "data-pipeline/pipeline"
COUNTRIES_DIR = DATA_DIR / "countries"

COUNTRIES_DIR.mkdir(parents=True, exist_ok=True)


def slugify(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "-", name.strip().lower())
    return s.strip("-")


def save_json(path: Path, data: Dict[str, Any]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
