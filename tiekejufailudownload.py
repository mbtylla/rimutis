from __future__ import annotations

import re
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import requests


TIMEZONE = ZoneInfo("Europe/Vilnius")
OUTPUT_DIR = Path("downloads")

SUPPLIERS = {
    "Marini": {
        "url": "https://marini.pl/b2b/marini-b2b.xml",
        "extension": ".xml",
    },
    "Zuja": {
        "url": (
            "https://zuja.lt/index.php?route=feed/store/generate"
            "?filters=YToyOntzOjI0OiJmaWx0ZXJfY3VzdG9tZXJfZ3JvdXBfaWQiO3M6MjoiMTIiO3M6Mzoia2V5IjtzOjMyOiJjODFlNzI4ZDlkNGMyZjYzNmYwNjdmODljYzE0ODYyYyI7fQ=="
            "&key=c81e728d9d4c2f636f067f89cc14862c"
        ),
        "extension": ".xml",
    },
    "Tylla": {
        # Google Sheets eksportas į XLSX
        "url": "https://docs.google.com/spreadsheets/d/1T4Gpfk4Uv9FuvDO0WQeRfPIvqjfMxOdGIXLoKB8kE6o/export?format=xlsx",
        "extension": ".xlsx",
    },
}


def sanitize_filename(name: str) -> str:
    """
    Pašalina failo vardui netinkamus simbolius.
    """
    return re.sub(r'[\\/*?:"<>|]+', "_", name).strip()


def build_filename(supplier_name: str, extension: str) -> str:
    """
    Sugeneruoja failo vardą su LT laiku.
    Pvz.: Marini_2026-03-19_14-23-11.xml
    """
    now_lt = datetime.now(TIMEZONE)
    timestamp = now_lt.strftime("%Y-%m-%d_%H-%M-%S")
    safe_supplier_name = sanitize_filename(supplier_name)
    return f"{safe_supplier_name}_{timestamp}{extension}"


def download_file(url: str, target_path: Path, timeout: int = 120) -> None:
    """
    Parsisiunčia failą į diską.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; stock-sync-bot/1.0)",
    }

    with requests.get(url, headers=headers, stream=True, timeout=timeout) as response:
        response.raise_for_status()
        with open(target_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for supplier_name, config in SUPPLIERS.items():
        url = config["url"]
        extension = config["extension"]
        filename = build_filename(supplier_name, extension)
        target_path = OUTPUT_DIR / filename

        try:
            print(f"[INFO] Parsisiunčiu: {supplier_name}")
            download_file(url, target_path)
            print(f"[OK] Išsaugota: {target_path}")
        except requests.HTTPError as e:
            print(f"[ERROR] HTTP klaida ({supplier_name}): {e}")
        except requests.RequestException as e:
            print(f"[ERROR] Tinklo klaida ({supplier_name}): {e}")
        except Exception as e:
            print(f"[ERROR] Nenumatyta klaida ({supplier_name}): {e}")


if __name__ == "__main__":
    main()
