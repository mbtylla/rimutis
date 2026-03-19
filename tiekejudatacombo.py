from __future__ import annotations

import csv
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo


TIMEZONE = ZoneInfo("Europe/Vilnius")
CSV_DIR = Path("csv")
OUTPUT_DIR = Path("combined")

SUPPLIERS = ["Marini", "Zuja", "Tylla"]
INPUT_COLUMNS = ["Kodas", "EAN", "Likutis"]
OUTPUT_COLUMNS = [
    "Kodas",
    "EAN",
    "stock_local",
    "stock_supplier_fast",
    "stock_supplier_slow",
    "total_stock",
]


def current_timestamp() -> str:
    return datetime.now(TIMEZONE).strftime("%Y-%m-%d_%H-%M-%S")


def extract_supplier(filename: str) -> str | None:
    match = re.match(
        r"^(Marini|Zuja|Tylla)_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\.csv$",
        filename,
    )
    if not match:
        return None
    return match.group(1)


def get_latest_supplier_file(folder: Path, supplier_name: str) -> Path:
    matching_files = []

    for file_path in folder.glob("*.csv"):
        detected_supplier = extract_supplier(file_path.name)
        if detected_supplier == supplier_name:
            matching_files.append(file_path)

    if not matching_files:
        raise FileNotFoundError(f"Nerastas CSV failas tiekėjui: {supplier_name}")

    return max(matching_files, key=lambda p: p.stat().st_mtime)


def validate_columns(fieldnames: list[str] | None, file_path: Path) -> None:
    if fieldnames is None:
        raise ValueError(f"CSV failas neturi antraštės: {file_path}")

    missing = [col for col in INPUT_COLUMNS if col not in fieldnames]
    if missing:
        raise ValueError(f"CSV faile {file_path} trūksta stulpelių: {', '.join(missing)}")


def clean_text(value: object) -> str:
    if value is None:
        return ""

    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return str(value)

    text = str(value).strip()

    if text.endswith(".0"):
        try:
            return str(int(float(text)))
        except Exception:
            pass

    return text


def parse_stock(value: object) -> int:
    text = clean_text(value).replace(",", ".")
    if not text:
        return 0

    try:
        return int(float(text))
    except ValueError:
        return 0


def make_key(kodas: str, ean: str) -> str:
    kodas = clean_text(kodas)
    ean = clean_text(ean)

    if kodas:
        return f"KODAS::{kodas}"
    if ean:
        return f"EAN::{ean}"
    return ""


def read_supplier_csv(file_path: Path) -> list[dict[str, str]]:
    rows = []

    with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        validate_columns(reader.fieldnames, file_path)

        for row in reader:
            kodas = clean_text(row.get("Kodas"))
            ean = clean_text(row.get("EAN"))
            likutis = clean_text(row.get("Likutis"))

            if not kodas and not ean:
                continue

            rows.append({
                "Kodas": kodas,
                "EAN": ean,
                "Likutis": likutis,
            })

    return rows


def merge_supplier_rows(
    combined: dict[str, dict[str, object]],
    supplier_name: str,
    rows: list[dict[str, str]],
) -> None:
    for row in rows:
        kodas = row["Kodas"]
        ean = row["EAN"]
        likutis = parse_stock(row["Likutis"])

        key = make_key(kodas, ean)
        if not key:
            continue

        if key not in combined:
            combined[key] = {
                "Kodas": kodas,
                "EAN": ean,
                "stock_local": 0,
                "stock_supplier_fast": 0,
                "stock_supplier_slow": 0,
            }

        if not combined[key]["Kodas"] and kodas:
            combined[key]["Kodas"] = kodas
        if not combined[key]["EAN"] and ean:
            combined[key]["EAN"] = ean

        if supplier_name == "Tylla":
            combined[key]["stock_local"] = likutis
        elif supplier_name == "Zuja":
            combined[key]["stock_supplier_fast"] = likutis
        elif supplier_name == "Marini":
            combined[key]["stock_supplier_slow"] = likutis


def finalize_rows(combined: dict[str, dict[str, object]]) -> list[dict[str, object]]:
    final_rows = []

    for _, item in combined.items():
        stock_local = int(item["stock_local"])
        stock_supplier_fast = int(item["stock_supplier_fast"])
        stock_supplier_slow = int(item["stock_supplier_slow"])
        total_stock = stock_local + stock_supplier_fast + stock_supplier_slow

        final_rows.append({
            "Kodas": clean_text(item["Kodas"]),
            "EAN": clean_text(item["EAN"]),
            "stock_local": stock_local,
            "stock_supplier_fast": stock_supplier_fast,
            "stock_supplier_slow": stock_supplier_slow,
            "total_stock": total_stock,
        })

    final_rows.sort(key=lambda x: (x["Kodas"], x["EAN"]))
    return final_rows


def save_combined_csv(rows: list[dict[str, object]], timestamp: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"tiekejulikuciai_{timestamp}.csv"

    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    return output_path


def indent_xml(elem: ET.Element, level: int = 0) -> None:
    indent = "\n" + level * "    "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = indent + "    "
        for child in elem:
            indent_xml(child, level + 1)
        if not child.tail or not child.tail.strip():
            child.tail = indent
    if level and (not elem.tail or not elem.tail.strip()):
        elem.tail = indent


def save_combined_xml(rows: list[dict[str, object]], timestamp: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"tiekejulikuciai_{timestamp}.xml"

    root = ET.Element("products")

    for row in rows:
        product = ET.SubElement(root, "product")

        ET.SubElement(product, "kodas").text = clean_text(row["Kodas"])
        ET.SubElement(product, "ean").text = clean_text(row["EAN"])
        ET.SubElement(product, "stock_local").text = str(row["stock_local"])
        ET.SubElement(product, "stock_supplier_fast").text = str(row["stock_supplier_fast"])
        ET.SubElement(product, "stock_supplier_slow").text = str(row["stock_supplier_slow"])
        ET.SubElement(product, "total_stock").text = str(row["total_stock"])

    indent_xml(root)

    tree = ET.ElementTree(root)
    tree.write(output_path, encoding="utf-8", xml_declaration=True)

    return output_path


def main() -> None:
    if not CSV_DIR.exists():
        raise FileNotFoundError(f"Nerastas folderis: {CSV_DIR}")

    latest_files = {}
    for supplier in SUPPLIERS:
        latest_files[supplier] = get_latest_supplier_file(CSV_DIR, supplier)
        print(f"[INFO] {supplier}: naudojamas failas {latest_files[supplier].name}")

    combined: dict[str, dict[str, object]] = {}

    for supplier, file_path in latest_files.items():
        rows = read_supplier_csv(file_path)
        merge_supplier_rows(combined, supplier, rows)
        print(f"[OK] {supplier}: apdorota {len(rows)} eilučių")

    final_rows = finalize_rows(combined)
    timestamp = current_timestamp()

    csv_path = save_combined_csv(final_rows, timestamp)
    xml_path = save_combined_xml(final_rows, timestamp)

    print(f"[OK] Sukurtas CSV: {csv_path}")
    print(f"[OK] Sukurtas XML: {xml_path}")
    print(f"[OK] Iš viso eilučių: {len(final_rows)}")


if __name__ == "__main__":
    main()
