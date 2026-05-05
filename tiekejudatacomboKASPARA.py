from __future__ import annotations

import csv
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

MAX_STALE_HOURS = 24
FULL_XML_INTERVAL_HOURS = 24

TIMEZONE = ZoneInfo("Europe/Vilnius")
CSV_DIR = Path("csv")
OUTPUT_DIR = Path("combined")

DELTA_XML_PATH = OUTPUT_DIR / "tiekeju_likuciai.xml"
FULL_XML_PATH = OUTPUT_DIR / "tiekeju_likuciai_full.xml"

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


def get_file_age_hours(file_path: Path) -> float:
    now = datetime.now(TIMEZONE)
    file_time = datetime.fromtimestamp(file_path.stat().st_mtime, TIMEZONE)
    return (now - file_time).total_seconds() / 3600


def should_generate_full_xml() -> bool:
    if not FULL_XML_PATH.exists():
        return True

    age_hours = get_file_age_hours(FULL_XML_PATH)
    return age_hours >= FULL_XML_INTERVAL_HOURS


def current_timestamp() -> str:
    return datetime.now(TIMEZONE).strftime("%Y-%m-%d_%H-%M-%S")


def parse_timestamp_from_supplier_filename(filename: str) -> datetime | None:
    match = re.match(
        r"^(Marini|Zuja|Tylla)_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\.csv$",
        filename,
    )
    if not match:
        return None

    try:
        return datetime.strptime(match.group(2), "%Y-%m-%d_%H-%M-%S")
    except ValueError:
        return None


def parse_timestamp_from_combined_filename(filename: str) -> datetime | None:
    match = re.match(
        r"^tiekejulikuciai_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\.csv$",
        filename,
    )
    if not match:
        return None

    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d_%H-%M-%S")
    except ValueError:
        return None


def extract_supplier(filename: str) -> str | None:
    match = re.match(
        r"^(Marini|Zuja|Tylla)_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\.csv$",
        filename,
    )
    if not match:
        return None

    return match.group(1)


def get_latest_supplier_file(folder: Path, supplier_name: str) -> Path:
    matching_files: list[tuple[Path, datetime]] = []

    for file_path in folder.glob("*.csv"):
        detected_supplier = extract_supplier(file_path.name)
        if detected_supplier != supplier_name:
            continue

        parsed_ts = parse_timestamp_from_supplier_filename(file_path.name)
        if parsed_ts is None:
            continue

        matching_files.append((file_path, parsed_ts))

    if not matching_files:
        raise FileNotFoundError(f"Nerastas CSV failas tiekėjui: {supplier_name}")

    matching_files.sort(key=lambda x: x[1], reverse=True)
    return matching_files[0][0]


def validate_columns(fieldnames: list[str] | None, file_path: Path) -> None:
    if fieldnames is None:
        raise ValueError(f"CSV failas neturi antraštės: {file_path}")

    missing = [col for col in INPUT_COLUMNS if col not in fieldnames]
    if missing:
        raise ValueError(
            f"CSV faile {file_path} trūksta stulpelių: {', '.join(missing)}"
        )


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


def make_key(kodas: str) -> str:
    return clean_text(kodas)


def read_supplier_csv(file_path: Path) -> list[dict[str, str]]:
    rows = []

    with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        validate_columns(reader.fieldnames, file_path)

        for row in reader:
            kodas = clean_text(row.get("Kodas"))
            ean = clean_text(row.get("EAN"))
            likutis = clean_text(row.get("Likutis"))

            if not kodas:
                continue

            rows.append(
                {
                    "Kodas": kodas,
                    "EAN": ean,
                    "Likutis": likutis,
                }
            )

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

        key = make_key(kodas)
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

    for item in combined.values():
        stock_local = int(item["stock_local"])
        stock_supplier_fast = int(item["stock_supplier_fast"])
        stock_supplier_slow = int(item["stock_supplier_slow"])
        total_stock = stock_local + stock_supplier_fast + stock_supplier_slow

        final_rows.append(
            {
                "Kodas": clean_text(item["Kodas"]),
                "EAN": clean_text(item["EAN"]),
                "stock_local": stock_local,
                "stock_supplier_fast": stock_supplier_fast,
                "stock_supplier_slow": stock_supplier_slow,
                "total_stock": total_stock,
            }
        )

    final_rows.sort(key=lambda x: (clean_text(x["Kodas"]), clean_text(x["EAN"])))
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


def save_xml(rows: list[dict[str, object]], output_path: Path) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    root = ET.Element("products")

    for row in rows:
        product = ET.SubElement(root, "product")

        ET.SubElement(product, "kodas").text = clean_text(row["Kodas"])

        ean_value = clean_text(row["EAN"])
        if ean_value:
            ET.SubElement(product, "ean").text = ean_value

        ET.SubElement(product, "stock_local").text = str(row["stock_local"])
        ET.SubElement(product, "stock_supplier_fast").text = str(
            row["stock_supplier_fast"]
        )
        ET.SubElement(product, "stock_supplier_slow").text = str(
            row["stock_supplier_slow"]
        )
        ET.SubElement(product, "total_stock").text = str(row["total_stock"])

    indent_xml(root)

    tree = ET.ElementTree(root)
    tree.write(output_path, encoding="utf-8", xml_declaration=True)

    return output_path


def save_delta_xml(rows: list[dict[str, object]]) -> Path:
    return save_xml(rows, DELTA_XML_PATH)


def get_previous_combined_csv(current_file: Path) -> Path | None:
    files_with_ts: list[tuple[Path, datetime]] = []

    for file_path in OUTPUT_DIR.glob("tiekejulikuciai_*.csv"):
        parsed_ts = parse_timestamp_from_combined_filename(file_path.name)
        if parsed_ts is None:
            continue

        files_with_ts.append((file_path, parsed_ts))

    files_with_ts.sort(key=lambda x: x[1], reverse=True)

    for file_path, _ in files_with_ts:
        if file_path != current_file:
            return file_path

    return None


def load_combined_csv_as_dict(file_path: Path | None) -> dict[str, dict[str, object]]:
    data: dict[str, dict[str, object]] = {}

    if file_path is None or not file_path.exists():
        return data

    with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            kodas = clean_text(row.get("Kodas"))
            if not kodas:
                continue

            data[kodas] = {
                "Kodas": kodas,
                "EAN": clean_text(row.get("EAN")),
                "stock_local": parse_stock(row.get("stock_local")),
                "stock_supplier_fast": parse_stock(row.get("stock_supplier_fast")),
                "stock_supplier_slow": parse_stock(row.get("stock_supplier_slow")),
                "total_stock": parse_stock(row.get("total_stock")),
            }

    return data


def generate_delta_rows(
    current_rows: list[dict[str, object]],
    previous_data: dict[str, dict[str, object]],
) -> list[dict[str, object]]:
    delta_rows: dict[str, dict[str, object]] = {}
    current_kodai: set[str] = set()

    for row in current_rows:
        kodas = clean_text(row["Kodas"])
        if not kodas:
            continue

        current_kodai.add(kodas)

        current_stock = {
            "stock_local": int(row["stock_local"]),
            "stock_supplier_fast": int(row["stock_supplier_fast"]),
            "stock_supplier_slow": int(row["stock_supplier_slow"]),
            "total_stock": int(row["total_stock"]),
        }

        previous = previous_data.get(kodas)

        if previous is None:
            delta_rows[kodas] = row
            continue

        previous_stock = {
            "stock_local": int(previous["stock_local"]),
            "stock_supplier_fast": int(previous["stock_supplier_fast"]),
            "stock_supplier_slow": int(previous["stock_supplier_slow"]),
            "total_stock": int(previous["total_stock"]),
        }

        if current_stock != previous_stock:
            delta_rows[kodas] = row

    for kodas, previous in previous_data.items():
        if kodas not in current_kodai:
            delta_rows[kodas] = {
                "Kodas": kodas,
                "EAN": clean_text(previous.get("EAN")),
                "stock_local": 0,
                "stock_supplier_fast": 0,
                "stock_supplier_slow": 0,
                "total_stock": 0,
            }

    final_delta_rows = list(delta_rows.values())
    final_delta_rows.sort(key=lambda x: (clean_text(x["Kodas"]), clean_text(x["EAN"])))

    return final_delta_rows


def main() -> None:
    if not CSV_DIR.exists():
        raise FileNotFoundError(f"Nerastas folderis: {CSV_DIR}")

    latest_files: dict[str, Path] = {}
    supplier_status: dict[str, bool] = {}
    supplier_age: dict[str, float] = {}

    for supplier in SUPPLIERS:
        file_path = get_latest_supplier_file(CSV_DIR, supplier)
        latest_files[supplier] = file_path

        age_hours = get_file_age_hours(file_path)
        supplier_age[supplier] = age_hours

        is_fresh_enough = age_hours <= MAX_STALE_HOURS
        supplier_status[supplier] = is_fresh_enough

        print(
            f"[INFO] {supplier}: {file_path.name} | "
            f"age={age_hours:.1f}h | allowed={is_fresh_enough}"
        )

    combined: dict[str, dict[str, object]] = {}

    for supplier, file_path in latest_files.items():
        rows = read_supplier_csv(file_path)

        print(f"[DEBUG] {supplier}: pirmos 3 eilutės preview: {rows[:3]}")

        merge_supplier_rows(combined, supplier, rows)

        print(f"[OK] {supplier}: apdorota {len(rows)} eilučių")

    final_rows = finalize_rows(combined)
    timestamp = current_timestamp()

    csv_path = save_combined_csv(final_rows, timestamp)
    print(f"[OK] Sukurtas pilnas CSV: {csv_path}")

    previous_csv = get_previous_combined_csv(csv_path)

    if previous_csv:
        print(f"[INFO] Ankstesnis CSV palyginimui: {previous_csv.name}")
    else:
        print("[INFO] Ankstesnio CSV nerasta, visos prekės bus laikomos naujomis")

    previous_data = load_combined_csv_as_dict(previous_csv)
    delta_rows = generate_delta_rows(final_rows, previous_data)

    all_ok = all(supplier_status.values())

    if not all_ok:
        print("[WARN] Bent vieno tiekėjo paskutinis failas senesnis nei 24h")

        for supplier in SUPPLIERS:
            if not supplier_status[supplier]:
                print(
                    f"[WARN] {supplier}: paskutinis failas per senas "
                    f"({supplier_age[supplier]:.1f}h > {MAX_STALE_HOURS}h)"
                )

    if not all_ok:
        print("[WARN] XML negeneruojamas dėl per senų fallback duomenų")
    elif delta_rows:
        xml_path = save_delta_xml(delta_rows)
        print(f"[OK] Sukurtas DELTA XML: {xml_path}")
        print(f"[OK] Pokyčių / delta eilučių: {len(delta_rows)}")
    else:
        print("[INFO] Pokyčių nėra, DELTA XML negeneruojamas")

    if all_ok and should_generate_full_xml():
        full_xml_path = save_xml(final_rows, FULL_XML_PATH)
        print(f"[OK] Sukurtas PILNAS XML: {full_xml_path}")
        print(f"[OK] Pilno XML eilučių: {len(final_rows)}")
    elif all_ok:
        print("[INFO] Pilnas XML negeneruojamas, nes jau buvo sukurtas per paskutines 24h")

    print(f"[OK] Iš viso pilno CSV eilučių: {len(final_rows)}")


if __name__ == "__main__":
    main()
