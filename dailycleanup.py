from __future__ import annotations

from pathlib import Path


FOLDERS_TO_CLEAN = [
    Path("csv"),
    Path("downloads"),
]

KEEP_LATEST = 50


def cleanup_folder(folder: Path, keep_latest: int = KEEP_LATEST) -> None:
    if not folder.exists():
        print(f"[INFO] Folderis neegzistuoja: {folder}")
        return

    if not folder.is_dir():
        print(f"[WARN] Tai ne folderis: {folder}")
        return

    files = [item for item in folder.iterdir() if item.is_file()]

    if len(files) <= keep_latest:
        print(f"[INFO] {folder}: failų {len(files)}, trinti nereikia")
        return

    # Rikiuojam nuo naujausio iki seniausio pagal modifikavimo laiką
    files_sorted = sorted(files, key=lambda f: f.stat().st_mtime, reverse=True)

    files_to_keep = files_sorted[:keep_latest]
    files_to_delete = files_sorted[keep_latest:]

    print(f"[INFO] {folder}: paliekami {len(files_to_keep)}, trinami {len(files_to_delete)}")

    for file_path in files_to_delete:
        try:
            file_path.unlink()
            print(f"[OK] Ištrintas: {file_path}")
        except Exception as e:
            print(f"[ERROR] Nepavyko ištrinti {file_path}: {e}")


def main() -> None:
    for folder in FOLDERS_TO_CLEAN:
        cleanup_folder(folder)


if __name__ == "__main__":
    main()
