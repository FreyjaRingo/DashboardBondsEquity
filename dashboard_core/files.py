import json
from pathlib import Path, PurePath


APP_ROOT = Path(__file__).resolve().parent.parent
ASSET_DIR = APP_ROOT / "dashboard_assets"
SUMMARY_PDF_PATH = ASSET_DIR / "summary.pdf"
SUMMARY_PDF_META_PATH = ASSET_DIR / "summary_pdf.json"


def _clean_filename(filename):
    name = PurePath(filename or "ringkasan.pdf").name
    return name if name.lower().endswith(".pdf") else f"{name}.pdf"


def save_summary_pdf(data, filename):
    if not data or not data.startswith(b"%PDF"):
        raise ValueError("File yang diupload bukan PDF valid.")

    ASSET_DIR.mkdir(parents=True, exist_ok=True)
    SUMMARY_PDF_PATH.write_bytes(data)
    SUMMARY_PDF_META_PATH.write_text(
        json.dumps({"filename": _clean_filename(filename)}, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def delete_summary_pdf():
    for path in (SUMMARY_PDF_PATH, SUMMARY_PDF_META_PATH):
        if path.exists():
            path.unlink()


def summary_pdf_exists():
    return SUMMARY_PDF_PATH.exists()


def get_summary_pdf_bytes():
    if not SUMMARY_PDF_PATH.exists():
        return None
    return SUMMARY_PDF_PATH.read_bytes()


def get_summary_pdf_name():
    if not SUMMARY_PDF_META_PATH.exists():
        return "ringkasan.pdf"

    try:
        meta = json.loads(SUMMARY_PDF_META_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return "ringkasan.pdf"

    return _clean_filename(meta.get("filename"))
