import zipfile
import os

# Works both inside Docker (/app/data) and on the host (../../data)
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_DATA_DIR = os.environ.get(
    "DATA_DIR", os.path.join(_SCRIPT_DIR, "..", "..", "data")
)
RAW_DIR = os.path.join(_DATA_DIR, "raw")
PROCESSED_DIR = os.path.join(_DATA_DIR, "processed")


def extract_zip(zip_filename: str) -> None:
    # Normalize the supplied capstone archive into individual files under
    # data/processed so the ingestion step can load them one by one.
    zip_path = os.path.join(RAW_DIR, zip_filename)

    if not os.path.exists(zip_path):
        print(f"[SKIP] {zip_path} not found.")
        return

    os.makedirs(PROCESSED_DIR, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            # Skip __MACOSX metadata and directories
            if "__MACOSX" in member.filename or member.filename.endswith("/"):
                continue

            # Strip any leading directory paths, extract file directly
            filename = os.path.basename(member.filename)
            if not filename:
                continue

            target_path = os.path.join(PROCESSED_DIR, filename)
            # Keep extraction idempotent for mounted volumes where files may be read-only.
            if os.path.exists(target_path):
                print(f"[SKIP] Already exists -> {target_path}")
                continue

            try:
                # Stream the zip member directly into the processed folder
                # instead of extracting nested directory structures.
                with zf.open(member) as src, open(target_path, "wb") as dst:
                    dst.write(src.read())
                print(f"[OK] Extracted -> {target_path}")
            except PermissionError:
                print(f"[SKIP] No write permission -> {target_path}")


if __name__ == "__main__":
    extract_zip("project_data.zip")
