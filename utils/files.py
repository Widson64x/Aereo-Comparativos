# utils/files.py
from __future__ import annotations
from pathlib import Path

def ensure_dirs(*dirs: Path):
    for d in dirs:
        Path(d).mkdir(parents=True, exist_ok=True)

def allowed_file(filename: str, allowed_exts: set[str]) -> bool:
    return "." in filename and Path(filename).suffix.lower() in allowed_exts
