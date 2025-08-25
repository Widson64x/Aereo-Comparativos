# routes/historicos_faturas.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Blueprint, render_template, current_app, send_file, abort, request

from config import AppConfig

bp = Blueprint("hist", __name__, template_folder="../templates", url_prefix="/historico")

TZ = ZoneInfo("America/Sao_Paulo")

@dataclass
class FileRow:
    name: str
    path: Path
    size_bytes: int
    mtime: float

    @property
    def size_mb(self) -> float:
        return round(self.size_bytes / (1024*1024), 2)

    @property
    def mtime_local(self) -> str:
        dt = datetime.fromtimestamp(self.mtime, TZ)
        return dt.strftime("%d/%m/%Y %H:%M:%S")

def _list_dir(dirpath: Path) -> list[FileRow]:
    if not dirpath.exists():
        return []
    rows: list[FileRow] = []
    for p in dirpath.iterdir():
        if p.is_file():
            st = p.stat()
            rows.append(FileRow(name=p.name, path=p, size_bytes=st.st_size, mtime=st.st_mtime))
    rows.sort(key=lambda r: r.mtime, reverse=True)
    return rows

@bp.get("/faturas")
def faturas_home():
    app_cfg: AppConfig = current_app.config["APP_CFG"]
    paths = app_cfg.paths
    uploads = _list_dir(paths.UPLOAD_DIR)
    outputs = _list_dir(paths.OUTPUT_DIR)
    return render_template("historicos.html", uploads=uploads, outputs=outputs)

def _safe_lookup(base: Path, filename: str) -> Path | None:
    # evita path traversal
    candidate = base / filename
    try:
        candidate.relative_to(base)
    except Exception:
        return None
    return candidate if candidate.exists() and candidate.is_file() else None

@bp.get("/download/uploads/<path:filename>")
def download_upload(filename: str):
    app_cfg: AppConfig = current_app.config["APP_CFG"]
    paths = app_cfg.paths
    p = _safe_lookup(paths.UPLOAD_DIR, filename)
    if not p:
        abort(404)
    return send_file(p, as_attachment=True, download_name=p.name)

@bp.get("/download/outputs/<path:filename>")
def download_output(filename: str):
    app_cfg: AppConfig = current_app.config["APP_CFG"]
    paths = app_cfg.paths
    p = _safe_lookup(paths.OUTPUT_DIR, filename)
    if not p:
        abort(404)
    return send_file(p, as_attachment=True, download_name=p.name)
