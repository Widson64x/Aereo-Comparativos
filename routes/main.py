# routes/main.py
from __future__ import annotations
from flask import Blueprint, render_template
from datetime import datetime
from zoneinfo import ZoneInfo

bp = Blueprint("main", __name__)

@bp.route("/", methods=["GET"])
def index():
    tz = ZoneInfo("America/Sao_Paulo")
    return render_template("index.html", last_update=datetime.now(tz))
