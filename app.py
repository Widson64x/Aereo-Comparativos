# app.py
from __future__ import annotations
from datetime import datetime
import os
from pathlib import Path
from flask import Flask
from config import AppConfig, Paths
from utils.files import ensure_dirs

# BASE PREFIX fixo para essa app
BASE_PREFIX = "/aereo-comparativos"

def create_app() -> Flask:
    app = Flask(__name__)
    @app.context_processor
    def inject_now():
        return {'now': datetime.now}

    app.secret_key = os.environ.get('SECRET_KEY', 'chave01')

    base_dir = Path(__file__).parent.resolve()
    paths = Paths(
        BASE_DIR=base_dir,
        UPLOAD_DIR=base_dir / "uploads",
        OUTPUT_DIR=base_dir / "outputs",
        CACHE_DIR=base_dir / "cache",
    )
    ensure_dirs(paths.UPLOAD_DIR, paths.OUTPUT_DIR, paths.CACHE_DIR)
    app.config["APP_CFG"] = AppConfig(paths=paths)

    # -------- IMPORTS EXPLÍCITOS DOS BLUEPRINTS --------
    # Cada arquivo em routes/* deve expor "bp = Blueprint(...)".
    from routes.main import bp as main_bp
    from routes.PDF_Exporter import bp as pdf_bp
    from routes.historical_documents import bp as hist_bp
    from routes.freight_tables_formatter import bp as freight_bp

    # -------- REGISTRO COM PREFIXO BASE --------
    # Tudo vive sob /aereo-comparativos
    app.register_blueprint(main_bp,     url_prefix=f"{BASE_PREFIX}/")
    app.register_blueprint(pdf_bp,      url_prefix=f"{BASE_PREFIX}/fatura")
    app.register_blueprint(hist_bp,     url_prefix=f"{BASE_PREFIX}/historico")
    app.register_blueprint(freight_bp,  url_prefix=f"{BASE_PREFIX}/formatar-acordos")

    # Health local (sem prefixo) opcional, útil p/ testar direto a porta
    @app.get("/healthz")
    def healthz():
        return {"status": "ok", "app": "aereo-comparativos"}, 200

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(host="127.0.0.1", port=int(os.environ.get("PORT", 5000)), debug=True)
