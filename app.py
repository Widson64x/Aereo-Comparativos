# app.py
from __future__ import annotations
from datetime import datetime
import os
from pathlib import Path
from flask import Flask
from routes import freight_tables_formatter, historical_documents, main, PDF_Exporter
from config import AppConfig, Paths
from utils.files import ensure_dirs

BASE_PREFIX = "/aereo-comparativos"

def create_app() -> Flask:
    app = Flask(
        __name__,
        static_folder="static",                      # C:\ProjetosPython\Aereo\static
        static_url_path=f"{BASE_PREFIX}/static",    # => /aereo-comparativos/static/...
    )
    app.jinja_env.globals['BASE_PREFIX'] = '/aereo-comparativos'
    @app.context_processor
    def inject_now():
        return {"now": datetime.now, "BASE_PREFIX": BASE_PREFIX}

    app.secret_key = os.environ.get("SECRET_KEY", "chave01")

    base_dir = Path(__file__).parent.resolve()
    paths = Paths(
        BASE_DIR=base_dir,
        UPLOAD_DIR=base_dir / "uploads",
        OUTPUT_DIR=base_dir / "outputs",
        CACHE_DIR=base_dir / "cache",
    )
    ensure_dirs(paths.UPLOAD_DIR, paths.OUTPUT_DIR, paths.CACHE_DIR)
    app.config["APP_CFG"] = AppConfig(paths=paths)

    # Blueprints sob o mesmo prefixo
    app.register_blueprint(main.bp,                       url_prefix=f"{BASE_PREFIX}/")
    app.register_blueprint(PDF_Exporter.bp,               url_prefix=f"{BASE_PREFIX}/fatura")
    app.register_blueprint(historical_documents.bp,       url_prefix=f"{BASE_PREFIX}/historico")
    app.register_blueprint(freight_tables_formatter.bp,   url_prefix=f"{BASE_PREFIX}/formatar-acordos")

    # Health no prefixo (facilita teste via Nginx)
    @app.get(f"{BASE_PREFIX}/healthz")
    def healthz():
        return {"status": "ok"}, 200

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(host="127.0.0.1", port=int(os.environ.get("PORT", 9003)), debug=True)
