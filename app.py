from __future__ import annotations
from datetime import datetime
import os
from pathlib import Path
from flask import Flask
from routes import freight_tables_formatter, historical_documents, main, PDF_Exporter
from config import AppConfig, Paths
from utils.files import ensure_dirs


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

    # Blueprints
    app.register_blueprint(main.bp)
    app.register_blueprint(PDF_Exporter.bp, url_prefix="/fatura")
    app.register_blueprint(historical_documents.bp, url_prefix="/historico")
    app.register_blueprint(freight_tables_formatter.bp, url_prefix="/formatar-acordos")

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
