# app.py
from __future__ import annotations
from datetime import datetime
import os
from pathlib import Path
from flask import Flask
from Routes import ComparadorFretes, FormatadorTabelasFrete, Main
from Config import Appconfig, Paths
from Routes import HistoricoDocs
from Utils.Files import ensure_dirs

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
    app.config["MAX_PDF_UPLOAD_MB"] = 20      # MB por arquivo
    app.config["MAX_PDF_UPLOAD_COUNT"] = 10   # máx. de arquivos por envio
    # app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # limite total da requisição (opcional)
    base_dir = Path(__file__).parent.resolve()
    paths = Paths(
        BASE_DIR=base_dir,
        UPLOAD_DIR=base_dir / "uploads",
        OUTPUT_DIR=base_dir / "outputs",
        CACHE_DIR=base_dir / "cache",
    )
    ensure_dirs(paths.UPLOAD_DIR, paths.OUTPUT_DIR, paths.CACHE_DIR)
    app.config["APP_CFG"] = Appconfig(paths=paths)

    # Blueprints sob o mesmo prefixo
    app.register_blueprint(Main.bp,                       url_prefix=f"{BASE_PREFIX}/")
    app.register_blueprint(ComparadorFretes.bp,               url_prefix=f"{BASE_PREFIX}/fatura")
    app.register_blueprint(HistoricoDocs.bp,       url_prefix=f"{BASE_PREFIX}/historico")
    app.register_blueprint(FormatadorTabelasFrete.bp,   url_prefix=f"{BASE_PREFIX}/formatar-acordos")

    # Health no prefixo (facilita teste via Nginx)
    @app.get(f"{BASE_PREFIX}/healthz")
    def healthz():
        return {"status": "ok"}, 200

    return app

if __name__ == "__main__":
    app = create_app()
    print(f"Acesse: http://127.0.0.1:{os.environ.get('PORT', 9008)}{BASE_PREFIX}")
    app.run(host="127.0.0.1", port=int(os.environ.get("PORT", 9008)), debug=True)
    
