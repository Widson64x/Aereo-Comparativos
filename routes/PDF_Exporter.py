# routes/fatura_export.py
from __future__ import annotations

import re  # Importe o módulo de expressões regulares
import uuid
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
from flask import (Blueprint, render_template, request, redirect, url_for,
                   send_file, flash, current_app)

from config import AppConfig
from utils.files import ensure_dirs, allowed_file
from repositories.pdf_repository import extract_invoice_table
from services.comparison_tables_with_invoice import compare_fretes
from services.formatting import round_and_prepare

bp = Blueprint("fatura", __name__, template_folder="../templates")

# --- FUNÇÕES AUXILIARES ---

def _now_stamp() -> str:
    return datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d_%H-%M-%S")

def _get_file_id_from_name(filename: str) -> str | None:
    """Extrai o ID de um nome de arquivo como '2025-08-20_..._ID123.pdf'."""
    match = re.search(r'_([a-f0-9]{12})\.pdf$', filename)
    return match.group(1) if match else None

def _process_and_cache_pdf(pdf_path: Path, file_id: str) -> bool:
    """
    Função centralizada para extrair dados de um PDF e salvá-los em cache.
    Retorna True em sucesso, False em falha.
    """
    app_cfg: AppConfig = current_app.config["APP_CFG"]
    paths = app_cfg.paths
    try:
        df_pdf = extract_invoice_table(str(pdf_path))
        if df_pdf.empty:
            flash("Nenhuma tabela foi encontrada no PDF após a primeira página.")
            return False

        cache_path = paths.CACHE_DIR / f"{file_id}.feather"
        df_pdf.to_feather(cache_path)
        return True
    except Exception as e:
        flash(f"Ocorreu um erro ao processar o PDF: {e}")
        return False

# --- ROTAS ---

@bp.before_app_request
def _ensure_dirs():
    app_cfg: AppConfig = current_app.config.get("APP_CFG")
    if app_cfg:
        paths = app_cfg.paths
        ensure_dirs(paths.UPLOAD_DIR, paths.OUTPUT_DIR, paths.CACHE_DIR)

@bp.get("/")
def tool_home():
    """Exibe a página inicial com as opções e lista PDFs recentes."""
    app_cfg: AppConfig = current_app.config["APP_CFG"]
    paths = app_cfg.paths
    
    try:
        # Lista os PDFs na pasta de upload e ordena pelos mais recentes
        recent_uploads = sorted(
            paths.UPLOAD_DIR.glob("*.pdf"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )
        # Pega apenas os 20 mais recentes para não poluir a interface
        recent_files = [p.name for p in recent_uploads[:20]]
    except FileNotFoundError:
        recent_files = []

    return render_template("fatura_index.html", recent_files=recent_files)

@bp.post("/process-pdf")
def process_pdf():
    """Processa o upload de um NOVO arquivo PDF."""
    app_cfg: AppConfig = current_app.config["APP_CFG"]
    paths = app_cfg.paths

    if "pdf_file" not in request.files:
        flash("Por favor, envie um arquivo PDF.")
        return redirect(url_for("fatura.tool_home"))

    file = request.files["pdf_file"]
    if file.filename == "" or not allowed_file(file.filename, {".pdf"}):
        flash("Arquivo inválido. Envie um PDF.")
        return redirect(url_for("fatura.tool_home"))

    file_id = uuid.uuid4().hex[:12]
    ts = _now_stamp()
    pdf_path = paths.UPLOAD_DIR / f"{ts}_{file_id}.pdf"
    file.save(str(pdf_path))

    if _process_and_cache_pdf(pdf_path, file_id):
        return redirect(url_for("fatura.compare_page", file_id=file_id))
    
    return redirect(url_for("fatura.tool_home"))

@bp.post("/use-existing-pdf")
def use_existing_pdf():
    """Usa um PDF existente (do histórico) para pular para a comparação."""
    app_cfg: AppConfig = current_app.config["APP_CFG"]
    paths = app_cfg.paths

    filename = request.form.get("existing_pdf")
    if not filename:
        flash("Por favor, selecione um PDF do histórico.")
        return redirect(url_for("fatura.tool_home"))

    file_id = _get_file_id_from_name(filename)
    if not file_id:
        flash(f"Não foi possível extrair um ID válido do arquivo '{filename}'.")
        return redirect(url_for("fatura.tool_home"))

    # Verifica se os dados já estão em cache
    cache_path = paths.CACHE_DIR / f"{file_id}.feather"
    if cache_path.exists():
        return redirect(url_for("fatura.compare_page", file_id=file_id))

    # Se não estiver em cache, tenta processar o PDF do histórico
    pdf_path = paths.UPLOAD_DIR / filename
    if not pdf_path.exists():
        flash(f"Arquivo '{filename}' não encontrado nos uploads.")
        return redirect(url_for("fatura.tool_home"))
    
    flash(f"Cache não encontrado. Re-processando {filename}...")
    if _process_and_cache_pdf(pdf_path, file_id):
        return redirect(url_for("fatura.compare_page", file_id=file_id))

    return redirect(url_for("fatura.tool_home"))


# As rotas 'compare_page' e 'download_file' continuam iguais
@bp.route("/compare/<file_id>", methods=["GET", "POST"])
def compare_page(file_id: str):
    # ... (seu código existente aqui, sem alterações)
    app_cfg: AppConfig = current_app.config["APP_CFG"]
    paths = app_cfg.paths

    cache_path = paths.CACHE_DIR / f"{file_id}.feather"
    if not cache_path.exists():
        flash("Sessão expirada ou arquivo não encontrado. Por favor, envie o PDF novamente.")
        return redirect(url_for("fatura.tool_home"))

    df_base = pd.read_feather(cache_path)

    if request.method == "POST":
        if "acordos_file" not in request.files:
            flash("Por favor, envie a planilha de acordos (.xlsx).")
            return redirect(url_for("fatura.compare_page", file_id=file_id))

        acordos_file = request.files["acordos_file"]
        if acordos_file.filename == "" or not allowed_file(acordos_file.filename, {".xlsx"}):
            flash("Arquivo inválido. Envie um .xlsx.")
            return redirect(url_for("fatura.compare_page", file_id=file_id))

        try:
            ts = _now_stamp()
            acordos_path = paths.UPLOAD_DIR / f"{ts}_{file_id}_acordos.xlsx"
            acordos_file.save(str(acordos_path))

            df_final = compare_fretes(df_base, str(acordos_path), app_cfg)
            df_export, df_display = round_and_prepare(df_final, app_cfg.tuning.EPSILON)

            out_path = paths.OUTPUT_DIR / f"{ts}_{file_id}_comparativo.xlsx"
            try:
                with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
                    df_excel = df_export.copy()
                    if "Dif_%" in df_excel.columns:
                        df_excel["Dif_%"] = pd.to_numeric(df_excel["Dif_%"], errors="coerce") / 100.0
                    df_excel.to_excel(writer, index=False, sheet_name="Comparativo")

                    wb = writer.book
                    ws = writer.sheets["Comparativo"]
                    num_fmt = wb.add_format({'num_format': '#,##0.00'})
                    pct_fmt = wb.add_format({'num_format': '0.00%'})
                    col_idx = {col: i for i, col in enumerate(df_excel.columns)}

                    for name in ("Frete_Peso", "Frete_Acordo", "Diferenca", "Dif_abs"):
                        if name in col_idx:
                            ws.set_column(col_idx[name], col_idx[name], 12, num_fmt)
                    if "Dif_% " in col_idx:
                        ws.set_column(col_idx["Dif_% "], col_idx["Dif_% "], 12, pct_fmt)
                    elif "Dif_%" in col_idx:
                        ws.set_column(col_idx["Dif_%"], col_idx["Dif_%"], 12, pct_fmt)

                    for name, width in [
                        ("Tipo_Serviço", 20), ("Origem", 10), ("Destino", 10),
                        ("Documento", 18), ("Fonte_Tarifa", 30)
                    ]:
                        if name in col_idx:
                            ws.set_column(col_idx[name], col_idx[name], width)
            except Exception:
                df_export.to_excel(out_path, index=False)

            return render_template(
                "compare.html",
                file_id=file_id,
                table_html=df_display.to_html(
                    classes="table table-sm table-hover",
                    index=False, justify="left", na_rep="-"
                ),
                rows=len(df_display),
                download_url=url_for("fatura.download_file", file_id=file_id),
            )

        except Exception as e:
            flash(f"Ocorreu um erro ao processar a planilha de acordos: {e}")
            return redirect(url_for("fatura.compare_page", file_id=file_id))

    return render_template(
        "compare.html",
        file_id=file_id,
        table_html=df_base.head(100).to_html(
            classes="table table-sm table-hover",
            index=False, justify="left", na_rep="-"
        ),
        rows=len(df_base),
        download_url=None
    )

@bp.get("/download/<file_id>")
def download_file(file_id: str):
    app_cfg: AppConfig = current_app.config["APP_CFG"]
    paths = app_cfg.paths
    matches = sorted(
        paths.OUTPUT_DIR.glob(f"*_{file_id}_comparativo.xlsx"),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )
    if not matches:
        flash("Arquivo final não encontrado. Tente processar novamente.")
        return redirect(url_for("fatura.tool_home"))
    latest = matches[0]
    return send_file(latest, as_attachment=True, download_name=latest.name)