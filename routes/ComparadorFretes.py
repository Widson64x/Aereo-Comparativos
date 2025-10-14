# C:\Programs\Aéreo-Comparativos\Routes\ComparadorFretes.py
from __future__ import annotations

import re
import json
import uuid
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Tuple, Optional

import pandas as pd
from flask import (
    Blueprint, render_template, request, redirect, url_for,
    send_file, flash, current_app
)

from Config import Appconfig
from Utils.Files import ensure_dirs, allowed_file

# Importa fill_numeric_nans_with_zero do novo local (Utils.DataFrame_Helpers)
from Utils.DataFrame_Helpers import fill_numeric_nans_with_zero

# Importa o extrator
from Repositories.Repositorio_FaturaLatam import extract_invoice_table as extract_invoice_table_latam
from Services.Latam.ComparativoLatam import LatamFreightComparer 
from Services.Latam.Latam_Metrics import LatamMetricsCalculator

bp = Blueprint("fatura", __name__, template_folder="../Templates")

# Dicionário de Serviços para centralizar a lógica por companhia
COMPARISON_SERVICES = {
    'LATAM': {
        'extractor': extract_invoice_table_latam,
        'comparator': LatamFreightComparer, # Classe armazenada
    },
    # 'AZUL': { # Futuramente, você adicionará a lógica da AZUL aqui
    #     'extractor': extract_invoice_table_azul,
    #     'comparator': compare_fretes_azul,
    # }
}

# ---------------- Helpers ----------------

def _now_stamp() -> str:
    return datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d_%H-%M-%S")

def _get_info_from_name(filename: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extrai a companhia e o ID do nome do arquivo.
    Formato esperado: ..._COMPANHIA_id123.pdf
    Formato legado: ..._id123.pdf (assume LATAM)
    """
    # Tenta o novo formato
    m_new = re.search(r'_([A-Z]+)_([a-f0-9]{12})\.pdf$', filename)
    if m_new:
        return m_new.group(1).upper(), m_new.group(2)
    # Tenta o formato legado
    m_legacy = re.search(r'_([a-f0-9]{12})\.pdf$', filename)
    if m_legacy:
        return 'LATAM', m_legacy.group(1) # Assume LATAM para arquivos antigos
    return None, None

def _file_size_mb(filestorage) -> float:
    stream = filestorage.stream
    pos = stream.tell()
    try:
        stream.seek(0, 2)
        size_bytes = stream.tell()
        stream.seek(pos)
        return size_bytes / (1024 * 1024)
    except Exception:
        return 0

def _process_and_cache_pdf(pdf_path: Path, file_id: str, company: str, source_name: str | None = None) -> pd.DataFrame | None:
    app_cfg: Appconfig = current_app.config["APP_CFG"]
    service = COMPARISON_SERVICES.get(company)
    if not service or 'extractor' not in service:
        flash(f"Companhia '{company}' não configurada para extração.")
        return None
    
    extractor_func = service['extractor']
    try:
        df_pdf = extractor_func(str(pdf_path))
        if df_pdf is None or df_pdf.empty:
            flash(f"Nenhuma tabela encontrada em: {pdf_path.name} (Extrator: {company}).")
            return None
        if "__source_pdf" not in df_pdf.columns:
            df_pdf["__source_pdf"] = source_name or pdf_path.name
        
        cache_path = app_cfg.paths.CACHE_DIR / f"{file_id}.feather"
        df_pdf.to_feather(cache_path)
        return df_pdf
    except Exception as e:
        flash(f"Erro ao processar {pdf_path.name}: {e}")
        return None

def _save_batch_manifest(batch_id: str, items: list[dict], company: str) -> Path:
    app_cfg: Appconfig = current_app.config["APP_CFG"]
    manifest_path = app_cfg.paths.CACHE_DIR / f"batch_{batch_id}.json"
    manifest_data = {"batch_id": batch_id, "company": company, "items": items}
    manifest_path.write_text(json.dumps(manifest_data, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest_path

def _load_batch_manifest(batch_id: str) -> dict | None:
    app_cfg: Appconfig = current_app.config["APP_CFG"]
    manifest_path = app_cfg.paths.CACHE_DIR / f"batch_{batch_id}.json"
    if not manifest_path.exists(): return None
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return None

def _load_batch_df(batch_id: str) -> pd.DataFrame | None:
    app_cfg: Appconfig = current_app.config["APP_CFG"]
    paths = app_cfg.paths
    batch_feather = paths.CACHE_DIR / f"batch_{batch_id}.feather"

    if batch_feather.exists():
        try:
            return pd.read_feather(batch_feather)
        except Exception as e:
            flash(f"Erro ao ler o cache consolidado: {e}")

    manifest = _load_batch_manifest(batch_id)
    if not manifest:
        flash("Manifesto do batch não encontrado.")
        return None
    
    items = manifest.get("items", [])
    dfs: list[pd.DataFrame] = []
    for it in items:
        file_id = it.get("file_id")
        fname = it.get("filename")
        if not file_id: continue
        
        file_feather = paths.CACHE_DIR / f"{file_id}.feather"
        if file_feather.exists():
            df = pd.read_feather(file_feather)
            if "__source_pdf" not in df.columns:
                df["__source_pdf"] = fname or f"{file_id}.pdf"
            dfs.append(df)
            
    if not dfs:
        flash("Nenhum cache para reconstruir o batch.")
        return None
        
    df_all = pd.concat(dfs, ignore_index=True)
    df_all.to_feather(batch_feather)
    return df_all

# ---------------- Hooks ----------------

@bp.before_app_request
def _ensure_dirs():
    app_cfg: Appconfig = current_app.config.get("APP_CFG")
    if app_cfg:
        ensure_dirs(app_cfg.paths.UPLOAD_DIR, app_cfg.paths.OUTPUT_DIR, app_cfg.paths.CACHE_DIR)

# ---------------- Views ----------------

@bp.get("/")
def tool_home():
    app_cfg: Appconfig = current_app.config["APP_CFG"]
    try:
        # Pega todos os PDFs para o JavaScript filtrar no frontend
        recent_uploads = sorted(app_cfg.paths.UPLOAD_DIR.glob("*.pdf"), key=lambda p: p.stat().st_mtime, reverse=True)
        recent_files = [p.name for p in recent_uploads[:500]] # Aumentado o limite para o filtro
    except FileNotFoundError:
        recent_files = []

    max_mb = int(current_app.config.get("MAX_PDF_UPLOAD_MB", 20))
    max_files = int(current_app.config.get("MAX_PDF_UPLOAD_COUNT", 10))

    return render_template(
        "Tools/ImportarFrete.html",
        recent_files=recent_files,
        limits={"max_mb": max_mb, "max_files": max_files}
    )

@bp.post("/process-pdfs")
def process_pdfs():
    app_cfg: Appconfig = current_app.config["APP_CFG"]
    paths = app_cfg.paths
    
    company = request.form.get("company", "").upper()
    if company not in COMPARISON_SERVICES:
        flash(f"Companhia '{company}' não está disponível.")
        return redirect(url_for("fatura.tool_home"))

    files = request.files.getlist("pdf_files")
    if not files or all(not f.filename for f in files):
        flash("Envie ao menos um PDF.")
        return redirect(url_for("fatura.tool_home"))

    max_mb = int(current_app.config.get("MAX_PDF_UPLOAD_MB", 20))
    max_files = int(current_app.config.get("MAX_PDF_UPLOAD_COUNT", 10))

    if len(files) > max_files:
        flash(f"Limite de {max_files} arquivos excedido.")
        return redirect(url_for("fatura.tool_home"))

    batch_id = uuid.uuid4().hex[:12]
    dfs, items = [], []

    for fil in files:
        fname = (fil.filename or "").strip()
        if not fname or not allowed_file(fname, {".pdf"}): continue
        if _file_size_mb(fil) > max_mb:
            flash(f"{fname} excede o limite de {max_mb} MB.")
            continue

        file_id = uuid.uuid4().hex[:12]
        ts = _now_stamp()
        # Salva o arquivo com o nome da companhia
        pdf_path = paths.UPLOAD_DIR / f"{ts}_{company}_{file_id}.pdf"
        try:
            fil.save(str(pdf_path))
        except Exception as e:
            flash(f"Falha ao salvar {fname}: {e}")
            continue

        df = _process_and_cache_pdf(pdf_path, file_id, company, source_name=fname)
        if df is not None and not df.empty:
            dfs.append(df)
            items.append({"file_id": file_id, "filename": pdf_path.name})

    if not dfs:
        flash("Nenhum PDF válido foi processado.")
        return redirect(url_for("fatura.tool_home"))

    df_all = pd.concat(dfs, ignore_index=True)
    df_all.to_feather(paths.CACHE_DIR / f"batch_{batch_id}.feather")
    _save_batch_manifest(batch_id, items, company)

    return redirect(url_for("fatura.compare_batch_page", batch_id=batch_id))

@bp.post("/use-existing-pdfs")
def use_existing_pdfs():
    app_cfg: Appconfig = current_app.config["APP_CFG"]
    paths = app_cfg.paths

    company = request.form.get("company", "").upper()
    if company not in COMPARISON_SERVICES:
        flash(f"Companhia '{company}' é inválida.")
        return redirect(url_for("fatura.tool_home"))

    selected_files = request.form.getlist("existing_pdfs")
    if not selected_files:
        flash("Selecione ao menos um PDF do histórico.")
        return redirect(url_for("fatura.tool_home"))

    batch_id = uuid.uuid4().hex[:12]
    dfs, items = [], []

    for filename in selected_files:
        pdf_company, file_id = _get_info_from_name(filename)
        
        # Valida se o arquivo pertence à companhia selecionada
        if not file_id or pdf_company != company:
            flash(f"Arquivo '{filename}' ignorado por não pertencer à companhia {company}.")
            continue

        cache_path = paths.CACHE_DIR / f"{file_id}.feather"
        if cache_path.exists():
            try:
                df = pd.read_feather(cache_path)
                if "__source_pdf" not in df.columns: df["__source_pdf"] = filename
                dfs.append(df)
                items.append({"file_id": file_id, "filename": filename})
                continue
            except Exception as e:
                flash(f"Cache de {filename} corrompido, reprocessando: {e}")

        pdf_path = paths.UPLOAD_DIR / filename
        if not pdf_path.exists():
            flash(f"Arquivo '{filename}' não encontrado.")
            continue

        df = _process_and_cache_pdf(pdf_path, file_id, company, source_name=filename)
        if df is not None and not df.empty:
            dfs.append(df)
            items.append({"file_id": file_id, "filename": filename})

    if not dfs:
        flash("Nenhum PDF válido selecionado para o batch.")
        return redirect(url_for("fatura.tool_home"))

    df_all = pd.concat(dfs, ignore_index=True)
    df_all.to_feather(paths.CACHE_DIR / f"batch_{batch_id}.feather")
    _save_batch_manifest(batch_id, items, company)

    return redirect(url_for("fatura.compare_batch_page", batch_id=batch_id))

@bp.route("/compare-batch/<batch_id>", methods=["GET", "POST"])
def compare_batch_page(batch_id: str):
    app_cfg: Appconfig = current_app.config["APP_CFG"]
    paths = app_cfg.paths

    manifest = _load_batch_manifest(batch_id)
    if not manifest:
        flash("Sessão inválida. Envie os arquivos novamente.")
        return redirect(url_for("fatura.tool_home"))
    
    company = manifest.get("company")
    service = COMPARISON_SERVICES.get(company)

    if not service or 'comparator' not in service:
        flash(f"Companhia '{company}' não é válida para comparação.")
        return redirect(url_for("fatura.tool_home"))
        
    df_base = _load_batch_df(batch_id)
    if df_base is None or df_base.empty:
        flash("Batch vazio. Envie os PDFs novamente.")
        return redirect(url_for("fatura.tool_home"))

    if request.method == "POST":
        acordos_file = request.files.get("acordos_file")
        if not acordos_file or not acordos_file.filename or not allowed_file(acordos_file.filename, {".xlsx"}):
            flash("Envie a planilha de Tabelas (.xlsx).")
            return redirect(url_for("fatura.compare_batch_page", batch_id=batch_id))
        
        try:
            ts = _now_stamp()
            acordos_path = paths.UPLOAD_DIR / f"{ts}_batch-{batch_id}_acordos.xlsx"
            acordos_file.save(str(acordos_path))

            # === MUDANÇA CHAVE AQUI: INSTANCIAÇÃO E CHAMADA DO MÉTODO ===
            
            # 1. Pega a CLASSE do dicionário de serviços
            ComparatorClass = service['comparator']
            
            # 2. Instancia a CLASSE, passando a configuração (app_cfg)
            comparer_instance = ComparatorClass(app_cfg)

            # 3. Chama o MÉTODO compare_fretes() da instância
            df_export, df_display = comparer_instance.compare_fretes(df_base, str(acordos_path))

            # ===========================================================
            # 1. Calcula as métricas usando o df_export
            metrics_calculator = LatamMetricsCalculator(df_export)
            metrics = metrics_calculator.calculate_metrics()
            # (Você pode passar 'metrics' para o template se quiser exibir os KPIs)

            # 2. Salva os arquivos
            # Nome do arquivo de saída agora inclui a companhia
            out_base = f"{ts}_batch-{batch_id}_{company}_comparativo"
            out_xlsx_path = paths.OUTPUT_DIR / f"{out_base}.xlsx"
            out_xlsx_zeros_path = paths.OUTPUT_DIR / f"{out_base}.zeros.xlsx"
            
            # Salva os arquivos de resultado...
            df_export.to_excel(out_xlsx_path, index=False)
            # fill_numeric_nans_with_zero é importada e usada normalmente
            fill_numeric_nans_with_zero(df_export).to_excel(out_xlsx_zeros_path, index=False)
            # (O código do ExcelWriter pode ser adicionado de volta se formatação for crucial)

            return render_template(
                "Tools/AnaliseFrete.html",
                batch_id=batch_id,
                company_name=company, 
                table_html=df_display.to_html(classes="table table-sm table-hover", index=False, justify="left", na_rep="-"),
                rows=len(df_display),
                metrics=metrics,
                download_url=url_for("fatura.download_batch", batch_id=batch_id)
            )
        except Exception as e:
            flash(f"Erro ao processar a planilha: {e}")
            return redirect(url_for("fatura.compare_batch_page", batch_id=batch_id))

    # GET request
    return render_template(
        "Tools/AnaliseFrete.html",
        batch_id=batch_id,
        company_name=company, 
        table_html=df_base.head(100).to_html(classes="table table-sm table-hover", index=False, justify="left", na_rep="-"),
        rows=len(df_base),
        download_url=None,
        metrics=None  # <--- ADICIONE ESTA LINHA
    )

@bp.get("/download/batch/<batch_id>")
def download_batch(batch_id: str):
    app_cfg: Appconfig = current_app.config["APP_CFG"]
    paths = app_cfg.paths

    manifest = _load_batch_manifest(batch_id)
    if not manifest or "company" not in manifest:
        flash("Não foi possível encontrar a companhia para este batch.")
        return redirect(url_for("fatura.tool_home"))
        
    company = manifest["company"]
    fmt = (request.args.get("format") or "xlsx").lower().strip()
    use_zeros = (request.args.get("zeros") or "").lower() in {"1", "true"}
    suffix = ".zeros" if use_zeros else ""
    ext = "csv" if fmt == "csv" else "xlsx"

    # Padrão de busca atualizado com a companhia
    pattern = f"*_batch-{batch_id}_{company}_comparativo{suffix}.{ext}"
    matches = sorted(paths.OUTPUT_DIR.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)

    if matches:
        return send_file(matches[0], as_attachment=True)

    flash("Arquivo final não encontrado. Refaça o comparativo.")
    return redirect(url_for("fatura.tool_home"))

# ----------- Rotas Legadas (mantidas por compatibilidade) -----------
# Estas rotas agora são apenas redirecionamentos para a nova lógica de batch
# Elas não precisam de um seletor de companhia, pois criam um batch de 1 item

@bp.post("/process-pdf")
def process_pdf():
    # Esta rota agora simplesmente empacota um único arquivo em um batch
    app_cfg: Appconfig = current_app.config["APP_CFG"]
    file = request.files.get("pdf_file")
    if not file or not file.filename or not allowed_file(file.filename, {".pdf"}):
        flash("Envie um arquivo PDF válido.")
        return redirect(url_for("fatura.tool_home"))
    
    # Assume LATAM por padrão, já que não há seletor nesta rota legada
    company = 'LATAM'
    file_id = uuid.uuid4().hex[:12]
    pdf_path = app_cfg.paths.UPLOAD_DIR / f"{_now_stamp()}_{company}_{file_id}.pdf"
    file.save(str(pdf_path))

    df = _process_and_cache_pdf(pdf_path, file_id, company, source_name=file.filename)
    if df is None or df.empty:
        return redirect(url_for("fatura.tool_home"))

    batch_id = uuid.uuid4().hex[:12]
    df.to_feather(app_cfg.paths.CACHE_DIR / f"batch_{batch_id}.feather")
    _save_batch_manifest(batch_id, [{"file_id": file_id, "filename": pdf_path.name}], company)
    return redirect(url_for("fatura.compare_batch_page", batch_id=batch_id))


@bp.post("/use-existing-pdf")
def use_existing_pdf():
    # Esta rota usa um PDF existente para criar um batch de 1 item
    app_cfg: Appconfig = current_app.config["APP_CFG"]
    filename = request.form.get("existing_pdf")
    if not filename:
        flash("Selecione um PDF do histórico.")
        return redirect(url_for("fatura.tool_home"))

    company, file_id = _get_info_from_name(filename)
    if not file_id:
        flash(f"ID inválido em '{filename}'.")
        return redirect(url_for("fatura.tool_home"))

    # Reprocessa se necessário
    pdf_path = app_cfg.paths.UPLOAD_DIR / filename
    df = _process_and_cache_pdf(pdf_path, file_id, company, source_name=filename)
    if df is None or df.empty:
        return redirect(url_for("fatura.tool_home"))

    batch_id = uuid.uuid4().hex[:12]
    df.to_feather(app_cfg.paths.CACHE_DIR / f"batch_{batch_id}.feather")
    _save_batch_manifest(batch_id, [{"file_id": file_id, "filename": filename}], company)
    return redirect(url_for("fatura.compare_batch_page", batch_id=batch_id))