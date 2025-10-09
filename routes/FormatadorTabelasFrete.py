# routes/FormatadorTabelasFrete.py
from __future__ import annotations

from flask import Blueprint, render_template, request, flash, redirect, url_for, send_from_directory
from pathlib import Path
import pandas as pd

from Repositories.Repositorio_TabelasFretes import (
    save_uploaded_xlsx,
    normalize_and_persist,
    preview,
    OUTPUTS_DIR,
)

bp = Blueprint("formatador_acordos", __name__, url_prefix="/acordos")

@bp.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        file = request.files.get("arquivo") or request.files.get("acordos_file")
        if not file or file.filename == "":
            flash("Selecione um arquivo Excel de acordos (.xlsx).", "warning")
            return redirect(request.url)
        try:
            up_path = save_uploaded_xlsx(file, prefix="acordos")
            df, art = normalize_and_persist(up_path, ignore_first_sheet=False)

            # Gera XLSX irmão do CSV
            xlsx_path = Path(art.csv_path).with_suffix(".xlsx")
            try:
                with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
                    df.to_excel(writer, index=False, sheet_name="Fretes")
                    wb = writer.book
                    ws = writer.sheets["Fretes"]

                    if "Valor_Frete" in df.columns:
                        col_idx = list(df.columns).index("Valor_Frete") + 1  # 1-based
                        for cell in ws.iter_cols(min_col=col_idx, max_col=col_idx, min_row=2, max_row=ws.max_row):
                            for c in cell:
                                c.number_format = "0.00"  # exibe 4.20 em vez de 4.2
            except Exception as e:
                flash(f"Não foi possível gerar o XLSX: {e}", "warning")

            # Prévia
            prv = preview(df, n=500)
            table_html = pd.DataFrame(prv["rows"]).to_html(
                index=False, border=0, classes="table table-sm"
            ) if prv["rows"] else None

            download_csv_url  = url_for(".download_output", filename=Path(art.csv_path).name)
            download_xlsx_url = url_for(".download_output", filename=Path(xlsx_path).name) if xlsx_path.exists() else None

            return render_template(
                "Tools/ProcessarTabelaFrete.html",
                table_html=table_html,
                rows=len(df),
                preview_count=len(prv["rows"]),
                download_csv_url=download_csv_url,
                download_xlsx_url=download_xlsx_url,
            )
        except Exception as e:
            flash(f"Erro ao normalizar: {e}", "danger")

    # GET ou erro
    return render_template("Tools/ProcessarTabelaFrete.html", table_html=None)

@bp.route("/download/<path:filename>")
def download_output(filename: str):
    # Serve arquivos da pasta de outputs de acordos
    return send_from_directory(OUTPUTS_DIR, filename, as_attachment=True)
