# Repositories/Repositorio_FaturaLatam.py
from __future__ import annotations

import re
from typing import List
from pathlib import Path

import numpy as np
import pandas as pd

# Tenta usar Camelot; se não rolar, usa pdfplumber
try:
    import camelot
    HAS_CAMELOT = True
except Exception:
    HAS_CAMELOT = False

import pdfplumber

# Cabeçalhos (alvo) padronizados
TARGET_COLS = [
    "Tipo_Serviço", "Origem", "Data", "Destino", "Vlr Frete", "Outras Taxas",
    "Peso Taxado", "Vlr Total", "Numero Fiscal", "Documento", "Vlr Advalorem",
    "Tipo de Cte",
]

# Aliases para mapear variações de nomes de colunas
HEADER_ALIASES = {
    r"^origem$": "Origem", r"^data$": "Data", r"^destino$": "Destino",
    r"^v(ir|lr)\s*frete$": "Vlr Frete", r"^outras\s*taxas$": "Outras Taxas",
    r"^peso\s*taxado$": "Peso Taxado", r"^peso$": "Peso", r"^taxado$": "Taxado",
    r"^v(ir|lr)\s*total$": "Vlr Total", r"^(numero|n[úu]mero)\s*fiscal$": "Numero Fiscal",
    r"^documento$": "Documento", r"^v(ir|lr)\s*advalorem$": "Vlr Advalorem",
    r"^tipo\s*de\s*cte$": "Tipo de Cte",
}

def _normalize_header(cols: List[str]) -> List[str]:
    norm = []
    for c in cols:
        c0 = re.sub(r"\s+", " ", str(c or "")).strip()
        replacements = {
            "TaxasPeso": "Taxas|Peso", "TotalNumero": "Total|Numero",
            "FiscalDocumento": "Fiscal|Documento", "AdvaloremTipo": "Advalorem|Tipo",
            "Destino Peso": "Destino|Peso", "Advalorem Outras": "Advalorem|Outras"
        }
        for old, new in replacements.items():
            c0 = c0.replace(old, new)
        parts = [p.strip() for p in re.split(r"\|", c0)]
        norm.extend(parts)

    out = []
    for c in norm:
        key = c.lower()
        mapped = next((tgt for rx, tgt in HEADER_ALIASES.items() if re.search(rx, key, flags=re.I)), c)
        out.append(mapped)
    return out

def _coerce_numbers(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    df = df.copy()
    for col in cols:
        if col in df.columns:
            s = df[col].astype(str).str.replace(",", "", regex=False)
            df[col] = pd.to_numeric(s, errors="coerce")
    return df

def _clean_documento(df: pd.DataFrame) -> pd.DataFrame:
    if "Documento" in df.columns:
        df["Documento"] = (
            df["Documento"].astype(str)
            .str.replace(r"[^\d\-]", "", regex=True).str.replace("-", "", regex=False)
        )
    return df

def _drop_noise_rows(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if df.empty: return df
    mask_noise = df.iloc[:, 0].astype(str).str.contains(
        r"Sub\s*Total|Copyright|Total", case=False, regex=True, na=False
    )
    df = df[~mask_noise]
    if "Documento" in df.columns:
        is_data_row = df["Documento"].astype(str).str.contains(r"\d", regex=True, na=False)
        is_service_header = df.iloc[:, 0].astype(str).str.contains(
            r"RESERVADO|ESTANDAR|VELOZ", case=False, na=False
        )
        df = df[is_data_row | is_service_header]
    return df.dropna(how="all")

def _add_service_type_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    service_keywords = ['RESERVADO MEDS', 'ESTANDAR 10 BASICO', 'ESTANDAR 2 BASICO', 'ESTANDAR 2 MEDS', 'VELOZ']
    pattern = '|'.join(service_keywords)
    is_service_header = df.iloc[:, 0].astype(str).str.contains(pattern, case=False, na=False)
    df['Tipo_Serviço'] = df.iloc[:, 0].where(is_service_header)
    df['Tipo_Serviço'] = df['Tipo_Serviço'].ffill().fillna('Não especificado').str.strip()
    df = df[~is_service_header].reset_index(drop=True)
    return df

def _add_frete_peso(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "Vlr Frete" in df.columns and "Peso Taxado" in df.columns:
        vlr_frete = pd.to_numeric(df["Vlr Frete"], errors='coerce')
        peso_taxado = pd.to_numeric(df["Peso Taxado"], errors='coerce')
        df["Frete_Peso"] = np.where(peso_taxado.notna() & (peso_taxado != 0), vlr_frete / peso_taxado, pd.NA)
    return df

def _final_column_order(df: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in TARGET_COLS if c in df.columns]
    extras = [c for c in df.columns if c not in cols]
    return df[cols + extras]

def _combine_split_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if 'Peso' in df.columns and 'Taxado' in df.columns:
        df['Taxado'].fillna(df['Peso'], inplace=True)
        df = df.rename(columns={'Taxado': 'Peso Taxado'}).drop(columns=['Peso'])
    return df

# ---- reparo linhas mescladas ----
def _fix_merged_data_rows(df: pd.DataFrame) -> pd.DataFrame:
    splitter = re.compile(r'\s{2,}')
    repaired_rows = []
    for _, row in df.iterrows():
        is_merged = (pd.isna(row.get('Origem')) or pd.isna(row.get('Documento'))) and \
                    isinstance(row.iloc[0], str) and len(row.iloc[0]) > 40
        if is_merged:
            parts = splitter.split(row.iloc[0].strip())
            if len(parts) >= 9:
                num_cols_to_fill = min(len(parts), len(df.columns))
                row.iloc[:num_cols_to_fill] = parts[:num_cols_to_fill]
        repaired_rows.append(row)
    return pd.DataFrame(repaired_rows).reset_index(drop=True)

def _extract_tables_from_pdf(pdf_path: str, use_camelot: bool) -> List[pd.DataFrame]:
    if use_camelot and HAS_CAMELOT:
        try:
            tables = camelot.read_pdf(pdf_path, pages="2-end", flavor="stream", edge_tol=500)
            if tables.n > 0: 
                return [t.df for t in tables]
        except Exception:
            pass

    frames = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            if i == 0: 
                continue
            tables = page.extract_tables()
            for tb in tables or []:
                if tb: frames.append(pd.DataFrame(tb))
    return frames

def extract_invoice_table(pdf_path: str) -> pd.DataFrame:
    raw_tables = _extract_tables_from_pdf(pdf_path, use_camelot=True)
    if not raw_tables: raw_tables = _extract_tables_from_pdf(pdf_path, use_camelot=False)
    if not raw_tables: return pd.DataFrame()

    processed_frames = []
    for table in raw_tables:
        if table.empty: continue
        header = _normalize_header(table.iloc[0].tolist())
        body = table.copy()
        if len(header) < len(body.columns):
            header.extend([f'extra_{i}' for i in range(len(body.columns) - len(header))])
        body.columns = header[:len(body.columns)]
        processed_frames.append(body)

    df = pd.concat(processed_frames, ignore_index=True)

    # reparo antes de processar
    df = _fix_merged_data_rows(df)

    df = _combine_split_columns(df)
    df = _drop_noise_rows(df)
    df = _add_service_type_and_clean(df)
    df = _coerce_numbers(df, ["Vlr Frete", "Outras Taxas", "Peso Taxado", "Vlr Total", "Vlr Advalorem"])
    df = _clean_documento(df)
    df = _add_frete_peso(df)
    df = _final_column_order(df)

    return df.dropna(how="all").reset_index(drop=True)
