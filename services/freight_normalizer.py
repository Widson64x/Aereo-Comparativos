# services/freight_normalizer.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
import datetime as _dt
import unicodedata
import re

# Mapa de nomes de colunas → padrão
# (mantivemos curto; nomes “exatos” são resolvidos por normalização + regex)
COLUMN_MAP = {
    "origem": "Origem",
    "cidade origem": "Origem",
    "orig": "Origem",
    "o": "Origem",
    "destino": "Destino",
    "cidade destino": "Destino",
    "dest": "Destino",
    "d": "Destino",
    "valor": "Valor_Frete",
    "vlr": "Valor_Frete",
    "vlr frete": "Valor_Frete",
    "vlr_frete": "Valor_Frete",
    "frete": "Valor_Frete",
    "preco/kg": "Valor_Frete",
    "preço/kg": "Valor_Frete",
    "valor_kg": "Valor_Frete",
    "r$/kg": "Valor_Frete",
    "tratamento": "Tratamento",
    "tratamento cadastrado em sistema conferido": "Tratamento",
}

# Conjunto canônico de serviços
from services.service_types import normalize_tipo_servico, ALLOWED  # novo import


# -----------------------
# Helpers de normalização
# -----------------------
def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def _canon(s: str) -> str:
    s = _strip_accents(s).lower().strip()
    s = re.sub(r"[\s\-_/]+", " ", s)
    return s

def _canon_col(col: str) -> Optional[str]:
    base = _canon(col)
    return COLUMN_MAP.get(base, None)

DATE_RE_1 = re.compile(r"^\s*(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})\s*$")  # dd/mm/aaaa ou dd-mm-aaaa
DATE_RE_2 = re.compile(r"^\s*(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})\s*$")    # aaaa-mm-dd

def _date_to_rate(day: int, month: int) -> float | None:
    """
    Converte 'dia/mês' em tarifa decimal:
    04/02 -> 4.2   |  15/09 -> 15.9   |  04/11 -> 4.11
    (sem zero à esquerda no mês)
    """
    if 1 <= month <= 12 and 1 <= day <= 31:
        # sem zero à esquerda no mês
        return float(f"{int(day)}.{int(month)}")
    return None

def _safe_float(x) -> float | None:
    """
    Converte valores diversos para float:
    - números com vírgula/ponto e 'R$'
    - datas (Timestamp/datetime ou strings 'dd/mm/aaaa' / 'aaaa-mm-dd') → dia.mês
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None

    # 1) Timestamp/datetime ⇒ dia.mês
    if isinstance(x, (pd.Timestamp, _dt.datetime, _dt.date)):
        d = int(getattr(x, "day", x.day))
        m = int(getattr(x, "month", x.month))
        return _date_to_rate(d, m)

    s = str(x).strip()
    if s == "" or s in {"-", "—"}:
        return None

    # 2) Strings com formato de data
    m1 = DATE_RE_1.match(s)
    if m1:
        d, m, _y = int(m1.group(1)), int(m1.group(2)), int(m1.group(3))
        return _date_to_rate(d, m)
    m2 = DATE_RE_2.match(s)
    if m2:
        _y, m, d = int(m2.group(1)), int(m2.group(2)), int(m2.group(3))
        return _date_to_rate(d, m)

    # 3) Números comuns (com R$, ponto/vírgula, milhar)
    s = s.replace("R$", "").replace(" ", "")
    const_has_comma = "," in s
    const_has_dot   = "." in s
    if const_has_comma and const_has_dot:
        # último separador define decimal; removemos o outro como milhar
        last_comma = s.rfind(",")
        last_dot   = s.rfind(".")
        dec = "," if last_comma > last_dot else "."
        thou = "." if dec == "," else ","
        s = s.replace(thou, "")
        if dec == ",":
            s = s.replace(",", ".")
    elif const_has_comma:
        s = s.replace(".", "")
        s = s.replace(",", ".")
    else:
        s = s.replace(",", "")

    try:
        return float(s)
    except Exception:
        return None

def _normalize_place(val: str | None) -> str | None:
    if not val or str(val).strip() == "":
        return None
    s = str(val).upper().strip()
    s = _strip_accents(s)
    s = re.sub(r"\s*[-–/\\]\s*", "-", s)  # normaliza separadores para '-'
    s = re.sub(r"\s+", " ", s)
    return s

def _coalesce_cols(df: pd.DataFrame, candidates: List[str], target: str):
    for c in candidates:
        if c in df.columns:
            if target not in df.columns:
                df[target] = df[c]
            else:
                df[target] = df[target].fillna(df[c])
    return df

def read_all_sheets(path: Path) -> Dict[str, pd.DataFrame]:
    return pd.read_excel(path, sheet_name=None)


# -----------------------
# Regras específicas pedidas
# -----------------------
def _is_reservado_bases(name: str) -> bool:
    n = _canon(name)
    # casa "Reservado LUFT - Bases" com ou sem variações leves
    return n.startswith("reservado luft") and "bases" in n

_TARIFA_BASE_PATS = [
    r"^tarifas?\s+com\s+reajuste.*mar[cç]o.*2025.*",   # Tarifas com Reajuste Março 2025 ...
    r"^reajuste.*mar[cç]o.*2025.*",
    r"^tarifas?.*reajuste.*",
    r"reajuste",
]

def _tarifa_patterns_for(prefer_code: Optional[str]) -> List[re.Pattern]:
    pats: List[str] = []
    code = (prefer_code or "").strip().upper().replace(" ", "")
    if code:
        # Coluna mais específica primeiro (p.ex. ... ST2MD)
        pats = [
            rf"^tarifas?\s+com\s+reajuste.*{re.escape(code)}.*",
            rf"^reajuste.*{re.escape(code)}.*",
            rf"tarifas?.*{re.escape(code)}.*",
            rf".*{re.escape(code)}.*",
        ]
        if code == "ST2MD":
            pats = [r"reajuste.*st\s*2\s*md"] + pats
    # depois as genéricas
    pats += _TARIFA_BASE_PATS
    return [re.compile(p, flags=re.I) for p in pats]

def _find_first_matching_column(df: pd.DataFrame, patterns: List[re.Pattern]) -> List[str]:
    """Retorna colunas que casam com a lista de padrões (ordem preservada, sem repetir)."""
    found: List[str] = []
    for rx in patterns:
        for c in df.columns:
            if rx.search(str(c)):
                if c not in found:
                    found.append(c)
    return found


# -----------------------------------
# Processador da aba "Reservado ... "
# -----------------------------------
def _process_reservado_bases(raw: pd.DataFrame, sheet_name: str, file_name: str) -> pd.DataFrame:
    """
    A aba é uma matriz: primeira linha = cabeçalho (origens), primeira coluna = destino.
    Valor = interseção Origem x Destino.
    Tipo_Servico fixo: RESMD (base principal).
    """
    if raw is None or raw.empty:
        return pd.DataFrame()

    header = list(raw.iloc[0].tolist())
    if not header:
        return pd.DataFrame()
    header[0] = "Destino"
    df = raw.copy()
    df.columns = header
    df = df.iloc[1:, :].reset_index(drop=True)

    if "Destino" not in df.columns or len(df.columns) <= 1:
        return pd.DataFrame()

    origens = [c for c in df.columns if c != "Destino"]
    tidy = df.melt(id_vars=["Destino"], value_vars=origens, var_name="Origem", value_name="Valor_Frete")

    tidy["Origem"] = tidy["Origem"].astype(str).map(_normalize_place)
    tidy["Destino"] = tidy["Destino"].astype(str).map(_normalize_place)
    tidy["Valor_Frete"] = tidy["Valor_Frete"].map(_safe_float)

    # Limpa vazios
    tidy = tidy.dropna(subset=["Origem", "Destino", "Valor_Frete"]).copy()
    if tidy.empty:
        return tidy

    # Metadados
    tidy["Tipo_Servico"] = "RESMD"         # regra: Reservado -> RESMD
    tidy["Fonte_Arquivo"] = file_name
    tidy["Fonte_Aba"] = sheet_name
    tidy["Moeda"] = "BRL"
    tidy["Unidade_Valor"] = "R$/kg"
    tidy["Regra"] = "por_kg"

    # PRIORIDADE MÁXIMA para vencer duplicidades RESMD
    tidy["Source_Priority"] = 100

    order_cols = [
        "Origem", "Destino", "Tipo_Servico", "Valor_Frete",
        "Moeda", "Unidade_Valor", "Regra",
        "Peso_Min", "Peso_Max", "Vigencia_Inicio", "Vigencia_Fim",
        "Observacoes", "Fonte_Arquivo", "Fonte_Aba", "Source_Priority"
    ]
    for c in order_cols:
        if c not in tidy.columns:
            tidy[c] = None
    return tidy[order_cols].drop_duplicates()


# -----------------------
# Normalização principal
# -----------------------
def normalize_file(path: str | Path, ignore_first_sheet: bool = False) -> pd.DataFrame:
    path = Path(path)
    sheets = read_all_sheets(path)
    if ignore_first_sheet and sheets:
        first = list(sheets.keys())[0]
        sheets.pop(first, None)

    def _mask_notna(df: pd.DataFrame, col: str) -> pd.Series:
        return df[col].notna() if col in df.columns else pd.Series(False, index=df.index)

    frames: List[pd.DataFrame] = []

    for sheet_name, raw in sheets.items():
        if raw is None or raw.empty:
            continue

        # 0) Tratamento específico "Reservado LUFT - Bases" (matriz -> RESMD com prioridade 100)
        if _is_reservado_bases(sheet_name):
            df_res = _process_reservado_bases(raw, sheet_name, file_name=path.name)
            if not df_res.empty:
                # Garante prioridade 100 mesmo se a função não tiver setado
                if "Source_Priority" not in df_res.columns:
                    df_res["Source_Priority"] = 100
                frames.append(df_res)
            # Não reaproveita como aba normal
            continue

        # 1) Renomeia colunas para o padrão
        renamed = {}
        for c in raw.columns:
            std = _canon_col(str(c))
            if std:
                renamed[c] = std
        df = raw.rename(columns=renamed).copy()

        # 2) Coalesce sinônimos
        df = _coalesce_cols(df, ["Origem"], "Origem")
        df = _coalesce_cols(df, ["Destino"], "Destino")

        # 3) Abas normais exigem Origem/Destino
        if "Origem" not in df.columns or "Destino" not in df.columns:
            continue

        # 4) Determina/normaliza Tipo_Servico
        if "Tipo_Servico" not in df.columns and "Tratamento" in df.columns:
            df["Tipo_Servico"] = df["Tratamento"]
        if "Tipo_Servico" in df.columns:
            df["Tipo_Servico"] = df["Tipo_Servico"].astype(str).map(lambda x: normalize_tipo_servico(x))
        else:
            tip = normalize_tipo_servico(sheet_name)
            df["Tipo_Servico"] = tip if tip else None

        # 5) Normaliza nomes
        df["Origem"] = df["Origem"].astype(str).map(_normalize_place)
        df["Destino"] = df["Destino"].astype(str).map(_normalize_place)

        # 6) Valor_Frete + prioridade por linha
        if "Valor_Frete" in df.columns:
            df["Valor_Frete"] = df["Valor_Frete"].map(_safe_float)
            df["Source_Priority"] = 70  # valor explícito na planilha
        else:
            # Escolhe a tarifa por linha em função do código da própria linha
            def pick_tarifa_and_priority(row):
                code = row.get("Tipo_Servico")
                if pd.isna(code):
                    return (None, None)
                # tenta colunas específicas primeiro (com o código)
                cands = _find_first_matching_column(df, _tarifa_patterns_for(code))
                for c in cands:
                    v = _safe_float(row.get(c))
                    if v is not None:
                        name = str(c).upper().replace(" ", "")
                        prio = 80 if (isinstance(code, str) and code.upper().replace(" ", "") in name) else 60
                        return (v, prio)
                return (None, None)

            vals = df.apply(pick_tarifa_and_priority, axis=1, result_type="expand")
            df["Valor_Frete"] = vals[0]
            df["Source_Priority"] = vals[1]

        # 7) Fallback: ainda sem Tipo_Servico? tentar deduzir pelo nome de colunas
        if df["Tipo_Servico"].isna().any():
            for col in df.columns:
                guess = normalize_tipo_servico(col)
                if guess:
                    mask = df["Tipo_Servico"].isna()
                    if mask.any():
                        df.loc[mask, "Tipo_Servico"] = guess

        # 8) Mantém linhas válidas
        keep = _mask_notna(df, "Origem") & _mask_notna(df, "Destino") & _mask_notna(df, "Tipo_Servico")
        df = df[keep].copy()
        if df.empty:
            continue

        # 9) Filtra somente serviços canônicos (sem agrupar códigos distintos)
        df = df[df["Tipo_Servico"].isin(ALLOWED)]
        if df.empty:
            continue

        # 10) Mantém apenas linhas com valor
        df = df.dropna(subset=["Valor_Frete"]).copy()
        if df.empty:
            continue

        # 11) Metadados
        df["Moeda"] = "BRL"
        df["Unidade_Valor"] = "R$/kg"
        df["Regra"] = "por_kg"
        df["Fonte_Arquivo"] = path.name
        df["Fonte_Aba"] = sheet_name

        # 12) Colunas finais
        order_cols = [
            "Origem", "Destino", "Tipo_Servico", "Valor_Frete",
            "Moeda", "Unidade_Valor", "Regra",
            "Peso_Min", "Peso_Max", "Vigencia_Inicio", "Vigencia_Fim",
            "Observacoes", "Fonte_Arquivo", "Fonte_Aba", "Source_Priority"
        ]
        for c in order_cols:
            if c not in df.columns:
                df[c] = None
        df = df[order_cols].drop_duplicates()

        frames.append(df)

    if not frames:
        raise ValueError("Nenhuma aba com dados normalizáveis foi encontrada.")

    # Concatena tudo
    out = pd.concat(frames, ignore_index=True)

    # Segurança: garante coluna de prioridade
    if "Source_Priority" not in out.columns:
        out["Source_Priority"] = 0
    out["Source_Priority"] = out["Source_Priority"].fillna(0).astype(int)

    # Filtra canônicos, remove linhas sem valor
    out = out[out["Tipo_Servico"].isin(ALLOWED)].copy()
    out = out.dropna(subset=["Valor_Frete"]).copy()

    # 13) Deduplicação por rota+serviço, mantendo a MAIOR prioridade
    out = (
        out.sort_values(
            ["Origem", "Destino", "Tipo_Servico", "Source_Priority"],
            ascending=[True, True, True, False]
        )
        .drop_duplicates(subset=["Origem", "Destino", "Tipo_Servico"], keep="first")
        .copy()
    )

    # 14) Remove coluna interna de prioridade antes de devolver
    out = out.drop(columns=["Source_Priority"], errors="ignore")

    return out

def save_outputs(df: pd.DataFrame, outputs_dir: str | Path = "outputs", basename: str = "fretes_normalizados"):
    outputs_dir = Path(outputs_dir)
    outputs_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = outputs_dir / f"{basename}.parquet"
    csv_path = outputs_dir / f"{basename}.csv"
    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    return parquet_path, csv_path
