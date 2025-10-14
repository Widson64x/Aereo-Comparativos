# Repositories/Repositorio_TabelasFretes.py
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional, List, Tuple
import pandas as pd
import numpy as np

from Utils.Parse import to_num, std_text, normalize_label, tokens, strip_accents

# ======== CONSTANTES / PADRÕES EXISTENTES ========
ORIGEM_PAT   = r"^\s*origem\s*$"
DESTINO_PAT  = r"^\s*destino\s*$"
TRATAMENTO_PATS = [
    r"^tratamento.*conferido(?:\s*\d{1,2}/\d{1,2}/\d{2,4})?$",
    r"^tratamento.*sistema.*conferido.*$",
]

def _find_col(df: pd.DataFrame, patterns) -> Optional[str]:
    """Procura a primeira coluna cujo nome (bruto ou normalizado) casa com os padrões."""
    pats = patterns if isinstance(patterns, (list, tuple)) else [patterns]
    rxs = [re.compile(p, flags=re.I) for p in pats]
    norm_map = {c: normalize_label(c) for c in df.columns}
    for c in df.columns:
        raw, norm = str(c), norm_map[c]
        for rx in rxs:
            if rx.search(raw) or rx.search(norm):
                return c
    return None

def _tarifa_patterns_for(code: Optional[str]) -> List[str]:
    """Gera padrões de busca para colunas de tarifa/reajuste, com preferência por um código."""
    code_clean = (re.sub(r"\s+", "", (code or "").upper()) or "")
    pats = [
        r"^tarifas?\s+com\s+reajuste.*mar[cç]o.*2025.*",
        r"^reajuste.*mar[cç]o.*2025.*",
        r"^tarifas?.*reajuste.*",
        r"reajuste",
    ]
    if code_clean:
        pats = [
            rf"^tarifas?\s+com\s+reajuste.*{code_clean}.*",
            rf"^reajuste.*{code_clean}.*",
            rf"reajuste.*{code_clean}.*",
            rf"tarifas?.*{code_clean}.*",
        ] + pats
        if code_clean == "ST2MD":
            pats = [r"reajuste.*st\s*2\s*md"] + pats
    return pats

# -------- Bases (Reservado) --------
def processar_planilha_reservado_bases(xlsx_path: str) -> pd.DataFrame:
    """
    Lê a aba 'Reservado LUFT - Bases' no formato cruzado (Destino x Origens),
    e retorna tidy: [Origem, Destino, Frete_Acordo, Fonte_Tarifa].
    """
    BAS_SHEET = "Reservado LUFT - Bases"
    df_raw = pd.read_excel(xlsx_path, sheet_name=BAS_SHEET)

    header = list(df_raw.iloc[0].tolist()); header[0] = "Destino"
    df = df_raw.copy(); df.columns = header; df = df.iloc[1:, :].reset_index(drop=True)

    if "Destino" not in df.columns or len(df.columns) <= 1:
        raise ValueError("A aba 'Reservado LUFT - Bases' não está no formato esperado.")

    origens = [c for c in df.columns if c != "Destino"]
    tidy = df.melt(id_vars=["Destino"], value_vars=origens, var_name="Origem", value_name="Frete_Acordo")
    tidy["Origem"] = tidy["Origem"].apply(std_text)
    tidy["Destino"] = tidy["Destino"].apply(std_text)
    tidy["Frete_Acordo"] = tidy["Frete_Acordo"].apply(to_num)
    tidy = tidy.dropna(subset=["Origem","Destino","Frete_Acordo"])
    tidy["Fonte_Tarifa"] = f"{BAS_SHEET} - RESMD"
    return tidy.reset_index(drop=True)

# -------- Estações (tratamento único: ex. RESMD) --------
def _processar_aba_estacao_por_tratamento(df_sheet: pd.DataFrame, nome_aba: str,
                                          trat_code: str, tarifa_code: Optional[str]) -> pd.DataFrame:
    df = df_sheet.copy()
    col_origem  = _find_col(df, ORIGEM_PAT)
    col_destino = _find_col(df, DESTINO_PAT)
    col_trat    = _find_col(df, TRATAMENTO_PATS)
    if not (col_origem and col_destino and col_trat):
        return pd.DataFrame(columns=["Origem","Destino","Frete_Acordo","Fonte_Tarifa"])

    tarifa_candidates = []
    if tarifa_code:
        for pat in _tarifa_patterns_for(tarifa_code):
            c = _find_col(df, pat)
            if c and c not in tarifa_candidates: tarifa_candidates.append(c)
    if not tarifa_candidates:
        for pat in [r"reajuste.*", r"tarifas?.*reajuste.*"]:
            c = _find_col(df, pat)
            if c and c not in tarifa_candidates: tarifa_candidates.append(c)
    if not tarifa_candidates:
        return pd.DataFrame(columns=["Origem","Destino","Frete_Acordo","Fonte_Tarifa"])

    code_clean = re.sub(r"\s+", "", trat_code.upper())
    trat_norm = df[col_trat].astype(str).str.upper().str.replace(r"[^A-Z0-9]", "", regex=True)
    base = df[trat_norm.str.contains(code_clean, na=False)].copy()
    if base.empty:
        return pd.DataFrame(columns=["Origem","Destino","Frete_Acordo","Fonte_Tarifa"])

    def pick_tarifa(row):
        for c in tarifa_candidates:
            v = to_num(row.get(c))
            if not pd.isna(v): return v
        return np.nan

    tidy = pd.DataFrame({
        "Origem":  base[col_origem].astype(str).apply(std_text),
        "Destino": base[col_destino].astype(str).apply(std_text),
        "Frete_Acordo": base.apply(pick_tarifa, axis=1),
        "Fonte_Tarifa": f"{nome_aba} - {trat_code}",
    })
    tidy = tidy.dropna(subset=["Origem","Destino","Frete_Acordo"]).drop_duplicates(subset=["Origem","Destino"], keep="last")
    return tidy.reset_index(drop=True)

def processar_abas_estacoes_por_tratamento(xlsx_path: str, trat_code: str, tarifa_code: Optional[str]) -> pd.DataFrame:
    """
    Varre abas que começam com 'Est.' e retorna tidy para um único tratamento (ex.: RESMD).
    """
    xls = pd.ExcelFile(xlsx_path)
    sheets_est = [s for s in xls.sheet_names if s.strip().lower().startswith("est.")]
    frames = []
    for s in sheets_est:
        df_sheet = pd.read_excel(xlsx_path, sheet_name=s)
        frames.append(_processar_aba_estacao_por_tratamento(df_sheet, s, trat_code, tarifa_code))
    frames = [f for f in frames if not f.empty]
    if not frames: return pd.DataFrame(columns=["Origem","Destino","Frete_Acordo","Fonte_Tarifa"])
    return pd.concat(frames, ignore_index=True).drop_duplicates(subset=["Origem","Destino"], keep="last").reset_index(drop=True)

# -------- Estações (múltiplos códigos: ST2MD / MEDICAMENTOS / MD/PE / MD) --------
def _find_matching_code(text: str, codes: list[str]) -> str | None:
    text_tokens = set(tokens(text))
    for code in codes:  # ordem importa!
        code_tokens = set(tokens(code))
        if code_tokens and code_tokens.issubset(text_tokens):
            return code
    return None

def _processar_aba_estacao_por_codigos(df_sheet: pd.DataFrame, nome_aba: str,
                                       valid_service_codes: list[str],
                                       prefer_tarifa_code: Optional[str]) -> pd.DataFrame:
    df = df_sheet.copy()
    col_origem  = _find_col(df, ORIGEM_PAT)
    col_destino = _find_col(df, DESTINO_PAT)
    col_trat    = _find_col(df, TRATAMENTO_PATS)
    if not (col_origem and col_destino and col_trat):
        return pd.DataFrame(columns=["Origem","Destino","Frete_Acordo","Fonte_Tarifa"])

    tarifa_candidates = []
    for pat in _tarifa_patterns_for(prefer_tarifa_code):
        c = _find_col(df, pat)
        if c and c not in tarifa_candidates: tarifa_candidates.append(c)
    if not tarifa_candidates:
        return pd.DataFrame(columns=["Origem","Destino","Frete_Acordo","Fonte_Tarifa"])

    def pick_tarifa(row):
        for c in tarifa_candidates:
            v = to_num(row.get(c))
            if not pd.isna(v): return v
        return np.nan

    matched_series = df[col_trat].apply(lambda x: _find_matching_code(x, valid_service_codes))
    base = df[matched_series.notna()].copy()
    if base.empty:
        return pd.DataFrame(columns=["Origem","Destino","Frete_Acordo","Fonte_Tarifa"])
    matched_series = matched_series.loc[base.index]

    tidy = pd.DataFrame({
        "Origem":  base[col_origem].astype(str).apply(std_text),
        "Destino": base[col_destino].astype(str).apply(std_text),
        "Frete_Acordo": base.apply(pick_tarifa, axis=1),
        "Fonte_Tarifa": matched_series.apply(lambda code: f"{nome_aba} - {code}"),
    })
    tidy = tidy.dropna(subset=["Origem","Destino","Frete_Acordo"]).drop_duplicates(subset=["Origem","Destino"], keep="last")
    return tidy.reset_index(drop=True)

def processar_abas_estacoes_por_codigos(xlsx_path: str,
                                        valid_service_codes: list[str],
                                        prefer_tarifa_code: Optional[str]) -> pd.DataFrame:
    """
    Varre abas que começam com 'Est.' e mapeia múltiplos códigos de serviço
    (ex.: ST2MD, MEDICAMENTOS, MD/PE ...), retornando tidy.
    """
    xls = pd.ExcelFile(xlsx_path)
    sheets_est = [s for s in xls.sheet_names if s.strip().lower().startswith("est.")]
    frames = []
    for s in sheets_est:
        df_sheet = pd.read_excel(xlsx_path, sheet_name=s)
        frames.append(_processar_aba_estacao_por_codigos(df_sheet, s, valid_service_codes, prefer_tarifa_code))
    frames = [f for f in frames if not f.empty]
    if not frames: return pd.DataFrame(columns=["Origem","Destino","Frete_Acordo","Fonte_Tarifa"])
    return pd.concat(frames, ignore_index=True).drop_duplicates(subset=["Origem","Destino"], keep="last").reset_index(drop=True)

# ==================== NOVO: Padronizador UNIFICADO ====================
# Integra com Services/freight_normalizer.py sem quebrar o que já existe acima.
try:
    from Services.Latam.Format_TabelaFreteLatam import normalize_file as _normalize_file, save_outputs as _save_outputs
except Exception as _e_import:
    _normalize_file = None
    _save_outputs = None

def normalizar_acordos(xlsx_path: str | Path, ignore_first_sheet: bool = False) -> pd.DataFrame:
    """
    Usa o novo Padronizador unificado para transformar QUALQUER aba/planilha
    no padrão FRETES_NORMALIZADOS:
      [Origem, Destino, Tipo_Servico, Valor_Frete, Moeda, Unidade_Valor, Regra,
       Peso_Min, Peso_Max, Vigencia_Inicio, Vigencia_Fim, Observacoes,
       Fonte_Arquivo, Fonte_Aba]
    """
    if _normalize_file is None:
        raise ImportError("Services.freight_normalizer não disponível. Verifique a instalação/import.")
    return _normalize_file(Path(xlsx_path), ignore_first_sheet=ignore_first_sheet)

def salvar_normalizados(df: pd.DataFrame, outputs_dir: str | Path = "outputs",
                        basename: str = "fretes_normalizados") -> Tuple[Path, Path]:
    """
    Salva o DataFrame normalizado em Parquet e CSV dentro de 'outputs/'.
    Retorna (parquet_path, csv_path).
    """
    if _save_outputs is None:
        raise ImportError("Services.freight_normalizer.save_outputs não disponível.")
    return _save_outputs(df, outputs_dir=outputs_dir, basename=basename)

def normalizar_e_salvar(xlsx_path: str | Path,
                        outputs_dir: str | Path = "outputs",
                        basename: str = "fretes_normalizados",
                        ignore_first_sheet: bool = False) -> tuple[pd.DataFrame, Path, Path]:
    """
    Atalho: normaliza um arquivo e já salva os resultados.
    Retorna (df, parquet_path, csv_path).
    """
    df = normalizar_acordos(xlsx_path, ignore_first_sheet=ignore_first_sheet)
    pq, csv = salvar_normalizados(df, outputs_dir=outputs_dir, basename=basename)
    return df, pq, csv


# ==================== NOVO BLOCO: integração com o Padronizador ====================
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Tuple
import datetime as dt
import re

# Tenta pegar pastas de config.py; se não tiver, usa padrão local
try:
    import config  # type: ignore
    BASE_UPLOADS = Path(getattr(config, "UPLOAD_DIR", "uploads"))
    BASE_OUTPUTS = Path(getattr(config, "OUTPUT_DIR", "outputs"))
except Exception:
    BASE_UPLOADS = Path("uploads")
    BASE_OUTPUTS = Path("outputs")

# Subpastas específicas deste domínio
UPLOADS_DIR = BASE_UPLOADS / "acordos"
OUTPUTS_DIR = BASE_OUTPUTS / "acordos"

# Importa o serviço de normalização
try:
    from Services.Latam.Format_TabelaFreteLatam import normalize_file as _normalize_file, save_outputs as _save_outputs
except Exception as _e_import:
    _normalize_file = None
    _save_outputs = None

# Helpers de nome/salvamento
_SLUG_RE = re.compile(r"[^A-Za-z0-9._-]+")

def _slugify(name: str) -> str:
    base = name.strip().replace(" ", "_")
    base = _SLUG_RE.sub("", base)
    return base or "arquivo"

def _timestamp() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")

def _ensure_dirs():
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

@dataclass
class NormalizedArtifact:
    csv_path: Path
    parquet_path: Path
    total_rows: int
    created_at: dt.datetime
    source_file: Path

    def to_dict(self) -> Dict:
        return {
            "csv_path": str(self.csv_path),
            "parquet_path": str(self.parquet_path),
            "total_rows": self.total_rows,
            "created_at": self.created_at.isoformat(),
            "source_file": str(self.source_file),
        }

def save_uploaded_xlsx(file_storage, prefix: str = "acordos") -> Path:
    """
    Salva o arquivo enviado (Flask FileStorage) em uploads/acordos com nome seguro + timestamp.
    """
    _ensure_dirs()
    original = getattr(file_storage, "filename", "") or "acordos.xlsx"
    stem = original.rsplit(".", 1)[0]
    safe_name = f"{prefix}__{_slugify(stem)}__{_timestamp()}.xlsx"
    out_path = UPLOADS_DIR / safe_name
    file_storage.save(out_path)  # type: ignore
    return out_path

def normalize_and_persist(xlsx_path: str | Path, ignore_first_sheet: bool = False) -> tuple[pd.DataFrame, NormalizedArtifact]:
    """
    Normaliza o Excel completo e salva CSV/Parquet em outputs/acordos.
    Retorna (DataFrame, NormalizedArtifact).
    """
    if _normalize_file is None or _save_outputs is None:
        raise ImportError("Services.freight_normalizer não disponível. Verifique imports.")
    _ensure_dirs()

    xlsx_path = Path(xlsx_path)
    df = _normalize_file(xlsx_path, ignore_first_sheet=ignore_first_sheet)

    basename = f"fretes_normalizados__{_slugify(xlsx_path.stem)}__{_timestamp()}"
    parquet_path, csv_path = _save_outputs(df, outputs_dir=OUTPUTS_DIR, basename=basename)  # retorna (parquet, csv)

    art = NormalizedArtifact(
        csv_path=csv_path,
        parquet_path=parquet_path,
        total_rows=len(df),
        created_at=dt.datetime.now(),
        source_file=xlsx_path,
    )
    return df, art

def preview(df: pd.DataFrame, n: int = 100) -> Dict[str, list]:
    """
    Retorna um dicionário com 'columns' e 'rows' (até n linhas) para exibir na UI.
    """
    cols = list(df.columns)
    rows = df.head(n).to_dict(orient="records")
    return {"columns": cols, "rows": rows}

# ==================== DEFINIÇÃO DA CLASSE REPOSITORY ====================
# Esta é a classe que estava faltando e que seu SimulationService precisa importar.
# ========================================================================

class TabelaFretesRepository:
    """
    Esta classe centraliza o acesso e o processamento das tabelas de fretes
    a partir de um arquivo Excel. Ela utiliza as funções definidas neste mesmo módulo.
    """

    def __init__(self, xlsx_path: str | Path):
        """
        Inicializa o repositório, guardando o caminho para o arquivo Excel de fretes.

        Args:
            xlsx_path (str | Path): O caminho para o arquivo .xlsx que contém as tabelas.
        """
        self.xlsx_path = Path(xlsx_path)
        if not self.xlsx_path.exists():
            raise FileNotFoundError(f"O arquivo de fretes não foi encontrado em: {self.xlsx_path}")

    def carregar_fretes_normalizados(self, ignore_first_sheet: bool = False) -> pd.DataFrame:
        """
        Usa a função `normalizar_acordos` para carregar e padronizar
        todos os fretes do arquivo Excel.

        Returns:
            pd.DataFrame: Um DataFrame com todos os fretes em formato padronizado.
        """
        # A função `normalizar_acordos` já existe no seu arquivo.
        # A classe apenas a utiliza para organizar o acesso aos dados.
        print(f"Normalizando fretes do arquivo: {self.xlsx_path}")
        return normalizar_acordos(self.xlsx_path, ignore_first_sheet=ignore_first_sheet)

    def carregar_fretes_por_tratamento(self, trat_code: str, tarifa_code: Optional[str] = None) -> pd.DataFrame:
        """
        Carrega fretes para um código de tratamento específico (ex: "RESMD"),
        varrendo as abas que começam com "Est.".

        Args:
            trat_code (str): O código do tratamento a ser buscado.
            tarifa_code (Optional[str]): Um código opcional para ajudar a encontrar a coluna de tarifa correta.

        Returns:
            pd.DataFrame: Um DataFrame com os fretes encontrados para aquele tratamento.
        """
        # A função `processar_abas_estacoes_por_tratamento` também já existe no arquivo.
        print(f"Carregando fretes para o tratamento '{trat_code}'...")
        return processar_abas_estacoes_por_tratamento(self.xlsx_path, trat_code, tarifa_code)
