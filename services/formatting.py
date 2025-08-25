# services/formatting.py
from __future__ import annotations
import pandas as pd

def round_and_prepare(df: pd.DataFrame, eps: float):
    """
    Prepara dois DataFrames:
      - df_export: numérico para exportação (Excel/CSV)
      - df_display: para exibição (com Dif_% formatado)

    Regras de nomenclatura para saída:
      - 'Documento'     -> 'nOca'
      - 'Frete_Acordo'  -> 'Frete_Tabela'
      - excluir colunas: 'Dif_abs', 'Observacao'

    Observação: a leitura de PDF e cálculos permanecem com os nomes originais.
    """
    df_export = df.copy()

    def _roundeps(series, decimals=2):
        s = pd.to_numeric(series, errors="coerce")
        s = s.mask(s.abs() < eps, 0.0)
        return s.round(decimals)

    # arredondamentos numéricos mantidos na exportação
    if "Diferenca" in df_export.columns:
        df_export["Diferenca"] = _roundeps(df_export["Diferenca"], 2)
    if "Dif_abs" in df_export.columns:
        df_export["Dif_abs"] = _roundeps(df_export["Dif_abs"], 2)
    if "Dif_%" in df_export.columns:
        df_export["Dif_%"] = _roundeps(df_export["Dif_%"], 2)

    # cópia para exibição (formata %) 
    df_display = df_export.copy()
    if "Dif_%" in df_display.columns:
        df_display["Dif_%"] = df_display["Dif_%"].apply(lambda v: f"{v:.2f}%" if pd.notna(v) else "-")

    # ===== renomeações apenas para saída =====
    rename_map = {
        "Documento": "nOca",
        "Frete_Acordo": "Frete_Tabela",
        "Tipo_Serviço": "Tipo_Servico",
        "Dif_%": "Dif_Perc"
    }
    df_export = df_export.rename(columns=rename_map)
    df_display = df_display.rename(columns=rename_map)

    # ===== remoções de colunas =====
    cols_drop = ["Dif_abs", "Observacao"]
    df_export = df_export.drop(columns=[c for c in cols_drop if c in df_export.columns], errors="ignore")
    df_display = df_display.drop(columns=[c for c in cols_drop if c in df_display.columns], errors="ignore")

    return df_export, df_display
