# services/comparison_tables_with_invoice.py
from __future__ import annotations
import numpy as np
import pandas as pd

from config import AppConfig
from core.matching import service_code_from_tipo
from repositories.freight_agreements_repository import (
    processar_planilha_reservado_bases,
    processar_abas_estacoes_por_tratamento,
    processar_abas_estacoes_por_codigos,
)
from utils.parse import std_text

def compare_fretes(df_base: pd.DataFrame, acordos_xlsx_path: str, cfg: AppConfig) -> pd.DataFrame:
    """Mantém o comportamento do table_compare.comparar_fretes original (com melhorias)."""
    if df_base.empty:
        return df_base

    df = df_base.copy()
    df["__TRAT_CODE__"] = df.get("Tipo_Serviço", "").apply(service_code_from_tipo)

    results = []

    # --- RESERVADO MEDS (RESMD) ---
    df_res = df[df["__TRAT_CODE__"] == cfg.services.RESERVADO_CODE].copy()
    if not df_res.empty:
        # 1) Bases
        df_bases = processar_planilha_reservado_bases(acordos_xlsx_path)
        bloco1 = _comparar_bloco(df_res, df_bases, cfg.tuning.TOLERANCIA_PCT_DEFAULT)

        # 2) Fallback Estações (onde faltou)
        falt_mask = bloco1["Frete_Acordo"].isna()
        if falt_mask.any():
            df_est_res = processar_abas_estacoes_por_tratamento(
                acordos_xlsx_path, cfg.services.RESERVADO_CODE, cfg.services.RESERVADO_CODE
            )
            if not df_est_res.empty:
                # Alinha o índice do mask com o df_res
                falt_mask_aligned = falt_mask.reindex(df_res.index, fill_value=False)
                aux = _comparar_bloco(df_res[falt_mask_aligned].copy(), df_est_res, cfg.tuning.TOLERANCIA_PCT_DEFAULT)
                for c in ["Frete_Acordo","Diferenca","Dif_abs","Dif_%","Status","Fonte_Tarifa","Observacao"]:
                    bloco1.loc[falt_mask, c] = aux[c].values
        results.append(bloco1)

    # --- ESTANDAR family (mapped to ST2MD): só Estações, múltiplos códigos válidos ---
    df_std = df[df["__TRAT_CODE__"] == cfg.services.ESTANDAR_CODE].copy()
    if not df_std.empty:
        df_est_st2 = processar_abas_estacoes_por_codigos(
            acordos_xlsx_path,
            valid_service_codes=cfg.services.VALID_CODES_ST2,  # ["ST2MD","MD/PE","MEDICAMENTOS","MD"]
            prefer_tarifa_code="ST2MD",
        )
        bloco2 = _comparar_bloco(df_std, df_est_st2, cfg.tuning.TOLERANCIA_PCT_DEFAULT)
        results.append(bloco2)

    if results:
        out = pd.concat(results, ignore_index=True)
        return out.drop(columns=["__TRAT_CODE__"], errors="ignore")

    out = df.drop(columns=["__TRAT_CODE__"], errors="ignore").copy()
    out["Aviso"] = "Nenhuma linha com Tipo_Serviço suportado (RESERVADO MEDS / ESTANDAR ...) foi encontrada."
    return out

def _comparar_bloco(df_pdf_bloco: pd.DataFrame, df_acordos_tidy: pd.DataFrame, tolerancia_pct: float) -> pd.DataFrame:
    if df_pdf_bloco.empty:
        return df_pdf_bloco

    df = df_pdf_bloco.copy()
    for col in ("Origem", "Destino"):
        if col in df.columns: 
            df[col] = df[col].apply(std_text)
        else: 
            df[col] = ""

    df_final = pd.merge(df, df_acordos_tidy, on=["Origem","Destino"], how="left", validate="m:1")

    # Conversões numéricas
    df_final["Frete_Peso"]    = pd.to_numeric(df_final.get("Frete_Peso",    np.nan), errors="coerce")
    df_final["Frete_Acordo"]  = pd.to_numeric(df_final.get("Frete_Acordo",  np.nan), errors="coerce")
    df_final["Vlr Frete"]     = pd.to_numeric(df_final.get("Vlr Frete",     np.nan), errors="coerce")  # <- necessário para frete mínimo

    # Cálculos padrão
    df_final["Diferenca"] = df_final["Frete_Peso"] - df_final["Frete_Acordo"]
    df_final["Dif_abs"]   = df_final["Diferenca"].abs()
    df_final["Dif_%"]     = np.where(
        df_final["Frete_Acordo"].notna() & (df_final["Frete_Acordo"] != 0),
        (df_final["Frete_Peso"] / df_final["Frete_Acordo"] - 1.0) * 100.0,
        np.nan,
    )

    tol = float(tolerancia_pct)

    # Status padrão (quando não é frete mínimo)
    df_final["Status"] = np.where(
        df_final["Frete_Acordo"].isna(),
        "Tarifa Não Localizada",
        np.where(df_final["Dif_%"].abs() <= tol, "Dentro da tolerância", "Fora da tolerância"),
    )

    # Observação padrão
    df_final["Observacao"] = np.where(
        df_final["Frete_Acordo"].isna(),
        "Sem tarifa encontrada nas Estações para (Origem, Destino).",
        "",
    )

    # --- Regra do FRETE MÍNIMO ---
    # Se "Vlr Frete" <= 60, não calcula diferença e força Status/Observacao
    minimo_mask = df_final["Vlr Frete"].notna() & (df_final["Vlr Frete"] <= 60)
    if minimo_mask.any():
        df_final.loc[minimo_mask, ["Diferenca", "Dif_abs", "Dif_%"]] = np.nan
        df_final.loc[minimo_mask, "Status"] = "Frete Mínimo"
        df_final.loc[minimo_mask, "Observacao"] = "Frete mínimo aplicado (Vlr Frete ≤ 60)."
        
    colunas_principais = [
        "Tipo_Serviço","Origem","Destino","Data","Documento","Vlr Frete",
        "Frete_Peso","Frete_Acordo","Diferenca","Dif_abs","Dif_%","Status","Fonte_Tarifa",
        "Peso Taxado",
    ]
    restantes = [c for c in df_final.columns if c not in colunas_principais and c != "__TRAT_CODE__"]
    return df_final[colunas_principais + restantes].reset_index(drop=True)
