# C:\Programs\Aéreo-Comparativos\Services\Latam\ComparativoLatam.py
from __future__ import annotations
import unicodedata
import re
import numpy as np
import pandas as pd

from Config import Appconfig
from Db import engine
from Utils.Parse import service_code_from_tipo
from Repositories.Repositorio_TabelasFretes import (
    processar_planilha_reservado_bases,
    processar_abas_estacoes_por_tratamento,
    processar_abas_estacoes_por_codigos,
)
from Repositories.Db_Queries import get_first_ctc_motivodoc, get_ctcs, get_ctc_peso, get_tipo_servico
from Utils.Parse import std_text


# ========= Helpers (Parse/Numeric) - Funções de Ajuda para Conversão Numérica =========

def _smart_to_numeric(val):
    """
    Converte um valor para numérico (float) de forma inteligente.
    """
    if pd.isna(val): return np.nan
    if isinstance(val, (int, float, np.number)): return float(val)
    
    s = str(val).strip()
    if s == "": return np.nan
    
    if "," in s and "." in s and s.rfind(",") > s.rfind("."):
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
        
    try:
        return float(s)
    except (ValueError, TypeError):
        return pd.to_numeric(s, errors="coerce")

def _to_numeric_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Aplica a conversão numérica inteligente a uma lista de colunas de um DataFrame."""
    for c in cols:
        if c in df.columns:
            df[c] = df[c].apply(_smart_to_numeric)
    return df

# ========= Helpers (Organização e Limpeza da Planilha) - Funções de Ajuda para Preparar o DataFrame =========

def _sanitize_header(name: str) -> str:
    """
    Limpa e padroniza os nomes das colunas para exportação.
    """
    s = unicodedata.normalize('NFKD', str(name)).encode('ascii', 'ignore').decode('ascii') # Remove acentos
    s = s.replace('%', 'pct') # Substitui '%' por 'pct'
    s = s.lower() # Converte para minúsculas
    s = re.sub(r'[^a-z0-9]+', '_', s) # Substitui espaços e outros símbolos por underscores
    s = s.strip('_') # Remove underscores extras no início/fim

    # Capitaliza a primeira letra de cada palavra separada por '_'
    if s:
        s = '_'.join(word.capitalize() for word in s.split('_'))
    
    return s

def _sanitize_and_dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Garante que não haja colunas duplicadas no DataFrame."""
    if df is None or df.empty: return df
    df2 = df.copy()
    df2.columns = pd.Index([str(c).strip() for c in df2.columns])
    mask = ~df2.columns.duplicated(keep="first")
    return df2.loc[:, mask]

def fill_numeric_nans_with_zero(df: pd.DataFrame) -> pd.DataFrame:
    """Preenche valores nulos (NaN) com zero em todas as colunas numéricas."""
    df2 = df.copy()
    for c in df2.columns:
        if pd.api.types.is_numeric_dtype(df2[c]):
            df2[c] = df2[c].fillna(0)
    return df2

def _finalize_dataframe(df: pd.DataFrame, epsilon: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Prepara os DataFrames finais: um para exibição e outro para exportação.
    """
    if df.empty: return df.copy(), df.copy()

    if "Frete_Acordo" in df.columns:
        df = df.drop(columns=["Frete_Acordo"])

    col_order = [
        "Tipo_Serviço", "Origem", "Destino", "Data", "Documento",
        "CTCs",
        "Vlr Frete", "Frete_Peso", "Frete_Tabela","Vlr_Frete_Peso_Tabela",
        "Diferenca_Frete", "Diferenca_Frete_Peso", "Peso Taxado", "Peso_Taxado_CTC", "Diferenca_Peso",
        "Status", "Fonte_Tarifa", "Observacao",
    ]
    existing_cols = [c for c in col_order if c in df.columns]
    other_cols = [c for c in df.columns if c not in existing_cols and not c.startswith("__")]
    df_processed = df[existing_cols + other_cols].copy()

    numeric_cols_to_round = [
        "Vlr Frete", "Frete_Peso", "Peso Taxado", "Vlr_Frete_Peso_Tabela",
        "Frete_Tabela", "Diferenca_Frete", "Diferenca_Frete_Peso"
    ]
    for col in numeric_cols_to_round:
        if col in df_processed.columns:
            df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce').round(2)
    if "Dif_%" in df_processed.columns:
        df_processed["Dif_%"] = pd.to_numeric(df_processed["Dif_%"], errors='coerce').round(2)

    df_processed = _sanitize_and_dedupe_columns(df_processed)

    df_for_display = df_processed.copy()
    if "Dif_%" in df_for_display.columns:
        is_meaningful = df_for_display["Dif_%"].abs() > epsilon
        df_for_display["Dif_%"] = np.where(
            pd.notna(df_for_display["Dif_%"]) & is_meaningful,
            df_for_display["Dif_%"].apply(lambda x: f"{x:,.2f}%".replace(",", "X").replace(".", ",").replace("X", ".")),
            "—"
        )
    for col in numeric_cols_to_round:
        if col in df_for_display.columns:
            df_for_display[col] = df_for_display[col].apply(
                lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if pd.notna(x) else "—"
            )

    df_for_export = df_processed.copy()
    
    if "Dif_%" in df_for_export.columns:
        df_for_export["Dif_%"] = pd.to_numeric(df_for_export["Dif_%"], errors="coerce") / 100.0

    df_for_export.columns = [_sanitize_header(col) for col in df_for_export.columns]
    return df_for_export, df_for_display


# ========= Lógica Principal =========
def compare_fretes(df_base: pd.DataFrame, acordos_xlsx_path: str, cfg: Appconfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Orquestra a comparação seguindo a lógica de prioridade do banco de dados.
    """
    if df_base.empty:
        return pd.DataFrame(), pd.DataFrame()

    # 1. RECEBE OS DADOS EXTRAÍDOS DO PDF
    df = df_base.copy().reset_index(drop=True)
    df["__ROW_ID__"] = np.arange(len(df))

    # 2. CONSULTA O BANCO E SUBSTITUI O TIPO DE SERVIÇO
    # O valor do banco de dados (se existir) tem prioridade sobre o do PDF.
    noca_list = df['Documento'].astype(str).dropna().unique().tolist()
    df_servico_db = get_tipo_servico(noca_list)
    if not df_servico_db.empty:
        df_servico_db.rename(columns={'Tipo_Servico': 'Tipo_Servico_DB'}, inplace=True)
        df = pd.merge(df, df_servico_db, on="Documento", how="left")
        
        # A substituição acontece aqui: usa o valor do DB, senão, mantém o original.
        df['Tipo_Serviço'] = np.where(
            df['Tipo_Servico_DB'].notna() & (df['Tipo_Servico_DB'] != ''), 
            df['Tipo_Servico_DB'], 
            df['Tipo_Serviço']
        )
        df.drop(columns=['Tipo_Servico_DB'], inplace=True, errors='ignore')

    # 3. A PARTIR DAQUI, TUDO SE BASEIA NO ÚLTIMO TIPO DE SERVIÇO
    # Cria o código de tratamento baseado no Tipo_Serviço já finalizado.
    df["__TRAT_CODE__"] = df.get("Tipo_Serviço", "").apply(service_code_from_tipo)

    results = []

    # --- Bloco 1: Processamento do serviço "RESERVADO" ---
    df_res = df[df["__TRAT_CODE__"] == cfg.Services.RESERVADO_CODE].copy()
    if not df_res.empty:
        df_bases = processar_planilha_reservado_bases(acordos_xlsx_path)
        bloco1 = _comparar_bloco(df_res, df_bases, cfg.tuning.TOLERANCIA_PCT_DEFAULT)
        
        yet_missing_mask = bloco1["Status"].eq("TARIFA NAO LOCALIZADA")
        if yet_missing_mask.any():
            missing_row_ids = bloco1.loc[yet_missing_mask, "__ROW_ID__"].to_numpy()
            df_est_res = processar_abas_estacoes_por_tratamento(
                acordos_xlsx_path, cfg.Services.RESERVADO_CODE, cfg.Services.RESERVADO_CODE
            )
            if not df_est_res.empty:
                subset_df_res = df_res[df_res["__ROW_ID__"].isin(missing_row_ids)].copy()
                aux = _comparar_bloco(subset_df_res, df_est_res, cfg.tuning.TOLERANCIA_PCT_DEFAULT)
                merged = bloco1.merge(aux, on="__ROW_ID__", how="left", suffixes=("", "__AUX"))
                was_missing = merged["__ROW_ID__"].isin(missing_row_ids)
                update_cols = [c for c in aux.columns if c in bloco1.columns and c != "__ROW_ID__"]
                for c in update_cols:
                    ac = f"{c}__AUX"
                    if ac in merged.columns:
                        merged[c] = np.where(was_missing & merged[ac].notna(), merged[ac], merged[c])
                bloco1 = merged.drop(columns=[c for c in merged.columns if c.endswith("__AUX")])
        results.append(bloco1)

    # --- Bloco 2: Processamento do serviço "ESTANDAR" ---
    df_std = df[df["__TRAT_CODE__"] == cfg.Services.ESTANDAR_CODE].copy()
    if not df_std.empty:
        df_est_st2 = processar_abas_estacoes_por_codigos(
            acordos_xlsx_path,
            valid_service_codes=cfg.Services.VALID_CODES_ST2,
            prefer_tarifa_code="ST2MD",
        )
        bloco2 = _comparar_bloco(df_std, df_est_st2, cfg.tuning.TOLERANCIA_PCT_DEFAULT)
        results.append(bloco2)

    if not results:
        out = df.drop(columns=["__TRAT_CODE__", "__ROW_ID__"], errors="ignore").copy()
        out["Aviso"] = "Nenhuma linha com Tipo_Serviço suportado (RESERVADO MEDS / ESTANDAR ...) foi encontrada."
        return out, pd.DataFrame()

    df_raw_final = pd.concat(results, ignore_index=True)
    df_export, df_display = _finalize_dataframe(df_raw_final, cfg.tuning.EPSILON)
    return df_export, df_display


def _comparar_bloco(df_pdf_bloco: pd.DataFrame, df_acordos_tidy: pd.DataFrame, tolerancia_pct: float) -> pd.DataFrame:
    """
    Realiza a comparação para um bloco de dados, com lógica de devolução
    baseada na busca de tarifa invertida (Destino -> Origem).
    """
    if df_pdf_bloco.empty: return df_pdf_bloco
    df = df_pdf_bloco.copy()
    if "__ROW_ID__" not in df.columns:
        df = df.reset_index(drop=True)
        df["__ROW_ID__"] = np.arange(len(df))
    
    # 1. Padroniza Origem e Destino
    for col in ("Origem", "Destino"):
        if col in df.columns: df[col] = df[col].apply(std_text)
    
    noca_list = df['Documento'].astype(str).dropna().unique().tolist()
    
    # LÓGICA DOS CTCS E PESOS (BUSCA OTIMIZADA NO DB) - MANTIDA
    # OBS: Você precisará garantir que get_ctcs e get_ctc_peso sejam as funções corretas para buscar esses dados
    df_ctcs = get_ctcs(noca_list) 
    if not df_ctcs.empty:
        df = pd.merge(df, df_ctcs, on="Documento", how="left")
    else:
        df["CTCs"] = None 
        
    df_peso_ctc = get_ctc_peso(noca_list) 
    if not df_peso_ctc.empty:
        df = pd.merge(df, df_peso_ctc, on="Documento", how="left")
        df.rename(columns={'PesoCTC_Total': 'Peso_Taxado_CTC'}, inplace=True) 
    else:
        df["Peso_Taxado_CTC"] = np.nan 
        
    # LÓGICA PRINCIPAL DE COMPARAÇÃO
    
    # 2. Tenta encontrar a tarifa direta (Origem -> Destino)
    df_norm = pd.merge(df, df_acordos_tidy, on=["Origem", "Destino"], how="left", validate="m:1")
    df_norm["__EH_DEV__"] = False # Flag para marcar se é uma devolução

    # 3. Se faltou tarifa, tenta a busca invertida para identificar devolução
    faltou_tarifa_unit = df_norm.get("Frete_Acordo", pd.Series(dtype=float)).isna()
    
    if faltou_tarifa_unit.any():
        
        # Seleciona apenas as linhas onde a tarifa não foi encontrada
        subset_ids = df_norm.loc[faltou_tarifa_unit, "__ROW_ID__"].to_numpy()
        subset = df[df["__ROW_ID__"].isin(subset_ids)].copy()
        
        # Merge invertido: left_on=["Origem", "Destino"], right_on=["Destino", "Origem"]
        inv = pd.merge(subset, df_acordos_tidy, left_on=["Origem", "Destino"], right_on=["Destino", "Origem"], 
                       how="left", validate="m:1", suffixes=("", "__DEV"))
        
        # Colunas de acordo a serem copiadas
        acordo_cols = [c for c in df_acordos_tidy.columns if c not in ("Origem", "Destino", "Destino", "Origem")]
        
        if acordo_cols:
            inv_small = inv[["__ROW_ID__"] + acordo_cols].copy()
            inv_small.rename(columns={c: f"{c}__DEV" for c in acordo_cols}, inplace=True)
            df_norm = pd.merge(df_norm, inv_small, on="__ROW_ID__", how="left")
            
            # Máscara para as linhas onde a tarifa invertida foi encontrada
            dev_found = df_norm.get("Frete_Acordo__DEV", pd.Series(dtype=float)).notna()
            
            # Atualiza as colunas de acordo com os valores de devolução, onde faltavam
            for c in acordo_cols:
                dc = f"{c}__DEV"
                if dc in df_norm.columns:
                    df_norm[c] = np.where(df_norm[c].isna() & dev_found, df_norm[dc], df_norm[c])
            
            # Marca como devolução se a tarifa foi encontrada por essa busca invertida
            df_norm.loc[dev_found, "__EH_DEV__"] = True
            
            # Atualiza a Fonte_Tarifa
            dev_found_and_missing_source = dev_found & df_norm["Fonte_Tarifa"].isna()
            df_norm["Fonte_Tarifa"] = np.where(dev_found_and_missing_source, 
                                               "tarifa [devolucao]", 
                                               df_norm["Fonte_Tarifa"])

            # Remove colunas auxiliares
            drop_aux = [f"{c}__DEV" for c in acordo_cols if f"{c}__DEV" in df_norm.columns]
            if drop_aux: df_norm.drop(columns=drop_aux, inplace=True, errors="ignore")
            
    # Remove qualquer coluna __MOTIVODOC__ se ela existiu (caso venha do seu código atual)
    df_norm.drop(columns=['__MOTIVODOC__'], inplace=True, errors="ignore") 

    # 4. Realiza os cálculos de comparação
    df_final = _to_numeric_cols(df_norm, ["Frete_Peso", "Frete_Acordo", "Vlr Frete", "Peso Taxado", "Peso_Taxado_CTC"])
    df_final["Vlr_Frete_Peso_Tabela"] = df_final["Frete_Acordo"]
    df_final["Frete_Tabela"] = df_final["Vlr_Frete_Peso_Tabela"] * df_final["Peso Taxado"]
    df_final["Diferenca_Frete"] = df_final["Vlr Frete"] - df_final["Frete_Tabela"]
    df_final["Diferenca_Frete_Peso"] = df_final["Frete_Peso"] - df_final["Vlr_Frete_Peso_Tabela"]
    df_final["Diferenca_Peso"] = df_final["Peso Taxado"] - df_final.get("Peso_Taxado_CTC", 0) 
    
    df_final["Dif_%"] = np.where(df_final["Frete_Tabela"].notna() & (df_final["Frete_Tabela"] != 0), (df_final["Vlr Frete"] / df_final["Frete_Tabela"] - 1.0) * 100.0, np.nan)
    
    # 5. Define o Status da linha com base nos resultados
    tol = float(tolerancia_pct)
    has_tarifa = df_final["Vlr_Frete_Peso_Tabela"].notna()
    is_explicit_dev = df_final["__EH_DEV__"].eq(True)

    df_final["Status"] = np.where(~has_tarifa, "TARIFA NAO LOCALIZADA", 
                                  np.where(is_explicit_dev, "DEVOLUCAO", 
                                           np.where(df_final["Dif_%"].abs() <= tol, 
                                                    "DENTRO DA TOLERANCIA", 
                                                    "FORA DA TOLERANCIA")))
    
    df_final["Observacao"] = np.where(~has_tarifa, "Sem tarifa para (Origem, Destino).", "")

    # 6. Trata o caso especial de "frete mínimo"
    minimo_mask = df_final["Vlr Frete"].notna() & (df_final["Vlr Frete"] <= 60)
    if minimo_mask.any():
        df_final.loc[minimo_mask, ["Diferenca_Frete", "Diferenca_Frete_Peso", "Dif_%"]] = np.nan
        df_final.loc[minimo_mask, "Status"] = "FRETE MINIMO"
        df_final.loc[minimo_mask, "Observacao"] = "Frete minimo aplicado (Vlr Frete ≤ 60)."
        
    df_final.drop(columns=['__EH_DEV__'], inplace=True, errors="ignore") 
        
    return df_final.reset_index(drop=True)