# C:\Programs\Aéreo-Comparativos\Services\Latam\ComparativoLatam.py

from __future__ import annotations
import pandas as pd
import numpy as np

# Importa as configurações do aplicativo e a conexão com o banco
from Config import Appconfig
from Db import engine # (MANTIDO, assumindo que é usado por repositórios)
from Utils.Parse import service_code_from_tipo, std_text

# Importa as novas funções de helpers
from Utils.Numeric_Helpers import to_numeric_cols
from Utils.DataFrame_Helpers import sanitize_header, sanitize_and_dedupe_columns

# Importa as funções de repositório
from Repositories.Repositorio_TabelasFretes import (
    processar_planilha_reservado_bases,
    processar_abas_estacoes_por_tratamento,
    processar_abas_estacoes_por_codigos,
)
from Repositories.Db_Queries import get_tipo_servico, get_ctcs, get_ctc_peso 
# OBS: get_first_ctc_motivodoc não é mais usada na lógica, foi removida.


class LatamFreightComparer:
    """
    Classe responsável por orquestrar a lógica de comparação de fretes LATAM.

    Cruza dados de frete (base PDF) com tarifas acordadas (planilha XLSX)
    e dados adicionais do banco de dados.
    """

    def __init__(self, cfg: Appconfig):
        """Inicializa o comparador com as configurações do aplicativo."""
        self.cfg = cfg

    def _finalize_dataframe(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Prepara os DataFrames finais: um para exportação (limpo, numérico) 
        e outro para exibição (formatado com vírgulas/percentual).
        """
        if df.empty: 
            return df.copy(), df.copy()

        df = df.drop(columns=["Frete_Acordo"], errors='ignore')

        # --- MUDANÇA 1: Define a nova ordem das colunas, incluindo todos os pesos ---
        col_order = [
            "Tipo_Serviço", "Origem", "Destino", "Data", "Documento",
            "CTCs",
            "Vlr Frete", "Frete_Peso", "Frete_Tabela", "Vlr_Frete_Peso_Tabela",
            "Diferenca_Frete", "Diferenca_Frete_Peso", 
            # Bloco de Pesos na sequência solicitada
            "Peso Taxado",         # Do PDF
            "Peso_Taxado_CTC",     # Do DB
            "Peso_Bruto_CTC",      # Do DB
            "PesoUsado_CIA",       # O maior peso do DB
            "TipoPeso_CIA",        # Descrição do peso usado
            "Diferenca_Peso",      # Nova diferença
            # Fim do Bloco de Pesos
            "Status", "Fonte_Tarifa", "Observacao",
        ]
        existing_cols = [c for c in col_order if c in df.columns]
        other_cols = [c for c in df.columns if c not in existing_cols and not str(c).startswith("__")]
        df_processed = df[existing_cols + other_cols].copy()

        # --- MUDANÇA 2: Adiciona as novas colunas de peso para formatação numérica ---
        numeric_cols_to_round = [
            "Vlr Frete", "Frete_Peso", "Peso Taxado", "Vlr_Frete_Peso_Tabela",
            "Frete_Tabela", "Diferenca_Frete", "Diferenca_Frete_Peso",
            # Novas colunas adicionadas
            "Peso_Taxado_CTC", "Peso_Bruto_CTC", "PesoUsado_CIA", "Diferenca_Peso"
        ]
        
        for col in numeric_cols_to_round:
            if col in df_processed.columns:
                df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce').round(2)
        
        if "Dif_%" in df_processed.columns:
            df_processed["Dif_%"] = pd.to_numeric(df_processed["Dif_%"], errors='coerce').round(2)

        df_processed = sanitize_and_dedupe_columns(df_processed)

        # 2. DATAFRAME PARA EXIBIÇÃO (Lógica inalterada)
        df_for_display = df_processed.copy()
        epsilon = self.cfg.tuning.EPSILON

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

        # 3. DATAFRAME PARA EXPORTAÇÃO (Lógica inalterada)
        df_for_export = df_processed.copy()
        
        if "Dif_%" in df_for_export.columns:
            df_for_export["Dif_%"] = pd.to_numeric(df_for_export["Dif_%"], errors="coerce") / 100.0

        df_for_export.columns = [sanitize_header(col) for col in df_for_export.columns]
        
        return df_for_export, df_for_display

    # Nenhuma mudança nesta função
    def _process_service_type_from_db(self, df: pd.DataFrame) -> pd.DataFrame:
        noca_list = df['Documento'].astype(str).dropna().unique().tolist()
        df_servico_db = get_tipo_servico(noca_list)
        if not df_servico_db.empty:
            df_servico_db.rename(columns={'Tipo_Servico': 'Tipo_Servico_DB'}, inplace=True)
            df = pd.merge(df, df_servico_db, on="Documento", how="left")
            df['Tipo_Serviço'] = np.where(
                df['Tipo_Servico_DB'].notna() & (df['Tipo_Servico_DB'] != ''), 
                df['Tipo_Servico_DB'], 
                df['Tipo_Serviço']
            )
            df.drop(columns=['Tipo_Servico_DB'], inplace=True, errors='ignore')
        return df

    # Nenhuma mudança nesta função
    def compare_fretes(self, df_base: pd.DataFrame, acordos_xlsx_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        if df_base.empty:
            return pd.DataFrame(), pd.DataFrame()
        df = df_base.copy().reset_index(drop=True)
        df["__ROW_ID__"] = np.arange(len(df))
        df = self._process_service_type_from_db(df)
        df["__TRAT_CODE__"] = df.get("Tipo_Serviço", "").apply(service_code_from_tipo)
        results = []
        tolerancia_pct = self.cfg.tuning.TOLERANCIA_PCT_DEFAULT
        reservado_code = self.cfg.Services.RESERVADO_CODE
        df_res = df[df["__TRAT_CODE__"] == reservado_code].copy()
        if not df_res.empty:
            df_bases = processar_planilha_reservado_bases(acordos_xlsx_path)
            bloco1 = self._comparar_bloco(df_res, df_bases, tolerancia_pct)
            yet_missing_mask = bloco1["Status"].eq("TARIFA NAO LOCALIZADA")
            if yet_missing_mask.any():
                missing_row_ids = bloco1.loc[yet_missing_mask, "__ROW_ID__"].to_numpy()
                df_est_res = processar_abas_estacoes_por_tratamento(
                    acordos_xlsx_path, reservado_code, reservado_code
                )
                if not df_est_res.empty:
                    subset_df_res = df_res[df_res["__ROW_ID__"].isin(missing_row_ids)].copy()
                    aux = self._comparar_bloco(subset_df_res, df_est_res, tolerancia_pct)
                    merged = bloco1.merge(aux, on="__ROW_ID__", how="left", suffixes=("", "__AUX"))
                    was_missing = merged["__ROW_ID__"].isin(missing_row_ids)
                    update_cols = [c for c in aux.columns if c in bloco1.columns and c != "__ROW_ID__"]
                    for c in update_cols:
                        ac = f"{c}__AUX"
                        if ac in merged.columns:
                            merged[c] = np.where(was_missing & merged[ac].notna(), merged[ac], merged[c])
                    bloco1 = merged.drop(columns=[c for c in merged.columns if c.endswith("__AUX")])
            results.append(bloco1)
        df_std = df[df["__TRAT_CODE__"] == self.cfg.Services.ESTANDAR_CODE].copy()
        if not df_std.empty:
            df_est_st2 = processar_abas_estacoes_por_codigos(
                acordos_xlsx_path,
                valid_service_codes=self.cfg.Services.VALID_CODES_ST2,
                prefer_tarifa_code="ST2MD",
            )
            bloco2 = self._comparar_bloco(df_std, df_est_st2, tolerancia_pct)
            results.append(bloco2)
        if not results:
            out = df.drop(columns=["__TRAT_CODE__", "__ROW_ID__"], errors="ignore").copy()
            out["Aviso"] = "Nenhuma linha com Tipo_Serviço suportado (RESERVADO MEDS / ESTANDAR ...) foi encontrada."
            return out, pd.DataFrame()
        df_raw_final = pd.concat(results, ignore_index=True)
        df_export, df_display = self._finalize_dataframe(df_raw_final)
        return df_export, df_display

    def _comparar_bloco(self, df_pdf_bloco: pd.DataFrame, df_acordos_tidy: pd.DataFrame, tolerancia_pct: float) -> pd.DataFrame:
        """
        Realiza a comparação principal de um bloco de dados...
        """
        if df_pdf_bloco.empty: 
            return df_pdf_bloco
        
        df = df_pdf_bloco.copy()
        if "__ROW_ID__" not in df.columns:
            df = df.reset_index(drop=True)
            df["__ROW_ID__"] = np.arange(len(df))
        
        for col in ("Origem", "Destino"):
            if col in df.columns: 
                df[col] = df[col].apply(std_text)
        
        noca_list = df['Documento'].astype(str).dropna().unique().tolist()
        
        # 2. LÓGICA DOS CTCS E PESOS (Busca otimizada no DB)
        df_ctcs = get_ctcs(noca_list) 
        if not df_ctcs.empty:
            df = pd.merge(df, df_ctcs, on="Documento", how="left")
        else:
            df["CTCs"] = None 
            
        # --- MUDANÇA 3: Busca TODOS os pesos do DB de uma vez ---
        df_peso_ctc = get_ctc_peso(noca_list) 
        if not df_peso_ctc.empty:
            df = pd.merge(df, df_peso_ctc, on="Documento", how="left")
            # A linha df.rename(...) foi REMOVIDA, pois a função get_ctc_peso já retorna os nomes corretos
        else:
            # Garante que as colunas existam mesmo se não houver dados no DB
            df["Peso_Taxado_CTC"] = np.nan
            df["Peso_Bruto_CTC"] = np.nan
            df["PesoUsado_CIA"] = np.nan
            df["TipoPeso_CIA"] = None
            
        # 3. BUSCA DE TARIFA (Lógica inalterada)
        df_norm = pd.merge(df, df_acordos_tidy, on=["Origem", "Destino"], how="left", validate="m:1")
        df_norm["__EH_DEV__"] = False

        # 4. BUSCA DEVOLUÇÃO (Lógica inalterada)
        faltou_tarifa_unit = df_norm.get("Frete_Acordo", pd.Series(dtype=float)).isna()
        if faltou_tarifa_unit.any():
            subset_ids = df_norm.loc[faltou_tarifa_unit, "__ROW_ID__"].to_numpy()
            subset = df[df["__ROW_ID__"].isin(subset_ids)].copy()
            inv = pd.merge(
                subset, df_acordos_tidy, left_on=["Origem", "Destino"],
                right_on=["Destino", "Origem"], how="left",
                validate="m:1", suffixes=("", "__DEV")
            )
            acordo_cols = [c for c in df_acordos_tidy.columns if c not in ("Origem", "Destino")]
            if acordo_cols:
                inv_small = inv[["__ROW_ID__"] + acordo_cols].copy()
                inv_small.rename(columns={c: f"{c}__DEV" for c in acordo_cols}, inplace=True)
                df_norm = pd.merge(df_norm, inv_small, on="__ROW_ID__", how="left")
                dev_found = df_norm.get("Frete_Acordo__DEV", pd.Series(dtype=float)).notna()
                for c in acordo_cols:
                    dc = f"{c}__DEV"
                    if dc in df_norm.columns:
                        df_norm[c] = np.where(df_norm[c].isna() & dev_found, df_norm[dc], df_norm[c])
                df_norm.loc[dev_found, "__EH_DEV__"] = True
                dev_found_and_missing_source = dev_found & df_norm["Fonte_Tarifa"].isna()
                df_norm["Fonte_Tarifa"] = np.where(
                    dev_found_and_missing_source, "tarifa [devolucao]", df_norm["Fonte_Tarifa"]
                )
                drop_aux = [f"{c}__DEV" for c in acordo_cols if f"{c}__DEV" in df_norm.columns]
                if drop_aux: 
                    df_norm.drop(columns=drop_aux, inplace=True, errors="ignore")
            
        # 5. Realiza os cálculos de comparação
        # --- MUDANÇA 4: Garante que todas as colunas de peso sejam numéricas ---
        df_final = to_numeric_cols(
            df_norm, 
            [
                "Frete_Peso", "Frete_Acordo", "Vlr Frete", "Peso Taxado", 
                "Peso_Taxado_CTC", "Peso_Bruto_CTC", "PesoUsado_CIA"
            ]
        )
        
        df_final["Vlr_Frete_Peso_Tabela"] = df_final["Frete_Acordo"] 
        
        # --- CÁLCULO ORIGINAL MANTIDO ---
        # O cálculo do frete da tabela continua usando o "Peso Taxado" do PDF.
        df_final["Frete_Tabela"] = df_final["Vlr_Frete_Peso_Tabela"] * df_final["Peso Taxado"]
        
        # --- CÁLCULOS DE DIFERENÇA (com uma alteração) ---
        df_final["Diferenca_Frete"] = df_final["Vlr Frete"] - df_final["Frete_Tabela"]
        df_final["Diferenca_Frete_Peso"] = df_final["Frete_Peso"] - df_final["Vlr_Frete_Peso_Tabela"]
        
        # --- MUDANÇA 5: O cálculo de Diferenca_Peso agora usa o PesoUsado_CIA ---
        df_final["Diferenca_Peso"] = df_final["Peso Taxado"] - df_final.get("PesoUsado_CIA", 0) 
        
        # Lógica de Dif_% mantida
        df_final["Dif_%"] = np.where(
            df_final["Frete_Tabela"].notna() & (df_final["Frete_Tabela"] != 0), 
            (df_final["Vlr Frete"] / df_final["Frete_Tabela"] - 1.0) * 100.0, 
            np.nan
        )
        
        # 6. Define o Status da linha (Lógica inalterada)
        tol = float(tolerancia_pct)
        has_tarifa = df_final["Vlr_Frete_Peso_Tabela"].notna()
        is_explicit_dev = df_final["__EH_DEV__"].eq(True)
        df_final["Status"] = np.select(
            [ ~has_tarifa, is_explicit_dev, (df_final["Dif_%"].abs() <= tol) & has_tarifa ],
            [ "TARIFA NAO LOCALIZADA", "DEVOLUCAO", "DENTRO DA TOLERANCIA" ],
            default="FORA DA TOLERANCIA"
        )
        
        df_final["Observacao"] = np.where(
            ~has_tarifa, "Sem tarifa para (Origem, Destino).", ""
        )

        # 7. Trata o caso especial de "frete mínimo" (Lógica inalterada)
        minimo_mask = df_final["Vlr Frete"].notna() & (df_final["Vlr Frete"] <= 60)
        if minimo_mask.any():
            df_final.loc[minimo_mask, ["Diferenca_Frete", "Diferenca_Frete_Peso", "Dif_%"]] = np.nan
            df_final.loc[minimo_mask, "Status"] = "FRETE MINIMO"
            df_final.loc[minimo_mask, "Observacao"] = "Frete minimo aplicado (Vlr Frete ≤ 60)."
            
        df_final.drop(columns=['__EH_DEV__', '__TRAT_CODE__'], inplace=True, errors="ignore") 
            
        return df_final.reset_index(drop=True)