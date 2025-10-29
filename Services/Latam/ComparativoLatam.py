from __future__ import annotations
"""C\Programs\Aéreo-Comparativos\Services\Latam\ComparativoLatam.py

Comparador de fretes LATAM, com variáveis e comentários mais claros, mantendo a mesma API
(e comportamento externo) do código atual.

- Mantidos: classe `LatamFreightComparer`, métodos públicos e colunas de saída.
- Melhorias: nomes locais explícitos, blocos por etapas, docstrings, type hints,
  comentários sobre decisões, validações e tratamento de NaN.
"""

from typing import Tuple
import numpy as np
import pandas as pd

# App config
from Config import Appconfig

# Helpers
from Utils.Numeric_Helpers import to_numeric_cols
from Utils.DataFrame_Helpers import sanitize_header, sanitize_and_dedupe_columns
from Utils.Parse import std_text

# Repositórios
from Repositories.Repositorio_TabelasFretesLatam import ProcessarTabelaLatam
from Repositories.Db_Queries import get_tipo_servico, get_ctcs, get_ctc_peso


class LatamFreightComparer:
    """
    Orquestra a comparação de fretes da LATAM:
    - Fatura (PDF já parseado) vs. Tabelas de acordos (XLSX) vs. DB (CTCs, pesos e tipo de serviço)
    - Fallbacks: JUN/RES -> VELOZ (faixas) -> PADRÃO
    """

    # Aliases IATA
    SAO_IATAS = ["CGH", "GRU", "VCP"]
    BR_IATA_ALIAS = "BR"
    SAO_IATA_ALIAS = "SAO"

    def __init__(self, cfg: Appconfig) -> None:
        self.cfg = cfg

    # ---------------------------------------------------------------------
    # PÓS-PROCESSAMENTO: organiza DataFrames finais de exportação e exibição
    # ---------------------------------------------------------------------
    def _finalize_dataframe(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Prepara os DataFrames finais: exportação (técnico) e exibição (formatado)."""
        if df.empty:
            return df.copy(), df.copy()

        # Remove linhas sem Documento (inconsistência upstream)
        if "Documento" in df.columns:
            has_doc = df["Documento"].notna() & (df["Documento"].astype(str).str.strip() != "")
            df = df[has_doc].copy()
        if df.empty:
            return df.copy(), df.copy()

        # Colunas colaterais de merges
        df = df.drop(columns=[
            "Frete_Minimo",
            "Origem_x", "Destino_x",
            "Origem_y", "Destino_y",
        ], errors="ignore")

        # Ordenação base de colunas, preservando o restante
        col_order = [
            "Tipo_Serviço", "Origem", "Destino", "Data", "Documento", "CTCs",
            "Valor_Frete", "Valor_Tarifa",
            "Valor_Frete_Tabela", "Valor_Tarifa_Tabela", "FreteMinRota", "Data_Efetivacao_Tarifa",
            "Diferenca_Frete", "Diferenca_Tarifa", "Dif_%",
            "Peso Taxado", "Peso_Taxado_CTC", "Peso_Bruto_CTC",
            "PesoUsado_CIA", "TipoPeso_CIA", "Diferenca_Peso",
            "Status", "Fonte_Tarifa", "Observacao",
        ]
        first = [c for c in col_order if c in df.columns]
        tail = [c for c in df.columns if c not in first and not str(c).startswith("__")]
        df_processed = df[first + tail].copy()

        # Arredondamento numérico técnico
        num_cols = [
            "Valor_Frete", "Valor_Tarifa", "Peso Taxado", "Valor_Tarifa_Tabela", "FreteMinRota",
            "Valor_Frete_Tabela", "Diferenca_Frete", "Diferenca_Tarifa",
            "Peso_Taxado_CTC", "Peso_Bruto_CTC", "PesoUsado_CIA", "Diferenca_Peso",
        ]
        for col in num_cols:
            if col in df_processed.columns:
                df_processed[col] = pd.to_numeric(df_processed[col], errors="coerce").round(2)
        if "Dif_%" in df_processed.columns:
            df_processed["Dif_%"] = pd.to_numeric(df_processed["Dif_%"], errors="coerce").round(2)

        df_processed = sanitize_and_dedupe_columns(df_processed)

        # Exibição amigável
        df_display = df_processed.copy()
        if "Dif_%" in df_display.columns:
            epsilon = getattr(self.cfg.tuning, "EPSILON", 0)
            has_signal = df_display["Dif_%"].abs() > epsilon
            df_display["Dif_%"] = np.where(
                pd.notna(df_display["Dif_%"]) & has_signal,
                df_display["Dif_%"].apply(lambda x: f"{x:,.2f}%".replace(",", "X").replace(".", ",").replace("X", ".")),
                "—",
            )

        for col in num_cols:
            if col in df_display.columns:
                df_display[col] = df_display[col].apply(
                    lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if pd.notna(x) else "—"
                )

        if "Data" in df_display.columns:
            df_display["Data"] = pd.to_datetime(df_display["Data"]).dt.strftime("%d/%m/%Y").replace("NaT", "—")
        if "Data_Efetivacao_Tarifa" in df_display.columns:
            df_display["Data_Efetivacao_Tarifa"] = (
                pd.to_datetime(df_display["Data_Efetivacao_Tarifa"]).dt.strftime("%d/%m/%Y").replace("NaT", "—")
            )

        # Exportação técnica: Dif_% como fração e headers saneados
        df_export = df_processed.copy()
        if "Dif_%" in df_export.columns:
            df_export["Dif_%"] = pd.to_numeric(df_export["Dif_%"], errors="coerce") / 100.0
        df_export.columns = [sanitize_header(c) for c in df_export.columns]

        return df_export, df_display

    # ---------------------------------------------------------------------
    # ENRIQUECIMENTO: tipo de serviço vindo do DB
    # ---------------------------------------------------------------------
    def _inject_service_type_from_db(self, df: pd.DataFrame) -> pd.DataFrame:
        """Mistura o tipo de serviço do DB quando disponível."""
        nocas = df["Documento"].astype(str).dropna().unique().tolist()
        df_serv = get_tipo_servico(nocas)  # Esperado: [Documento, Tipo_Servico]
        if df_serv.empty:
            return df

        df_serv = df_serv.rename(columns={"Tipo_Servico": "Tipo_Servico_DB"})
        df = pd.merge(df, df_serv, on="Documento", how="left")
        df["Tipo_Serviço"] = np.where(
            df["Tipo_Servico_DB"].notna() & (df["Tipo_Servico_DB"] != ""),
            df["Tipo_Servico_DB"],
            df["Tipo_Serviço"],
        )
        return df.drop(columns=["Tipo_Servico_DB"], errors="ignore")

    # ---------------------------------------------------------------------
    # PONTO DE ENTRADA: compara faturas com acordos + fallbacks
    # ---------------------------------------------------------------------
    def compare_fretes(self, df_fatura: pd.DataFrame, acordos_xlsx_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if df_fatura.empty:
            return pd.DataFrame(), pd.DataFrame()

        df_in = df_fatura.copy().reset_index(drop=True)
        df_in["__ROW_ID__"] = np.arange(len(df_in))
        df_in = self._inject_service_type_from_db(df_in)

        # Carrega tabelas de tarifas
        try:
            processador = ProcessarTabelaLatam(acordos_xlsx_path)
            df_tarifa_bases = processador.processar_servicos_bases()
            df_tarifa_bases["Fonte_Tarifa"] = "JUN E RES"

            try:
                df_tarifa_veloz = processador.processar_servico_veloz()
                df_tarifa_veloz["Fonte_Tarifa"] = "VELOZ"
            except Exception as e:  # noqa: BLE001
                print(f"Aviso: falha ao ler VELOZ: {e}")
                df_tarifa_veloz = pd.DataFrame()

            try:
                df_tarifa_padrao = ProcessarTabelaLatam.processar_tabelas_padrao()
                if not df_tarifa_padrao.empty:
                    df_tarifa_padrao = df_tarifa_padrao.rename(columns={"Fonte_Arquivo": "Fonte_Tarifa"})
            except Exception as e:  # noqa: BLE001
                print(f"Aviso: falha ao ler PADRÃO: {e}")
                df_tarifa_padrao = pd.DataFrame()

        except Exception as e:  # noqa: BLE001
            print(f"Erro ao processar a planilha de acordos: {e}")
            df_in["Status"] = "ERRO"
            df_in["Observacao"] = f"Não foi possível ler a planilha de acordos: {e}"
            return self._finalize_dataframe(df_in)

        # Core
        df_raw = self._comparar_bloco(
            df_fatura=df_in,
            df_acordos=df_tarifa_bases,
            df_veloz=df_tarifa_veloz,
            df_padrao=df_tarifa_padrao,
        )
        return self._finalize_dataframe(df_raw)

    # ---------------------------------------------------------------------
    # MATCH: VELOZ (com faixas de peso e aliases SAO/BR)
    # ---------------------------------------------------------------------
    def _match_veloz(self, df_fatura: pd.DataFrame, df_veloz_raw: pd.DataFrame) -> pd.DataFrame:
        if df_fatura.empty or df_veloz_raw.empty:
            return pd.DataFrame()

        # Normaliza fatura para aliases SAO
        df_f = df_fatura.copy()
        df_f["Origem"] = df_f["Origem"].replace(self.SAO_IATAS, self.SAO_IATA_ALIAS)
        df_f["Destino"] = df_f["Destino"].replace(self.SAO_IATAS, self.SAO_IATA_ALIAS)

        # Normaliza planilha veloz
        df_v = df_veloz_raw.copy().rename(columns={"Tipo_Servico": "Tipo_Serviço"})
        if "Tipo_Serviço" in df_v.columns:
            df_v["Tipo_Serviço"] = df_v["Tipo_Serviço"].apply(std_text)
        for col in ("Origem", "Destino"):
            if col in df_v.columns:
                df_v[col] = df_v[col].astype(str).apply(std_text)
                df_v[col] = df_v[col].replace("SAO PAULO", self.SAO_IATA_ALIAS)
                df_v[col] = df_v[col].replace("BRASIL", self.BR_IATA_ALIAS)

        # Estágios de match tratando BR
        keys = ["Origem", "Destino", "Tipo_Serviço"]
        s1 = pd.merge(df_f, df_v, on=keys, how="left")
        s1_unmatched_ids = s1[s1["Data_Efetivacao_Tarifa"].isna()]["__ROW_ID__"]
        f2 = df_f[df_f["__ROW_ID__"].isin(s1_unmatched_ids)].copy()

        v_br_as_origem = df_v[df_v["Origem"] == self.BR_IATA_ALIAS].drop(columns="Origem")
        s2 = pd.merge(f2, v_br_as_origem, on=["Destino", "Tipo_Serviço"], how="left")
        s2_unmatched_ids = s2[s2["Data_Efetivacao_Tarifa"].isna()]["__ROW_ID__"]
        f3 = f2[f2["__ROW_ID__"].isin(s2_unmatched_ids)].copy()

        v_br_as_destino = df_v[df_v["Destino"] == self.BR_IATA_ALIAS].drop(columns="Destino")
        s3 = pd.merge(f3, v_br_as_destino, on=["Origem", "Tipo_Serviço"], how="left")

        got1 = s1[s1["Data_Efetivacao_Tarifa"].notna()]
        got2 = s2[s2["Data_Efetivacao_Tarifa"].notna()]
        got3 = s3[s3["Data_Efetivacao_Tarifa"].notna()]
        all_found = pd.concat([got1, got2, got3], ignore_index=True)
        if all_found.empty:
            return pd.DataFrame()

        # Escolha por data: passado mais recente ou futuro mais próximo
        past = all_found[all_found["Data_Efetivacao_Tarifa"] <= all_found["Data"]].copy()
        future = all_found[all_found["Data_Efetivacao_Tarifa"] > all_found["Data"]]
        best_past = past.sort_values("Data_Efetivacao_Tarifa", ascending=False).drop_duplicates("__ROW_ID__", keep="first")
        best_future = future.sort_values("Data_Efetivacao_Tarifa", ascending=True).drop_duplicates("__ROW_ID__", keep="first")
        best = pd.concat([best_past, best_future]).drop_duplicates("__ROW_ID__", keep="first")
        if best.empty:
            return pd.DataFrame()

        # Mapa de faixas
        raw_cols = [c for c in best.columns if isinstance(c, str) and pd.Series(c).str.match(r"^\d+(p\d+)?\+$").any()]
        if not raw_cols:
            print("Aviso: VELOZ sem colunas de faixa de peso.")
            return pd.DataFrame()

        weight_cols: list[tuple[float, str]] = []
        for c in raw_cols:
            try:
                weight_cols.append((float(c.replace("p", ".").replace("+", "")), c))
            except ValueError:
                pass
        weight_cols.sort(key=lambda x: x[0], reverse=True)

        def _pick_veloz_tariff(row: pd.Series) -> pd.Series:
            peso = row.get("Peso Taxado")
            out_idx = ["Valor_Tarifa_Acordo", "Faixa_Peso_Usada", "__Status_Veloz"]
            if pd.isna(peso):
                return pd.Series([np.nan, np.nan, np.nan], index=out_idx)
            if peso > 30:  # regra informada pelo usuário
                return pd.Series([np.nan, np.nan, "PESO EXCEDENTE"], index=out_idx)
            for lim, col_name in weight_cols:
                if peso >= lim:
                    return pd.Series([row[col_name], col_name, np.nan], index=out_idx)
            return pd.Series([np.nan, np.nan, "FAIXA NAO LOCALIZADA"], index=out_idx)

        details = best.apply(_pick_veloz_tariff, axis=1)
        best["Valor_Tarifa_Acordo"] = details["Valor_Tarifa_Acordo"]
        best["Faixa_Peso_Usada"] = details["Faixa_Peso_Usada"]
        best["__Status_Veloz"] = details["__Status_Veloz"]

        keep = ["__ROW_ID__", "Valor_Tarifa_Acordo", "Faixa_Peso_Usada", "Data_Efetivacao_Tarifa", "Fonte_Tarifa", "__Status_Veloz"]
        if "Frete_Minimo" in best.columns:
            keep.append("Frete_Minimo")
        return best[keep].copy()

    # ---------------------------------------------------------------------
    # MATCH: PADRÃO (sem data e sem faixas; usa aliases SAO/BR)
    # ---------------------------------------------------------------------
    def _match_padrao(self, df_fatura: pd.DataFrame, df_padrao_raw: pd.DataFrame) -> pd.DataFrame:
        if df_fatura.empty or df_padrao_raw.empty:
            return pd.DataFrame()

        df_f = df_fatura.copy()
        df_f["Origem"] = df_f["Origem"].replace(self.SAO_IATAS, self.SAO_IATA_ALIAS)
        df_f["Destino"] = df_f["Destino"].replace(self.SAO_IATAS, self.SAO_IATA_ALIAS)

        df_p = df_padrao_raw.copy().rename(columns={"Tipo_Servico": "Tipo_Serviço", "Valor_Tarifa": "Valor_Tarifa_Acordo"})
        if "Tipo_Serviço" in df_p.columns:
            df_p["Tipo_Serviço"] = df_p["Tipo_Serviço"].apply(std_text)
        for col in ("Origem", "Destino"):
            if col in df_p.columns:
                df_p[col] = df_p[col].astype(str).replace("SAO PAULO", self.SAO_IATA_ALIAS).replace("BRASIL", self.BR_IATA_ALIAS)

        keys = ["Origem", "Destino", "Tipo_Serviço"]
        s1 = pd.merge(df_f, df_p, on=keys, how="left")
        s1_unmatched_ids = s1[s1["Fonte_Tarifa"].isna()]["__ROW_ID__"]
        f2 = df_f[df_f["__ROW_ID__"].isin(s1_unmatched_ids)].copy()

        p_br_as_origem = df_p[df_p["Origem"] == self.BR_IATA_ALIAS].drop(columns="Origem")
        s2 = pd.merge(f2, p_br_as_origem, on=["Destino", "Tipo_Serviço"], how="left")
        s2_unmatched_ids = s2[s2["Fonte_Tarifa"].isna()]["__ROW_ID__"]
        f3 = f2[f2["__ROW_ID__"].isin(s2_unmatched_ids)].copy()

        p_br_as_destino = df_p[df_p["Destino"] == self.BR_IATA_ALIAS].drop(columns="Destino")
        s3 = pd.merge(f3, p_br_as_destino, on=["Origem", "Tipo_Serviço"], how="left")

        got1 = s1[s1["Fonte_Tarifa"].notna()]
        got2 = s2[s2["Fonte_Tarifa"].notna()]
        got3 = s3[s3["Fonte_Tarifa"].notna()]
        all_found = pd.concat([got1, got2, got3], ignore_index=True)
        if all_found.empty:
            return pd.DataFrame()

        best = all_found.drop_duplicates("__ROW_ID__", keep="first")
        keep = ["__ROW_ID__", "Valor_Tarifa_Acordo", "Fonte_Tarifa"]
        if "Frete_Minimo" in best.columns:
            keep.append("Frete_Minimo")
        return best[keep].copy()

    # ---------------------------------------------------------------------
    # CORE: merge principal + fallbacks + cálculos finais
    # ---------------------------------------------------------------------
    def _comparar_bloco(
        self,
        df_fatura: pd.DataFrame,
        df_acordos: pd.DataFrame,
        df_veloz: pd.DataFrame,
        df_padrao: pd.DataFrame,
    ) -> pd.DataFrame:
        if df_fatura.empty:
            return df_fatura

        df = df_fatura.copy()

        # Normalização mínima
        if "Data" in df.columns:
            df["Data"] = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
        for col in ("Origem", "Destino", "Tipo_Serviço"):
            if col in df.columns:
                df[col] = df[col].apply(std_text)

        # Prepara acordos base
        df_a = df_acordos.copy().rename(columns={"Tipo_Servico": "Tipo_Serviço", "Valor_Tarifa": "Valor_Tarifa_Acordo"})
        for col in ("Origem", "Destino", "Tipo_Serviço"):
            if col in df_a.columns:
                df_a[col] = df_a[col].apply(std_text)

        # Etapa 1: JUN/RES (ida e fallback de volta)
        ida = pd.merge(df, df_a, on=["Origem", "Destino", "Tipo_Serviço"], how="left")
        ida_past = ida[ida["Data_Efetivacao_Tarifa"] <= ida["Data"]].copy()
        ida_future = ida[ida["Data_Efetivacao_Tarifa"] > ida["Data"]]
        ida_best_past = ida_past.drop_duplicates("__ROW_ID__", keep="first")
        ida_best_future = ida_future.drop_duplicates("__ROW_ID__", keep="first")
        ida_best = pd.concat([ida_best_past, ida_best_future]).drop_duplicates("__ROW_ID__", keep="first")
        ida_best["__EH_DEV__"] = False

        still_unmatched = df[~df["__ROW_ID__"].isin(ida_best["__ROW_ID__"])]
        if not still_unmatched.empty:
            volta = pd.merge(
                still_unmatched,
                df_a,
                left_on=["Destino", "Origem", "Tipo_Serviço"],
                right_on=["Origem", "Destino", "Tipo_Serviço"],
                how="left",
            )
            volta_past = volta[volta["Data_Efetivacao_Tarifa"] <= volta["Data"]].copy()
            volta_future = volta[volta["Data_Efetivacao_Tarifa"] > volta["Data"]]
            volta_best_past = volta_past.drop_duplicates("__ROW_ID__", keep="first")
            volta_best_future = volta_future.drop_duplicates("__ROW_ID__", keep="first")
            volta_best = pd.concat([volta_best_past, volta_best_future]).drop_duplicates("__ROW_ID__", keep="first")
            volta_best["__EH_DEV__"] = True
            jun_res_matches = pd.concat([ida_best, volta_best])
        else:
            jun_res_matches = ida_best

        # Etapa 2: VELOZ fallback
        matched_ids_s1 = jun_res_matches["__ROW_ID__"]
        need_veloz = df[~df["__ROW_ID__"].isin(matched_ids_s1)].copy()
        veloz_matches = self._match_veloz(need_veloz, df_veloz) if (not need_veloz.empty and not df_veloz.empty) else pd.DataFrame()
        s1_s2 = pd.concat([jun_res_matches, veloz_matches])

        # Etapa 3: PADRÃO fallback
        matched_ids_s2 = s1_s2["__ROW_ID__"]
        need_padrao = df[~df["__ROW_ID__"].isin(matched_ids_s2)].copy()
        padrao_matches = self._match_padrao(need_padrao, df_padrao) if (not need_padrao.empty and not df_padrao.empty) else pd.DataFrame()
        all_matches = pd.concat([s1_s2, padrao_matches])

        # Merge final (traz __Status_Veloz e Faixa_Peso_Usada quando houver)
        cols_add = [c for c in all_matches.columns if c not in df.columns or c == "__ROW_ID__"]
        out = pd.merge(df, all_matches[cols_add], on="__ROW_ID__", how="left")

        # Enriquecimento com DB
        df_ctc = get_ctcs(out["Documento"].astype(str).dropna().unique().tolist())
        df_ctc_peso = get_ctc_peso(out["Documento"].astype(str).dropna().unique().tolist())
        if not df_ctc.empty:
            out = pd.merge(out, df_ctc, on="Documento", how="left")
        if not df_ctc_peso.empty:
            out = pd.merge(out, df_ctc_peso, on="Documento", how="left")

        # Tipagem numérica
        cols_num_convert = [
            "Valor_Tarifa", "Valor_Frete", "Peso Taxado", "Valor_Tarifa_Acordo",
            "Frete_Minimo", "Peso_Taxado_CTC", "Peso_Bruto_CTC", "PesoUsado_CIA",
        ]
        out = to_numeric_cols(out, cols_num_convert)

        # Renomeações finais de tarifa
        out["FreteMinRota"] = out["Frete_Minimo"]
        out = out.rename(columns={"Valor_Tarifa_Acordo": "Valor_Tarifa_Tabela"})

        # Enriquecimento da "Fonte_Tarifa" para VELOZ
        is_veloz = out["Fonte_Tarifa"].eq("VELOZ")
        faixa = out["Faixa_Peso_Usada"].fillna("N/A").astype(str)
        valor = out["Valor_Tarifa_Tabela"].map(lambda v: f"{v:.2f}" if pd.notna(v) else "N/A")
        out["Fonte_Tarifa"] = np.where(is_veloz, "VELOZ (" + faixa + " | R$ " + valor + ")", out["Fonte_Tarifa"])

        # Cálculos principais
        frete_calculado = out["Valor_Tarifa_Tabela"] * out["Peso Taxado"]
        frete_min_fallback = out["Frete_Minimo"].fillna(0)
        out["Valor_Frete_Tabela"] = np.maximum(frete_calculado, frete_min_fallback)

        out["Diferenca_Frete"] = out["Valor_Frete"] - out["Valor_Frete_Tabela"]
        out["Diferenca_Tarifa"] = out["Valor_Tarifa"] - out["Valor_Tarifa_Tabela"]
        out["Diferenca_Peso"] = out["Peso Taxado"] - out.get("PesoUsado_CIA", 0)
        out["Dif_%"] = np.where(
            out["Valor_Frete_Tabela"].notna() & (out["Valor_Frete_Tabela"] != 0),
            (out["Valor_Frete"] / out["Valor_Frete_Tabela"] - 1.0) * 100.0,
            np.nan,
        )

        # Status
        has_tarifa = out["Valor_Tarifa_Tabela"].notna()
        is_dev = out["__EH_DEV__"].eq(True)
        is_frete_min = (out["Valor_Frete"] <= out["Frete_Minimo"]) & has_tarifa
        is_peso_excedente = out.get("__Status_Veloz", pd.Series(index=out.index)).fillna("").eq("PESO EXCEDENTE")

        out["Status"] = np.select(
            [is_frete_min, is_dev, is_peso_excedente, ~has_tarifa],
            ["FRETE MINIMO", "DEVOLUCAO", "PESO EXCEDENTE", "TARIFA NAO LOCALIZADA"],
            default="COBRADO - TARIFADO",
        )

        # Zera difs em frete mínimo ou peso excedente
        mask_zero = is_frete_min | is_peso_excedente
        out.loc[mask_zero, ["Diferenca_Frete", "Diferenca_Tarifa", "Dif_%", "Valor_Frete_Tabela"]] = np.nan

        # Observações
        out["Observacao"] = np.where(
            is_frete_min,
            "Valor cobrado é igual ou inferior ao frete mínimo.",
            np.where(
                is_peso_excedente,
                "Peso taxado excede o limite de 30kg para o serviço Veloz.",
                np.where(~has_tarifa, "Sem tarifa para (Origem, Destino, Tipo de Serviço).", out.get("Observacao", "")),
            ),
        )

        # Limpeza de auxiliares
        out = out.drop(columns=["__EH_DEV__", "Faixa_Peso_Usada", "__Status_Veloz"], errors="ignore")
        return out.reset_index(drop=True)
