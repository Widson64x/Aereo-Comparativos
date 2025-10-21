from __future__ import annotations
import pandas as pd
import numpy as np

# Importa as configurações do aplicativo
from Config import Appconfig
# Importa as funções de helpers
from Utils.Numeric_Helpers import to_numeric_cols
from Utils.DataFrame_Helpers import sanitize_header, sanitize_and_dedupe_columns
from Utils.Parse import std_text

# <-- 1. ALTERAÇÃO AQUI: Importa a CLASSE ao invés da função -->
from Repositories.Repositorio_TabelasFretesLatam import ProcessarTabelaLatam
from Repositories.Db_Queries import get_tipo_servico, get_ctcs, get_ctc_peso

class LatamFreightComparer:
    """
    Orquestra a lógica de comparação de fretes, cruzando dados da fatura (PDF)
    com as tarifas da planilha de acordos (XLSX) e informações do banco de dados.
    """

    def __init__(self, cfg: Appconfig):
        """Inicializa o comparador com as configurações do aplicativo."""
        self.cfg = cfg
        # <-- NOVO: Constantes para regras de IATA -->
        self.SAO_IATAS = ['CGH', 'GRU', 'VCP']
        self.BR_IATA_ALIAS = 'BR' # Alias para "Todas as IATAS"
        self.SAO_IATA_ALIAS = 'SAO' # Alias para 'CGH', 'GRU', 'VCP'

    def _finalize_dataframe(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Prepara os DataFrames finais: um para exportação e outro para exibição.
        """
        if df.empty:
            return df.copy(), df.copy()

        # <-- MUDANÇA SOLICITADA: Remover linhas sem 'Documento' -->
        if 'Documento' in df.columns:
            # Garante que o Documento não seja NaN, None, ou uma string vazia/espaços
            is_valid_doc = df['Documento'].notna() & (df['Documento'].astype(str).str.strip() != '')
            df = df[is_valid_doc].copy()
        
        # Repete a verificação de "empty" caso o filtro tenha removido tudo
        if df.empty:
            return df.copy(), df.copy()

        cols_to_drop = [
            "Frete_Minimo", 
            'Origem_x', 'Destino_x',  # <-- CORRIGIDO AQUI (minúsculo)
            'Origem_y', 'Destino_y'   # <-- CORRIGIDO AQUI (minúsculo)
        ]
        df = df.drop(columns=cols_to_drop, errors='ignore')
        # <-- FIM DA CORREÇÃO -->

        col_order = [
            "Tipo_Serviço", "Origem", "Destino", "Data", "Documento", "CTCs",
            "Valor_Frete", "Valor_Tarifa",
            "Valor_Frete_Tabela", "Valor_Tarifa_Tabela", "FreteMinRota", "Data_Efetivacao_Tarifa",
            "Diferenca_Frete", "Diferenca_Tarifa", "Dif_%",
            "Peso Taxado", "Peso_Taxado_CTC", "Peso_Bruto_CTC",
            "PesoUsado_CIA", "TipoPeso_CIA", "Diferenca_Peso",
            "Status", "Fonte_Tarifa", "Observacao",
        ]
        existing_cols = [c for c in col_order if c in df.columns]
        other_cols = [c for c in df.columns if c not in existing_cols and not str(c).startswith("__")]
        df_processed = df[existing_cols + other_cols].copy()

        numeric_cols_to_round = [
            "Valor_Frete", "Valor_Tarifa", "Peso Taxado", "Valor_Tarifa_Tabela", "FreteMinRota",
            "Valor_Frete_Tabela", "Diferenca_Frete", "Diferenca_Tarifa",
            "Peso_Taxado_CTC", "Peso_Bruto_CTC", "PesoUsado_CIA", "Diferenca_Peso"
        ]
        
        for col in numeric_cols_to_round:
            if col in df_processed.columns:
                df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce').round(2)
        
        if "Dif_%" in df_processed.columns:
            df_processed["Dif_%"] = pd.to_numeric(df_processed["Dif_%"], errors='coerce').round(2)

        df_processed = sanitize_and_dedupe_columns(df_processed)

        # DATAFRAME PARA EXIBIÇÃO
        df_for_display = df_processed.copy()
        if "Dif_%" in df_for_display.columns:
            is_meaningful = df_for_display["Dif_%"].abs() > self.cfg.tuning.EPSILON
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
        
        if 'Data' in df_for_display.columns:
                df_for_display['Data'] = pd.to_datetime(df_for_display['Data']).dt.strftime('%d/%m/%Y').replace('NaT', '—')
        if 'Data_Efetivacao_Tarifa' in df_for_display.columns:
            df_for_display['Data_Efetivacao_Tarifa'] = pd.to_datetime(df_for_display['Data_Efetivacao_Tarifa']).dt.strftime('%d/%m/%Y').replace('NaT', '—')

        # DATAFRAME PARA EXPORTAÇÃO
        df_for_export = df_processed.copy()
        if "Dif_%" in df_for_export.columns:
            df_for_export["Dif_%"] = pd.to_numeric(df_for_export["Dif_%"], errors="coerce") / 100.0
        df_for_export.columns = [sanitize_header(col) for col in df_for_export.columns]
        
        return df_for_export, df_for_display

    def _process_service_type_from_db(self, df: pd.DataFrame) -> pd.DataFrame:
        noca_list = df['Documento'].astype(str).dropna().unique().tolist()
        df_servico_db = get_tipo_servico(noca_list) # DataFrame com colunas: Documento, Tipo_Servico
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

    def compare_fretes(self, df_fatura: pd.DataFrame, acordos_xlsx_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        if df_fatura.empty:
            return pd.DataFrame(), pd.DataFrame()

        df = df_fatura.copy().reset_index(drop=True)
        df["__ROW_ID__"] = np.arange(len(df))
        
        df = self._process_service_type_from_db(df)

        try:
            # <-- 2. ALTERAÇÃO AQUI: Usa a nova classe -->
            # Primeiro, cria a instância do processador
            processador = ProcessarTabelaLatam(acordos_xlsx_path)
            
            # Depois, chama o método para obter o DataFrame "JUN E RES"
            df_acordos = processador.processar_servicos_bases()
            df_acordos["Fonte_Tarifa"] = "JUN E RES"
            
            # <-- NOVO: Processa também o serviço VELOZ -->
            try:
                df_veloz = processador.processar_servico_veloz()
                df_veloz["Fonte_Tarifa"] = "VELOZ" # Adiciona a fonte
            except Exception as e:
                print(f"Aviso: Não foi possível processar a aba do serviço veloz: {e}")
                df_veloz = pd.DataFrame()

        except Exception as e:
            print(f"Erro ao processar a planilha de acordos: {e}")
            df["Status"] = "ERRO"
            df["Observacao"] = f"Não foi possível ler a planilha de acordos: {e}"
            return self._finalize_dataframe(df)

        # <-- NOVO: Passa ambos os dataframes de tarifa -->
        df_raw_final = self._comparar_bloco(df, df_acordos, df_veloz)
        
        df_export, df_display = self._finalize_dataframe(df_raw_final)
        return df_export, df_display

    # <-- NOVO: Método de busca para o serviço Veloz -->
    def _match_veloz(self, df_fatura: pd.DataFrame, df_veloz_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Busca tarifas na tabela 'Veloz' (PROXIMOVOO) para as faturas
        não localizadas na 'JUN E RES'.
        
        Esta função aplica a lógica de faixas de peso e aliases de IATA (SAO, BR).
        """
        if df_fatura.empty or df_veloz_raw.empty:
            return pd.DataFrame()
        
        # --- 1. PREPARAÇÃO DOS DADOS ---
        
        # Prepara df_fatura (faturas *já* padronizadas no _comparar_bloco)
        # Padroniza IATAs de SP para o alias 'SAO' para o merge
        df_fatura_prep = df_fatura.copy()
        df_fatura_prep['Origem'] = df_fatura_prep['Origem'].replace(self.SAO_IATAS, self.SAO_IATA_ALIAS)
        df_fatura_prep['Destino'] = df_fatura_prep['Destino'].replace(self.SAO_IATAS, self.SAO_IATA_ALIAS)
        
        # Prepara df_veloz (tabela de tarifas)
        df_veloz_prep = df_veloz_raw.copy()
        df_veloz_prep.rename(columns={'Tipo_Servico': 'Tipo_Serviço'}, inplace=True)
        
        # Padroniza colunas de texto, exceto IATAs que já são códigos
        for col in ("Tipo_Serviço",):
            if col in df_veloz_prep.columns: 
                df_veloz_prep[col] = df_veloz_prep[col].apply(std_text)
        
        # Padroniza Origem/Destino, tratando 'SAO PAULO' (de std_text) para 'SAO'
        for col in ("Origem", "Destino"):
            if col in df_veloz_prep.columns:
                df_veloz_prep[col] = df_veloz_prep[col].astype(str).apply(std_text)
                df_veloz_prep[col] = df_veloz_prep[col].replace('SAO PAULO', self.SAO_IATA_ALIAS)
                # Garante que 'BR' (de BRASIL) seja 'BR'
                df_veloz_prep[col] = df_veloz_prep[col].replace('BRASIL', self.BR_IATA_ALIAS)
        
        # --- 2. MERGE EM ESTÁGIOS (TRATANDO 'BR') ---
        # Chaves de merge
        keys = ["Origem", "Destino", "Tipo_Serviço"]
        
        # Estágio 1: Match exato (Origem, Destino, Tipo_Serviço)
        df_merged_s1 = pd.merge(df_fatura_prep, df_veloz_prep, on=keys, how='left')
        
        # Separa não-combinados para o próximo estágio
        unmatched_s1_ids = df_merged_s1[df_merged_s1['Data_Efetivacao_Tarifa'].isna()]['__ROW_ID__']
        unmatched_s1 = df_fatura_prep[df_fatura_prep['__ROW_ID__'].isin(unmatched_s1_ids)].copy()
        
        # Estágio 2: Match (BR, Destino, Tipo_Serviço)
        br_orig_tariffs = df_veloz_prep[df_veloz_prep['Origem'] == self.BR_IATA_ALIAS].drop(columns='Origem')
        df_merged_s2 = pd.merge(unmatched_s1, br_orig_tariffs, on=['Destino', 'Tipo_Serviço'], how='left')
        
        # Separa não-combinados
        unmatched_s2_ids = df_merged_s2[df_merged_s2['Data_Efetivacao_Tarifa'].isna()]['__ROW_ID__']
        unmatched_s2 = unmatched_s1[unmatched_s1['__ROW_ID__'].isin(unmatched_s2_ids)].copy()

        # Estágio 3: Match (Origem, BR, Tipo_Serviço)
        br_dest_tariffs = df_veloz_prep[df_veloz_prep['Destino'] == self.BR_IATA_ALIAS].drop(columns='Destino')
        df_merged_s3 = pd.merge(unmatched_s2, br_dest_tariffs, on=['Origem', 'Tipo_Serviço'], how='left')
        
        # Combina todos os matches encontrados
        matched_s1 = df_merged_s1[df_merged_s1['Data_Efetivacao_Tarifa'].notna()]
        matched_s2 = df_merged_s2[df_merged_s2['Data_Efetivacao_Tarifa'].notna()]
        matched_s3 = df_merged_s3[df_merged_s3['Data_Efetivacao_Tarifa'].notna()]
        
        all_veloz_matches = pd.concat([matched_s1, matched_s2, matched_s3], ignore_index=True)

        if all_veloz_matches.empty:
            return pd.DataFrame()
            
        # --- 3. SELEÇÃO DE TARIFA PELA DATA (Igual 'JUN E RES') ---
        past_matches = all_veloz_matches[all_veloz_matches['Data_Efetivacao_Tarifa'] <= all_veloz_matches['Data']].copy()
        future_matches = all_veloz_matches[all_veloz_matches['Data_Efetivacao_Tarifa'] > all_veloz_matches['Data']]

        best_past = past_matches.sort_values(by='Data_Efetivacao_Tarifa', ascending=False).drop_duplicates(subset=['__ROW_ID__'], keep='first')
        best_future = future_matches.sort_values(by='Data_Efetivacao_Tarifa', ascending=True).drop_duplicates(subset=['__ROW_ID__'], keep='first')

        best_veloz_matches = pd.concat([best_past, best_future]).drop_duplicates(subset=['__ROW_ID__'], keep='first')
        
        if best_veloz_matches.empty:
            return pd.DataFrame()

        # --- 4. BUSCAR TARIFA PELA FAIXA DE PESO ---
        # Parse colunas de tarifa (ex: '0+', '0p5+', '10+')
        tariff_cols_raw = [col for col in best_veloz_matches.columns if isinstance(col, str) and pd.Series(col).str.match(r'^\d+(p\d+)?\+$').any()]
        if not tariff_cols_raw:
            print("Aviso: Nenhuma coluna de faixa de peso encontrada na aba Veloz.")
            return pd.DataFrame()

        # Cria mapa: (limite_peso_kg, nome_coluna)
        tariff_map = []
        for col in tariff_cols_raw:
            try:
                weight = float(col.replace('p', '.').replace('+', ''))
                tariff_map.append((weight, col))
            except ValueError:
                continue
        
        # Ordena por peso, decrescente (ex: 2.0, 1.5, 1.0, 0.5, 0.0)
        # Isso facilita a busca: achamos o *maior* limite que seja *menor ou igual* ao peso taxado.
        tariff_map.sort(key=lambda x: x[0], reverse=True)

        # <-- ALTERAÇÃO: Esta função agora retorna valor, faixa E status -->
        def find_veloz_tariff_details(row):
            peso_taxado = row['Peso Taxado']
            
            # Define o índice para a Series de retorno
            return_index = ['Valor_Tarifa_Acordo', 'Faixa_Peso_Usada', '__Status_Veloz']
            
            if pd.isna(peso_taxado):
                # Retorna nulos
                return pd.Series([np.nan, np.nan, np.nan], index=return_index)

            # <-- MUDANÇA SOLICITADA: Verifica o peso excedente ANTES de buscar -->
            if peso_taxado > 30:
                # Retorna nulo, nulo, e o novo status
                return pd.Series([np.nan, np.nan, 'PESO EXCEDENTE'], index=return_index)
            # <-- FIM DA MUDANÇA -->
                
            # Itera do maior peso para o menor
            for weight_limit, col_name in tariff_map:
                if peso_taxado >= weight_limit: # Achou a faixa correta
                    # Retorna o valor, a faixa, e status nulo (OK)
                    return pd.Series([row[col_name], col_name, np.nan], index=return_index)
            
            # Não achou faixa (ex: peso negativo ou outro caso não coberto)
            # Retorna um status específico para isso
            return pd.Series([np.nan, np.nan, 'FAIXA NAO LOCALIZADA'], index=return_index)

        # Aplica a função para obter as *três* colunas
        tariff_details = best_veloz_matches.apply(find_veloz_tariff_details, axis=1)
        best_veloz_matches['Valor_Tarifa_Acordo'] = tariff_details['Valor_Tarifa_Acordo']
        best_veloz_matches['Faixa_Peso_Usada'] = tariff_details['Faixa_Peso_Usada']
        # <-- ADICIONADO: Captura o novo status -->
        best_veloz_matches['__Status_Veloz'] = tariff_details['__Status_Veloz']
        
        # --- 5. RETORNO ---
        # Colunas necessárias para o merge principal
        # <-- ALTERAÇÃO: Adiciona '__Status_Veloz' ao retorno -->
        final_cols = ['__ROW_ID__', 'Valor_Tarifa_Acordo', 'Faixa_Peso_Usada', 'Data_Efetivacao_Tarifa', 'Fonte_Tarifa', '__Status_Veloz']
        
        if 'Frete_Minimo' in best_veloz_matches.columns:
            final_cols.append('Frete_Minimo')
            
        return best_veloz_matches[final_cols].copy()

    # <-- ALTERADO: Assinatura do método e lógica de fallback -->
    def _comparar_bloco(self, df_fatura: pd.DataFrame, df_acordos: pd.DataFrame, df_veloz: pd.DataFrame) -> pd.DataFrame:
        """
        Realiza a comparação principal com a nova lógica de seleção de tarifa por data.
        """
        if df_fatura.empty: return df_fatura
        
        df = df_fatura.copy()

        # --- PREPARAÇÃO ---
        if 'Data' in df.columns:
            df['Data'] = pd.to_datetime(df['Data'], errors='coerce', dayfirst=True)

        for col in ("Origem", "Destino", "Tipo_Serviço"):
            if col in df.columns: df[col] = df[col].apply(std_text)
        
        df_acordos_prep = df_acordos.copy()
        df_acordos_prep.rename(columns={'Tipo_Servico': 'Tipo_Serviço'}, inplace=True)
        df_acordos_prep.rename(columns={'Valor_Tarifa': 'Valor_Tarifa_Acordo'}, inplace=True)

        for col in ("Origem", "Destino", "Tipo_Serviço"):
            if col in df_acordos_prep.columns: df_acordos_prep[col] = df_acordos_prep[col].apply(std_text)

        # --- SELEÇÃO DE TARIFA (ETAPA 1: JUN E RES) ---
        df_merged_ida = pd.merge(df, df_acordos_prep, on=["Origem", "Destino", "Tipo_Serviço"], how='left')
        
        past_ida = df_merged_ida[df_merged_ida['Data_Efetivacao_Tarifa'] <= df_merged_ida['Data']].copy()
        future_ida = df_merged_ida[(df_merged_ida['Data_Efetivacao_Tarifa'] > df_merged_ida['Data'])]

        best_past_ida = past_ida.drop_duplicates(subset=['__ROW_ID__'], keep='first')
        best_future_ida = future_ida.drop_duplicates(subset=['__ROW_ID__'], keep='first')

        best_ida_matches = pd.concat([best_past_ida, best_future_ida]).drop_duplicates(subset=['__ROW_ID__'], keep='first')
        best_ida_matches['__EH_DEV__'] = False

        unmatched_ids = df[~df['__ROW_ID__'].isin(best_ida_matches['__ROW_ID__'])]
        if not unmatched_ids.empty:
            df_merged_volta = pd.merge(unmatched_ids, df_acordos_prep, left_on=["Destino", "Origem", "Tipo_Serviço"], right_on=["Origem", "Destino", "Tipo_Serviço"], how='left')
            
            past_volta = df_merged_volta[df_merged_volta['Data_Efetivacao_Tarifa'] <= df_merged_volta['Data']].copy()
            future_volta = df_merged_volta[(df_merged_volta['Data_Efetivacao_Tarifa'] > df_merged_volta['Data'])]
            
            best_past_volta = past_volta.drop_duplicates(subset=['__ROW_ID__'], keep='first')
            best_future_volta = future_volta.drop_duplicates(subset=['__ROW_ID__'], keep='first')
            
            best_volta_matches = pd.concat([best_past_volta, best_future_volta]).drop_duplicates(subset=['__ROW_ID__'], keep='first')
            best_volta_matches['__EH_DEV__'] = True
            
            all_matches_jun_res = pd.concat([best_ida_matches, best_volta_matches])
        else:
            all_matches_jun_res = best_ida_matches
            
        # SELEÇÃO DE TARIFA (ETAPA 2: VELOZ FALLBACK)
        
        # Identifica faturas ainda não localizadas
        matched_jun_res_ids = all_matches_jun_res['__ROW_ID__']
        unmatched_for_veloz_df = df[~df['__ROW_ID__'].isin(matched_jun_res_ids)].copy()

        # Se houver não localizadas E a tabela veloz existir, tenta o match
        if not unmatched_for_veloz_df.empty and not df_veloz.empty:
            df_veloz_matches = self._match_veloz(unmatched_for_veloz_df, df_veloz)
        else:
            df_veloz_matches = pd.DataFrame()
            
        # Combina os resultados de "JUN E RES" e "VELOZ"
        all_found_matches = pd.concat([all_matches_jun_res, df_veloz_matches])
        
        # --- MERGE FINAL ---
        # O merge agora trará a coluna '__Status_Veloz'
        cols_to_keep = [c for c in all_found_matches.columns if c not in df.columns or c == '__ROW_ID__']
        df_final = pd.merge(df, all_found_matches[cols_to_keep], on='__ROW_ID__', how='left')

        # --- CÁLCULOS FINAIS ---
        df_ctcs = get_ctcs(df_final['Documento'].astype(str).dropna().unique().tolist())
        df_peso_ctc = get_ctc_peso(df_final['Documento'].astype(str).dropna().unique().tolist())
        if not df_ctcs.empty: df_final = pd.merge(df_final, df_ctcs, on="Documento", how="left")
        if not df_peso_ctc.empty: df_final = pd.merge(df_final, df_peso_ctc, on="Documento", how="left")
        
        cols_to_convert = ["Valor_Tarifa", "Valor_Frete", "Peso Taxado", "Valor_Tarifa_Acordo", "Frete_Minimo", "Peso_Taxado_CTC", "Peso_Bruto_CTC", "PesoUsado_CIA"]
        df_final = to_numeric_cols(df_final, cols_to_convert)
        
        df_final["FreteMinRota"] = df_final["Frete_Minimo"]
        # Renomeia a coluna que veio de 'JUN E RES' ou 'VELOZ'
        df_final.rename(columns={'Valor_Tarifa_Acordo': 'Valor_Tarifa_Tabela'}, inplace=True)
        
        # Formata a Fonte_Tarifa ANTES de calcular 
        is_veloz = df_final['Fonte_Tarifa'] == 'VELOZ' # Identifica linhas VELOZ
        # Formata a string, tratando nulos para não quebrar
        faixa_str = df_final['Faixa_Peso_Usada'].fillna('N/A').astype(str)
        # Formata o valor da tarifa com 2 casas decimais
        valor_str = df_final['Valor_Tarifa_Tabela'].map(lambda x: f"{x:.2f}" if pd.notna(x) else 'N/A')
        
        # Cria a string formatada
        # (Se a faixa for N/A e o status for PESO EXCEDENTE, isso será refletido no status final)
        fonte_veloz_str = 'VELOZ (' + faixa_str + ' | R$ ' + valor_str + ')'
        # Atualiza a coluna Fonte_Tarifa apenas para linhas 'VELOZ'
        df_final['Fonte_Tarifa'] = np.where(is_veloz, fonte_veloz_str, df_final['Fonte_Tarifa'])
        
        # --- Continuação dos Cálculos ---
        frete_calculado = df_final["Valor_Tarifa_Tabela"] * df_final["Peso Taxado"]
        
        # CORREÇÃO APLICADA AQUI
        # Trata o Frete_Minimo nulo como 0 para a comparação, evitando que o resultado vire nulo
        frete_min_com_fallback = df_final["Frete_Minimo"].fillna(0)
        df_final["Valor_Frete_Tabela"] = np.maximum(frete_calculado, frete_min_com_fallback)
        # FIM DA CORREÇÃO
        
        df_final["Diferenca_Frete"] = df_final["Valor_Frete"] - df_final["Valor_Frete_Tabela"]
        df_final["Diferenca_Tarifa"] = df_final["Valor_Tarifa"] - df_final["Valor_Tarifa_Tabela"]
        df_final["Diferenca_Peso"] = df_final["Peso Taxado"] - df_final.get("PesoUsado_CIA", 0)
        
        df_final["Dif_%"] = np.where(df_final["Valor_Frete_Tabela"].notna() & (df_final["Valor_Frete_Tabela"] != 0), (df_final["Valor_Frete"] / df_final["Valor_Frete_Tabela"] - 1.0) * 100.0, np.nan)

        # LÓGICA DE STATUS: Agora considera o fallback do Veloz E o Peso Excedente
        has_tarifa = df_final["Valor_Tarifa_Tabela"].notna()
        is_explicit_dev = df_final["__EH_DEV__"].eq(True)
        is_frete_minimo = (df_final["Valor_Frete"] <= df_final["Frete_Minimo"]) & has_tarifa
        
        # <-- MUDANÇA: Adiciona verificação de status do Veloz -->
        is_peso_excedente = False
        if '__Status_Veloz' in df_final.columns:
            # Garante que a coluna seja preenchida com string vazia
            # para evitar erros no .eq() em linhas que não são 'VELOZ' (que terão NaN)
            is_peso_excedente = df_final['__Status_Veloz'].fillna('').eq('PESO EXCEDENTE')
        
        df_final["Status"] = np.select(
            [
                is_frete_minimo, 
                is_explicit_dev, 
                is_peso_excedente, # <-- ADICIONADO (Prioridade alta)
                ~has_tarifa
            ],
            [
                "FRETE MINIMO", 
                "DEVOLUCAO", 
                "PESO EXCEDENTE", # <-- ADICIONADO
                "TARIFA NAO LOCALIZADA"
            ],
            default="COBRADO - TARIFADO"
        )
        # <-- FIM DA MUDANÇA -->
        
        cols_to_nullify = ["Diferenca_Frete", "Diferenca_Tarifa", "Dif_%", "Valor_Frete_Tabela"]
        # Se for frete mínimo OU peso excedente, anula as colunas de diferença
        is_frete_minimo_ou_excedente = is_frete_minimo | is_peso_excedente
        df_final.loc[is_frete_minimo_ou_excedente, cols_to_nullify] = np.nan

        # <-- MUDANÇA: Adiciona observação de Peso Excedente -->
        df_final["Observacao"] = np.where(
            is_frete_minimo, "Valor cobrado é igual ou inferior ao frete mínimo.",
            np.where(
                is_peso_excedente, "Peso taxado excede o limite de 30kg para o serviço Veloz.", # <-- ADICIONADO
                np.where(~has_tarifa, "Sem tarifa para (Origem, Destino, Tipo de Serviço).", df_final.get("Observacao", "")) # Mantém obs se houver
            )
        )
        # FIM DA MUDANÇA

        # <-- ALTERAÇÃO: Remove as colunas auxiliares, incluindo a nova -->
        df_final.drop(columns=['__EH_DEV__', 'Faixa_Peso_Usada', '__Status_Veloz'], inplace=True, errors="ignore")
        
        return df_final.reset_index(drop=True)