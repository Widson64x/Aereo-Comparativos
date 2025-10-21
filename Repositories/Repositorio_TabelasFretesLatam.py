# Repositories/ProcessadorTabelaLatam.py

from __future__ import annotations
import pandas as pd
from pathlib import Path
from typing import List, Tuple, Dict

# Supondo que suas funções de utilidade estejam em Utils/Parse.py
# Se não estiverem, você pode precisar ajustar o import.
from Utils.Parse import std_text

class ProcessarTabelaLatam:
    """
    Classe responsável por processar arquivos de tabela de frete da Latam,
    extraindo e tratando dados de diferentes abas e serviços.
    """

    def __init__(self, xlsx_path: str | Path):
        """
        Inicializa o processador, carregando o arquivo Excel.

        Args:
            xlsx_path (str | Path): O caminho para o arquivo Excel da Latam.
        
        Raises:
            FileNotFoundError: Se o arquivo não for encontrado no caminho especificado.
            IOError: Se ocorrer um erro ao tentar ler o arquivo Excel.
        """
        self.xlsx_path = Path(xlsx_path)
        if not self.xlsx_path.exists():
            raise FileNotFoundError(f"O arquivo não foi encontrado em: {self.xlsx_path}")

        try:
            self.xls = pd.ExcelFile(self.xlsx_path)
        except Exception as e:
            raise IOError(f"Falha ao abrir o arquivo Excel: {self.xlsx_path}. Verifique se o arquivo não está corrompido. Erro: {e}")

    def processar_servicos_bases(self) -> pd.DataFrame:
        """
        Processa a planilha de fretes a partir da aba 'JUN E RES', lidando
        corretamente com cabeçalhos de múltiplas linhas e selecionando a tarifa
        apropriada com base na data.

        Returns:
            pd.DataFrame: Um DataFrame tratado com os dados dos serviços base.
            
        Raises:
            ValueError: Se a aba 'JUN E RES' ou colunas essenciais não forem encontradas.
        """
        SHEET_NAME = "JUN E RES"
        
        if SHEET_NAME not in self.xls.sheet_names:
            raise ValueError(f"A aba '{SHEET_NAME}' não foi encontrada no arquivo: {self.xlsx_path}.")

        try:
            # Lê o cabeçalho de duas linhas
            header_df = pd.read_excel(self.xls, sheet_name=SHEET_NAME, nrows=2, header=None)
            
            header_row1 = header_df.iloc[0]
            header_row2 = header_df.iloc[1]
            final_header = header_row2.fillna(header_row1)

            # Lê os dados, pulando o cabeçalho que já processamos
            df_raw = pd.read_excel(self.xls, sheet_name=SHEET_NAME, header=None, skiprows=2)
            df_raw.columns = final_header

        except Exception as e:
            raise IOError(f"Falha ao ler os dados da aba '{SHEET_NAME}'. Verifique o formato. Erro: {e}")

        # --- MAPEAMENTO DE COLUNAS ATUALIZADO ---
        column_mapping = {
            'Código do Produto': 'Tipo_Servico_Sigla',
            'Nome do Produto': 'Tipo_Servico',
            'Origem': 'Origem',
            'Destino': 'Destino',
            'Min Charge': 'Frete_Minimo',
            '0+': 'Valor_Tarifa',
            'Effective Date': 'Data_Efetivacao_Tarifa'
        }

        existing_columns = {k: v for k, v in column_mapping.items() if k in df_raw.columns}
        
        if 'Effective Date' not in df_raw.columns:
            raise ValueError(f"A coluna 'Effective Date' é obrigatória e não foi encontrada na aba '{SHEET_NAME}'.")

        DF_ACORDO = df_raw[list(existing_columns.keys())].copy()
        DF_ACORDO.rename(columns=existing_columns, inplace=True)

        # --- LIMPEZA E CONVERSÃO DE TIPOS ---
        DF_ACORDO['Frete_Minimo'] = pd.to_numeric(DF_ACORDO.get('Frete_Minimo'), errors='coerce')
        DF_ACORDO['Valor_Tarifa'] = pd.to_numeric(DF_ACORDO.get('Valor_Tarifa'), errors='coerce')
        DF_ACORDO['Origem'] = DF_ACORDO.get('Origem', pd.Series(dtype=str)).astype(str).apply(std_text)
        DF_ACORDO['Destino'] = DF_ACORDO.get('Destino', pd.Series(dtype=str)).astype(str).apply(std_text)
        
        DF_ACORDO['Data_Efetivacao_Tarifa'] = pd.to_datetime(DF_ACORDO['Data_Efetivacao_Tarifa'], errors='coerce')

        DF_ACORDO.dropna(
            subset=['Origem', 'Destino', 'Tipo_Servico', 'Valor_Tarifa', 'Data_Efetivacao_Tarifa'], 
            inplace=True
        )
        
        # --- LÓGICA DE SELEÇÃO DE TARIFA ---
        # Ordena o DataFrame pela data de efetivação para garantir que as tarifas mais recentes apareçam primeiro.
        # A manutenção do histórico é importante para análises de faturas passadas.
        DF_ACORDO.sort_values(by='Data_Efetivacao_Tarifa', ascending=False, inplace=True)
        
        print("--- Debug: Verificando os dados extraídos de 'Serviços Bases' ---")
        print("\n### Primeiras 5 linhas (com histórico mantido):")
        print(DF_ACORDO.head())
        print("\n### Informações do DataFrame:")
        DF_ACORDO.info()
        print("------------------------------------------------------\n")

        return DF_ACORDO.reset_index(drop=True)

    def processar_servico_veloz(self) -> pd.DataFrame:
            """
            Processa a planilha de fretes a partir da aba 'PROXIMOVOO'.
            Esta versão carrega os dados mantendo a estrutura original das colunas
            de faixas de peso, conforme a planilha.

            Returns:
                pd.DataFrame: Um DataFrame tratado com os dados do serviço Veloz,
                              preservando as múltiplas colunas de tarifa.
                
            Raises:
                ValueError: Se a aba 'PROXIMOVOO' ou colunas essenciais não forem encontradas.
            """
            SHEET_NAME = "PROXIMOVOO"
            
            if SHEET_NAME not in self.xls.sheet_names:
                print(f"Aviso: A aba '{SHEET_NAME}' não foi encontrada no arquivo. Pulando o processamento do Serviço Veloz.")
                return pd.DataFrame()

            try:
                # A leitura do cabeçalho duplo permanece a mesma
                header_df = pd.read_excel(self.xls, sheet_name=SHEET_NAME, nrows=2, header=None)
                header_row1 = header_df.iloc[0]
                header_row2 = header_df.iloc[1]
                final_header = header_row2.fillna(header_row1)

                df_raw = pd.read_excel(self.xls, sheet_name=SHEET_NAME, header=None, skiprows=2)
                df_raw.columns = final_header
            except Exception as e:
                raise IOError(f"Falha ao ler os dados da aba '{SHEET_NAME}'. Verifique o formato. Erro: {e}")

            # --- MAPEAMENTO E SELEÇÃO DE COLUNAS ---
            column_mapping = {
                'Código do Produto': 'Tipo_Servico_Sigla',
                'Nome do Produto': 'Tipo_Servico',
                'Origem': 'Origem',
                'Destino': 'Destino',
                # <-- NOVO: Adicionado Min Charge -->
                'Min Charge': 'Frete_Minimo',
                'Effective Date': 'Data_Efetivacao_Tarifa'
            }

            # Identifica as colunas de faixa de peso (ex: '0+', '0p5+', '10+')
            tariff_cols = [col for col in df_raw.columns if isinstance(col, str) and pd.Series(col).str.match(r'^\d+(p\d+)?\+$').any()]
            
            # Pega todas as colunas que vamos usar: as mapeadas e as de tarifa
            id_cols = [col for col in column_mapping.keys() if col in df_raw.columns]
            all_cols_to_keep = id_cols + tariff_cols
            
            # Filtra o DataFrame para conter apenas as colunas desejadas
            df_filtered = df_raw[[col for col in all_cols_to_keep if col in df_raw.columns]].copy()
            
            # Renomeia as colunas de identificação
            df_filtered.rename(columns=column_mapping, inplace=True)

            # --- LIMPEZA E CONVERSÃO DE TIPOS ---
            # Converte as colunas de identificação
            df_filtered['Origem'] = df_filtered.get('Origem', pd.Series(dtype=str)).astype(str)
            df_filtered['Destino'] = df_filtered.get('Destino', pd.Series(dtype=str)).astype(str)
            df_filtered['Data_Efetivacao_Tarifa'] = pd.to_datetime(df_filtered.get('Data_Efetivacao_Tarifa'), errors='coerce')
            
            # <-- NOVO: Converte Frete_Minimo -->
            df_filtered['Frete_Minimo'] = pd.to_numeric(df_filtered.get('Frete_Minimo'), errors='coerce')

            # Converte TODAS as colunas de tarifa para numérico de uma só vez
            for col in tariff_cols:
                if col in df_filtered.columns:
                    df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce')

            # Remove linhas onde colunas essenciais são nulas
            essential_cols = ['Origem', 'Destino', 'Tipo_Servico', 'Data_Efetivacao_Tarifa']
            df_filtered.dropna(subset=[col for col in essential_cols if col in df_filtered.columns], inplace=True)

            # --- FINALIZAÇÃO ---
            df_final = df_filtered.sort_values(by='Data_Efetivacao_Tarifa', ascending=False)
            
            print(f"--- Debug: Verificando os dados extraídos de '{SHEET_NAME}' (Estrutura Original) ---")
            print("\n### Primeiras 5 linhas (com faixas de peso como colunas):")
            # Mostra algumas colunas de ID e as primeiras 5 colunas de tarifa para visualização
            display_cols_keys = [v for v in column_mapping.values() if v in df_final.columns]
            display_cols_tariffs = [col for col in tariff_cols if col in df_final.columns][:5]
            print(df_final[display_cols_keys + display_cols_tariffs].head())
            print("\n### Informações do DataFrame:")
            df_final.info()
            print("------------------------------------------------------\n")

            return df_final.reset_index(drop=True)