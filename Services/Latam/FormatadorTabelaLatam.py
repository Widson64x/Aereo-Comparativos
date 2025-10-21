# C:\Programs\Aéreo-Comparativos\Services\Latam\FormatadorTabelaLatam.py

from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np

# Importa as configurações do aplicativo
from Config import Appconfig
# Importa as funções de helpers
from Utils.Numeric_Helpers import to_numeric_cols
from Utils.DataFrame_Helpers import sanitize_header, sanitize_and_dedupe_columns
from Utils.Parse import std_text

from Repositories.Repositorio_TabelasFretesLatam import ProcessarTabelaLatam


class FormatadorTabelaLatam:
    """
    Classe responsável por formatar e preparar os dados das tabelas de frete da Latam
    para comparações e análises.
    """
    """
      Tabela TipoServico Origem  Destino FreteMinimo Tarifa
      1231   GOLLOG SAÚDE     SDU AJU 60  26,59
    """

    def __init__(self, app_config: Appconfig):
        self.app_config = app_config
        # Corrigido na última etapa (de .PATHS para .paths)
        self.paths = app_config.paths 

    def formatar_tabela(self, xlsx_path: str | Path) -> pd.DataFrame:
        """
        Formata a tabela de fretes da Latam a partir do arquivo Excel fornecido.

        Args:
            xlsx_path (str | Path): O caminho para o arquivo Excel da Latam.

        Returns:
            pd.DataFrame: Um DataFrame formatado com os dados relevantes.
        """
        processador = ProcessarTabelaLatam(xlsx_path)
        df_servicos_base = processador.processar_servicos_bases()

        # --- ALTERAÇÃO AQUI ---
        # Ajustamos as CHAVES (lado esquerdo) do dicionário para bater
        # com os nomes das colunas que vêm do 'df_servicos_base' (visto no debug).
        colunas_relevantes = {
            # "Tabela" removida por enquanto, pois não existe no df_servicos_base
            "Tipo_Servico": "TipoServico",     # Antes era "Tipo de Serviço"
            "Origem": "Origem",               # Já estava correto
            "Destino": "Destino",             # Já estava correto
            "Frete_Minimo": "FreteMinimo",    # Antes era "Frete Mínimo"
            "Valor_Tarifa": "Tarifa"          # Antes era "Tarifa"
        }
        # --- FIM DA ALTERAÇÃO ---

        # Esta linha agora vai funcionar
        df_formatado = df_servicos_base[list(colunas_relevantes.keys())].rename(columns=colunas_relevantes)

        # Converte colunas numéricas
        # Os nomes aqui ("FreteMinimo", "Tarifa") estão corretos, 
        # pois são os nomes DEPOIS do rename.
        df_formatado = to_numeric_cols(df_formatado, ["FreteMinimo", "Tarifa"])

        return df_formatado