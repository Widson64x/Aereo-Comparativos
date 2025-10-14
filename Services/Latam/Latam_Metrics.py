# C:\Programs\Aéreo-Comparativos\Services\Latam\LatamMetrics.py

from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Dict, Any, Union

class LatamMetricsCalculator:
    """
    Calcula e agrega as métricas financeiras de comparação de fretes LATAM
    para exibição nos cards (KPIs).

    Esta classe replica a lógica de agregação que estava em JavaScript, 
    mas utiliza o DataFrame final processado (df_export) no backend.
    """

    # Status de comparação usados na coluna 'Status'
    STATUS_DENTRO_TOL = "DENTRO DA TOLERANCIA"
    STATUS_FORA_TOL = "FORA DA TOLERANCIA"
    STATUS_DEVOLUCAO = "DEVOLUCAO"
    STATUS_NAO_LOCALIZADA = "TARIFA NAO LOCALIZADA"
    STATUS_FRETE_MINIMO = "FRETE MINIMO"

    def __init__(self, df_final: pd.DataFrame):
        """
        Inicializa o calculador com o DataFrame final de comparação (df_export).
        É crucial que o DataFrame aqui seja o 'df_export' (com números limpos).
        """
        # Mapeia colunas padronizadas do df_export (sanitized headers)
        self.df = df_final.rename(columns={
            'Vlr_Frete': 'VlrFrete',
            'Frete_Peso': 'FretePeso',
            'Peso_Taxado': 'PesoTaxado',
            'Frete_Tabela': 'FreteTabela',
            'Diferenca_Frete': 'DiferencaFrete',
            'Dif_Pct': 'DifPct' # A coluna 'Dif_%' do display vira 'Dif_Pct' no export
        }, errors='ignore')
        
        # Garante que as colunas numéricas necessárias são float
        self.numeric_cols = ['VlrFrete', 'FretePeso', 'PesoTaxado', 'FreteTabela', 'DiferencaFrete', 'DifPct']
        for col in self.numeric_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')


    def _calculate_cobrado_geral(self) -> float:
        """
        Calcula o Valor Total Cobrado (Geral). 
        É o somatório da coluna 'VlrFrete' (que contém o valor cobrado do PDF).
        """
        if 'VlrFrete' not in self.df.columns:
            return 0.0
            
        # O FreteTabela/Frete_Tabela não é usado aqui, apenas o Vlr Frete, que vem do PDF.
        total_cobrado_geral = self.df['VlrFrete'].sum(skipna=True)
        return total_cobrado_geral if pd.notna(total_cobrado_geral) else 0.0

    def calculate_metrics(self) -> Dict[str, Union[float, int]]:
        """
        Executa todos os cálculos e retorna o dicionário de métricas.
        """
        df = self.df.copy()
        
        # --- 1. Máscaras de Filtragem ---
        is_tarifado = df['Status'].isin([self.STATUS_DENTRO_TOL, self.STATUS_FORA_TOL])
        is_devolucao = df['Status'] == self.STATUS_DEVOLUCAO
        is_sem_tarifa = df['Status'] == self.STATUS_NAO_LOCALIZADA
        is_frete_minimo = df['Status'] == self.STATUS_FRETE_MINIMO
        
        # --- 2. Agregações ---
        
        # KPI: Cobrado Geral (Vlr Frete de todas as linhas)
        total_cobrado_geral = self._calculate_cobrado_geral()

        # KPI: Cobrado — Tarifado (Vlr Frete das linhas que encontraram tarifa)
        # Atenção: O código JS usava (Frete_Peso * Peso Taxado). 
        # Assumindo que o VlrFrete já é o total cobrado na linha, 
        # usaremos VlrFrete das linhas tarifadas.
        total_cobrado_tarifado = df.loc[is_tarifado, 'VlrFrete'].sum(skipna=True)
        total_cobrado_tarifado = total_cobrado_tarifado if pd.notna(total_cobrado_tarifado) else 0.0

        # KPI: Acordado — Simulado (Frete Tabela das linhas tarifadas)
        total_simulado = df.loc[is_tarifado, 'FreteTabela'].sum(skipna=True)
        total_simulado = total_simulado if pd.notna(total_simulado) else 0.0

        # KPI: Valores Verificados
        total_devolucao = df.loc[is_devolucao, 'VlrFrete'].sum(skipna=True)
        total_sem_tarifa = df.loc[is_sem_tarifa, 'VlrFrete'].sum(skipna=True)
        total_frete_minimo = df.loc[is_frete_minimo, 'VlrFrete'].sum(skipna=True)
        
        # --- 3. Cálculos Derivados ---
        
        # KPI: Diferença (Cobrado - Simulado)
        total_diferenca = total_cobrado_tarifado - total_simulado
        
        # KPI: % Diferença
        if total_simulado != 0:
            pct_diff = (total_diferenca / total_simulado) * 100
        else:
            pct_diff = 0.0 if total_diferenca == 0 else np.nan
            
        # KPI: Valores a Verificar (Geral - Tarifado)
        total_valores_a_verificar = total_cobrado_geral - total_cobrado_tarifado
        
        # Garante que os valores retornados sejam floats/ints padrão Python
        return {
            "total_cobrado_geral": round(total_cobrado_geral, 2),
            "total_cobrado_tarifado": round(total_cobrado_tarifado, 2),
            "total_simulado": round(total_simulado, 2),
            "total_diferenca": round(total_diferenca, 2),
            "pct_diff": round(pct_diff, 2) if pd.notna(pct_diff) else np.nan,
            "total_devolucao": round(total_devolucao, 2),
            "total_sem_tarifa": round(total_sem_tarifa, 2),
            "total_frete_minimo": round(total_frete_minimo, 2),
            "total_valores_a_verificar": round(total_valores_a_verificar, 2)
        }