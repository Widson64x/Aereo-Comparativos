# C:\Programs\Aéreo-Comparativos\Services\Latam\Latam_Metrics.py

from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Dict, Union

class LatamMetricsCalculator:
    """
    Calcula as métricas financeiras da comparação de fretes para os KPIs,
    seguindo a nova lógica de status e cálculo.
    """

    # Status de comparação atualizados
    STATUS_SIMULADO = "COBRADO - TARIFADO"  # Anteriormente "COBRADO - TARIFADO"
    STATUS_DEVOLUCAO = "DEVOLUCAO"
    STATUS_NAO_LOCALIZADA = "TARIFA NAO LOCALIZADA"
    STATUS_FRETE_MINIMO = "FRETE MINIMO" # Mantido para consistência, embora não seja mais um status ativo

    def __init__(self, df_final: pd.DataFrame):
        """
        Inicializa com o DataFrame final (df_export), que deve conter números puros.
        """
        self.df = df_final.rename(columns={
            'Valor_Frete': 'ValorFrete',
            'Peso_Taxado': 'PesoTaxado',
            'Valor_Frete_Tabela': 'ValorFreteTabela',
            'Valor_Tarifa_Tabela': 'ValorTarifaTabela' # Nova coluna necessária para o cálculo do KPI
        }, errors='ignore')
        
        numeric_cols = ['ValorFrete', 'PesoTaxado', 'ValorFreteTabela', 'ValorTarifaTabela']
        for col in numeric_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')

    def calculate_metrics(self) -> Dict[str, Union[float, int]]:
        """
        Executa todos os cálculos e retorna o dicionário de métricas.
        """
        df = self.df.copy()
        
        # --- 1. Máscaras de Filtragem (Lógica Simplificada) ---
        # "Tarifado" agora significa qualquer linha com o status 'COBRADO - TARIFADO'
        is_tarifado = df['Status'] == self.STATUS_SIMULADO
        is_devolucao = df['Status'] == self.STATUS_DEVOLUCAO
        is_sem_tarifa = df['Status'] == self.STATUS_NAO_LOCALIZADA
        is_frete_minimo = df['Status'] == self.STATUS_FRETE_MINIMO  # Mantido para consistência
        # --- 2. Agregações com as Novas Regras ---
        
        # KPI: Cobrado Geral (Soma de tudo que foi faturado no PDF)
        total_cobrado_geral = df['ValorFrete'].sum(skipna=True)

        # KPI: Cobrado — Tarifado (Soma do ValorFrete apenas das linhas que encontraram tarifa)
        total_cobrado_tarifado = df.loc[is_tarifado, 'ValorFrete'].sum(skipna=True)
        
        # --- NOVA REGRA DE CÁLCULO PARA O SIMULADO ---
        # KPI: COBRADO - TARIFADO (Calculado como ValorTarifa * PesoTaxado, ignorando Frete Mínimo)
        if 'ValorTarifaTabela' in df.columns and 'PesoTaxado' in df.columns:
            total_simulado = (df.loc[is_tarifado, 'ValorTarifaTabela'] * df.loc[is_tarifado, 'PesoTaxado']).sum()
        else:
            # Fallback para o ValorFreteTabela caso a coluna não esteja presente
            total_simulado = df.loc[is_tarifado, 'ValorFreteTabela'].sum(skipna=True)
        
        # Outros KPIs
        total_devolucao = df.loc[is_devolucao, 'ValorFrete'].sum(skipna=True)
        total_sem_tarifa = df.loc[is_sem_tarifa, 'ValorFrete'].sum(skipna=True)
        total_frete_minimo = df.loc[is_frete_minimo, 'ValorFrete'].sum(skipna=True)
        
        # --- 3. Cálculos Derivados ---
        total_diferenca = total_cobrado_tarifado - total_simulado
        pct_diff = (total_diferenca / total_simulado) * 100 if total_simulado != 0 else 0.0
        total_valores_a_verificar = total_cobrado_geral - total_cobrado_tarifado
        
        return {
            "total_cobrado_geral": round(float(total_cobrado_geral), 2),
            "total_cobrado_tarifado": round(float(total_cobrado_tarifado), 2),
            "total_simulado": round(float(total_simulado), 2),
            "total_diferenca": round(float(total_diferenca), 2),
            "pct_diff": round(float(pct_diff), 2) if pd.notna(pct_diff) else np.nan,
            "total_devolucao": round(float(total_devolucao), 2),
            "total_sem_tarifa": round(float(total_sem_tarifa), 2),
            "total_frete_minimo": round(float(total_frete_minimo), 2),
            "total_valores_a_verificar": round(float(total_valores_a_verificar), 2)
        }