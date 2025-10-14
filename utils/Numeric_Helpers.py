# C:\Programs\Aéreo-Comparativos\Utils\Numeric_Helpers

import pandas as pd
import numpy as np

def smart_to_numeric(val):
    """
    Converte um valor para numérico (float) de forma inteligente.

    Trata diferentes formatos de string para números, como:
    - Strings vazias ou NaN se tornam np.nan.
    - Troca vírgula por ponto como separador decimal, tratando formatos brasileiros/europeus.
    """
    # Retorna NaN se for um valor nulo (NaN do pandas)
    if pd.isna(val):
        return np.nan
    
    # Se já for um tipo numérico, converte diretamente para float
    if isinstance(val, (int, float, np.number)):
        return float(val)
    
    # Prepara a string
    s = str(val).strip()
    if s == "":
        return np.nan
    
    # Lógica de substituição de ponto/vírgula
    # Ex: '1.000,50' (ponto como separador de milhar, vírgula como decimal)
    if "," in s and "." in s and s.rfind(",") > s.rfind("."):
        s = s.replace(".", "").replace(",", ".")
    # Ex: '1000,50' (só vírgula como decimal)
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
        
    try:
        # Tenta converter para float
        return float(s)
    except (ValueError, TypeError):
        # Em caso de falha, usa pd.to_numeric com 'coerce' para transformar em NaN
        return pd.to_numeric(s, errors="coerce")

def to_numeric_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """
    Aplica a conversão numérica inteligente (smart_to_numeric) a uma lista de colunas de um DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame a ser processado.
        cols (list[str]): Lista de nomes das colunas para conversão.
        
    Returns:
        pd.DataFrame: DataFrame com as colunas especificadas convertidas para numérico.
    """
    df_out = df.copy()
    for c in cols:
        if c in df_out.columns:
            # Aplica a função de conversão inteligente a cada valor da coluna
            df_out[c] = df_out[c].apply(smart_to_numeric)
    return df_out