# C:\Programs\Aéreo-Comparativos\Utils\DataFrame_Helpers.py

import pandas as pd
import numpy as np
import unicodedata
import re

def sanitize_header(name: str) -> str:
    """
    Limpa e padroniza os nomes das colunas para exportação, seguindo o padrão Title_Case_With_Underscores.

    Exemplo: 'Vlr Frete %' -> 'Vlr_Frete_Pct'
    
    Args:
        name (str): Nome da coluna.
        
    Returns:
        str: Nome da coluna padronizado.
    """
    # Remove acentos
    s = unicodedata.normalize('NFKD', str(name)).encode('ascii', 'ignore').decode('ascii')
    s = s.replace('%', 'pct') # Substitui '%' por 'pct'
    s = s.lower() # Converte para minúsculas
    # Substitui caracteres que não são letras/números por underscores
    s = re.sub(r'[^a-z0-9]+', '_', s)
    s = s.strip('_') # Remove underscores extras no início/fim

    # Capitaliza a primeira letra de cada palavra separada por '_' (Title_Case)
    if s:
        s = '_'.join(word.capitalize() for word in s.split('_'))
    
    return s

def sanitize_and_dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Garante que não haja colunas duplicadas no DataFrame, mantendo a primeira ocorrência.
    
    Args:
        df (pd.DataFrame): DataFrame a ser limpo.
        
    Returns:
        pd.DataFrame: DataFrame com colunas limpas e deduplicadas.
    """
    if df is None or df.empty: 
        return df
    
    df2 = df.copy()
    # Limpa espaços em branco nos nomes das colunas
    df2.columns = pd.Index([str(c).strip() for c in df2.columns])
    # Cria uma máscara para remover colunas duplicadas, mantendo a primeira
    mask = ~df2.columns.duplicated(keep="first")
    return df2.loc[:, mask]

def fill_numeric_nans_with_zero(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preenche valores nulos (NaN) com zero apenas em colunas que são de tipo numérico.
    
    Args:
        df (pd.DataFrame): DataFrame a ser processado.
        
    Returns:
        pd.DataFrame: DataFrame com NaNs numéricos preenchidos com 0.
    """
    df2 = df.copy()
    for c in df2.columns:
        # Verifica se a coluna tem um tipo de dado numérico
        if pd.api.types.is_numeric_dtype(df2[c]):
            df2[c] = df2[c].fillna(0)
    return df2