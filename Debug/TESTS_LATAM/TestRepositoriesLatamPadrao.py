# C:\Programs\Aéreo-Comparativos\Debug\TESTS_LATAM\TestRepositoriesLatamPadrao.py

import os
import sys
import pandas as pd
from datetime import datetime

# raiz do projeto: ...\Aéreo-Comparativos
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, BASE_DIR)

# ajuste o import abaixo se o arquivo for ProcessadorTabelaLatam.py
from Repositories.Repositorio_TabelasFretesLatam import ProcessarTabelaLatam

pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 220)

PASTA_SAIDA = os.path.join(BASE_DIR, "Debug", "Archives", "LATAM", "PADRAO")
os.makedirs(PASTA_SAIDA, exist_ok=True)

print(f"Root: {BASE_DIR}")
print("Lendo tabelas padrão via ProcessarTabelaLatam.processar_tabelas_padrao()\n")

try:
    # agora sem passar caminho: lê Data/Tabelas/LATAM ou LATAM_TABLES_DIR
    df_padrao = ProcessarTabelaLatam.processar_tabelas_padrao()

    if df_padrao.empty:
        print("Sem dados consolidados. Verifique Data/Tabelas/LATAM ou a variável LATAM_TABLES_DIR.")
    else:
        print(f"Total de linhas: {len(df_padrao)}\n")
        print("Amostra (10 primeiras):")
        print(df_padrao.head(10).to_string(index=False))

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        arq_xlsx = os.path.join(PASTA_SAIDA, f"debug_tabelas_padrao_CONSOLIDADO_{ts}.xlsx")
        df_padrao.to_excel(arq_xlsx, index=False)

        print(f"\nArquivo gerado:\n- {arq_xlsx}")

except Exception as e:
    print(f"Erro no teste: {e}")
