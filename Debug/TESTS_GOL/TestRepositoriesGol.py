# C:\Programs\Aéreo-Comparativos\Debug\LATAM\TestRepositoriesLatamBases.py
# Executa o pipeline GOLLOG lendo vários arquivos e gerando um Excel único com abas
import os
import sys
from datetime import datetime
import pandas as pd

# Display amigável no terminal
pd.set_option('display.max_columns', 120)
pd.set_option('display.width', 220)
pd.set_option('display.precision', 2)

# raiz do projeto
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(project_root)

from Repositories.Repositorio_TabelasFretesGol import RepositorioTabelasFretesGol

# pasta de entrada (mantenha conforme você informou)
INPUT_DIR = os.path.join(project_root, 'Debug', 'Archives', 'GOLLOG_TABLES')
# saída
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
OUTPUT_XLSX = os.path.join(project_root, 'Debug', 'Archives', 'GOLLOG_TABLES', 'Outputs', f'GOLLOG_Tabelas_Consolidadas_{timestamp}.xlsx')

print(f"Raiz do projeto: {project_root}")
print(f"Lendo arquivos em: {INPUT_DIR}\n")

try:
    repo = RepositorioTabelasFretesGol(INPUT_DIR)
    sheets = repo.scan()

    if not sheets:
        print("[WARN] Nenhum DataFrame produzido. Verifique os arquivos de entrada.")
    else:
        print(f"\n[INFO] Total de abas a exportar: {len(sheets)}")
        out = repo.export_excel(OUTPUT_XLSX, sheets)
        print(f"Arquivo final: {out}")
except Exception as e:
    print(f"[ERRO] {e}")
