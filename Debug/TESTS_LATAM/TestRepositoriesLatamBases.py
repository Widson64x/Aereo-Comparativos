# C:\Programs\Aéreo-Comparativos\Debug\TESTS_LATAM\TestRepositoriesLatamBases.py

import os
import sys
import glob
import pandas as pd
from datetime import datetime

# --- Config Pandas ---
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 220)
pd.set_option('display.precision', 2)

# --- Raiz do projeto: sobe duas pastas (Debug/TESTS_LATAM -> Debug -> RAIZ) ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Import agora funciona
from Repositories.Repositorio_TabelasFretesLatam import ProcessarTabelaLatam

# --- Paths base ---
LATAM_DIR = os.path.join(PROJECT_ROOT, "Debug", "Archives", "LATAM", "ACORDO")
OUTPUT_DIR = os.path.join(LATAM_DIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

def escolher_arquivo_latam() -> str:
    """
    Prioriza arquivos cujo nome contenha 'ACORDO'. Se não houver, pega o XLS/XLSX mais recente.
    """
    candidatos = sorted(
        glob.glob(os.path.join(LATAM_DIR, "*ACORDO*.xls*")),
        key=os.path.getmtime,
        reverse=True,
    )
    if candidatos:
        return candidatos[0]

    todos = sorted(
        glob.glob(os.path.join(LATAM_DIR, "*.xls*")),
        key=os.path.getmtime,
        reverse=True,
    )
    if not todos:
        raise FileNotFoundError(f"Nenhum arquivo .xls/.xlsx em {LATAM_DIR}")
    return todos[0]

def exportar_excel(df: pd.DataFrame, caminho: str, aba: str = "Servicos_Base") -> None:
    with pd.ExcelWriter(caminho, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name=aba, index=False)

        ws = writer.sheets[aba]
        # Autoajuste de largura de coluna
        for i, col in enumerate(df.columns):
            try:
                col_len = max(df[col].astype(str).map(len).max(), len(col))
            except Exception:
                col_len = len(col)
            ws.set_column(i, i, min(col_len + 2, 60))

        # Congela cabeçalho
        ws.freeze_panes(1, 0)

def main():
    try:
        arquivo = escolher_arquivo_latam()
        print(f"\nIniciando processamento de 'Serviços Base' em: {arquivo}")

        proc = ProcessarTabelaLatam(arquivo)
        df_bases = proc.processar_servicos_bases()

        print("\n--- ANÁLISE DO DATAFRAME: SERVIÇOS BASE ---")
        print(f"Linhas: {len(df_bases)}")
        if not df_bases.empty:
            print("\nAmostra (top 10):")
            cols_show = [c for c in [
                "Tipo_Servico_Sigla", "Tipo_Servico",
                "Origem", "Destino",
                "Frete_Minimo", "Valor_Tarifa",
                "Data_Efetivacao_Tarifa"
            ] if c in df_bases.columns]
            print(df_bases[cols_show].head(10))
        else:
            print("DataFrame vazio.")

        # Ordena colunas para o Excel se existirem
        ordem = [
            "Tipo_Servico_Sigla", "Tipo_Servico",
            "Origem", "Destino",
            "Frete_Minimo", "Valor_Tarifa",
            "Data_Efetivacao_Tarifa"
        ]
        cols_export = [c for c in ordem if c in df_bases.columns] + \
                      [c for c in df_bases.columns if c not in ordem]

        df_export = df_bases[cols_export].copy()

        # Exporta
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_xlsx = os.path.join(OUTPUT_DIR, f"debug_servicos_base_{ts}.xlsx")
        exportar_excel(df_export, out_xlsx, aba="Servicos_Base")

        print(f"\n✅ Arquivo gerado: {out_xlsx}\n")

    except (FileNotFoundError, ValueError, IOError) as e:
        print("\nERRO no processamento de 'Serviços Base'.")
        print(f"Detalhes: {e}")

if __name__ == "__main__":
    main()
