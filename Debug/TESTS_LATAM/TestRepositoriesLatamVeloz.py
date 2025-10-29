# C:\Programs\Aéreo-Comparativos\Debug\TESTS_LATAM\TestRepositoriesLatamVeloz.py

import os
import sys
import glob
import re
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

# Import do processador
from Repositories.Repositorio_TabelasFretesLatam import ProcessarTabelaLatam  # noqa: E402

# --- Paths base (mesmo formato do *Bases*) ---
LATAM_DIR = os.path.join(PROJECT_ROOT, "Debug", "Archives", "LATAM", "ACORDO")
OUTPUT_DIR = LATAM_DIR
os.makedirs(OUTPUT_DIR, exist_ok=True)

def escolher_arquivo_latam() -> str:
    """
    Prioriza arquivos com 'ACORDO' no nome dentro de LATAM/ACORDO.
    Se não houver, pega o .xls/.xlsx mais recente.
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

def exportar_excel(df: pd.DataFrame, caminho: str, aba: str = "Servico_Veloz_Completo") -> None:
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
        caminho_do_arquivo = escolher_arquivo_latam()
        print(f"\nIniciando o processamento do arquivo: {caminho_do_arquivo}\n")

        # 1) Instancia o processador
        processador = ProcessarTabelaLatam(caminho_do_arquivo)

        # 2) (Opcional) Processa Serviços Base só para validar leitura
        print("--- 1. Processando 'Serviços Base' (JUN E RES)... ---")
        _ = processador.processar_servicos_bases()

        # 3) Processa o Serviço Veloz
        print("\n--- 2. Processando 'Serviço Veloz' (PROXIMOVOO)... ---")
        df_servico_veloz = processador.processar_servico_veloz()

        print("\n--- ANÁLISE DO DATAFRAME: SERVIÇO VELOZ ---")
        if df_servico_veloz.empty:
            print("DataFrame vazio. Nenhum arquivo será gerado.")
            return

        print(f"Total de {len(df_servico_veloz)} linhas carregadas.")
        print("Amostra das 5 primeiras linhas:")
        print(df_servico_veloz.head())

        # --- Ordenação das colunas de faixa de peso ---
        id_cols_pref = [
            'Tipo_Servico_Sigla', 'Tipo_Servico',
            'Origem', 'Destino', 'Data_Efetivacao_Tarifa', 'Frete_Minimo'
        ]
        id_cols = [c for c in id_cols_pref if c in df_servico_veloz.columns]

        # Colunas de tarifa no formato 0+, 0p5+, 1+, 1p5+, 10+, etc.
        tariff_cols = [col for col in df_servico_veloz.columns if re.match(r'^\d+(p\d+)?\+$', str(col))]
        tariff_cols_sorted = sorted(
            tariff_cols,
            key=lambda x: float(str(x).replace('+', '').replace('p', '.'))
        )

        all_cols_ordered = id_cols + tariff_cols_sorted
        # Garante que só usa colunas existentes
        all_cols_ordered = [c for c in all_cols_ordered if c in df_servico_veloz.columns]

        df_to_export = df_servico_veloz[all_cols_ordered].copy()

        # --- Geração do arquivo Excel de debug ---
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f'debug_servico_veloz_{ts}.xlsx'
        output_path = os.path.join(OUTPUT_DIR, output_filename)

        print(f"\n--- Gerando arquivo Excel com TODAS as faixas de peso ordenadas ---")
        exportar_excel(df_to_export, output_path, aba="Servico_Veloz_Completo")
        print(f"✅ Arquivo salvo com sucesso em: {output_path}\n")

    except (FileNotFoundError, ValueError, IOError) as e:
        print(f"\nERRO: Ocorreu um problema durante o processamento.")
        print(f"Detalhes: {e}")

if __name__ == "__main__":
    main()
