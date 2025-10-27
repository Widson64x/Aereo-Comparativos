# C:\Programs\Aéreo-Comparativos\Debug\test.py

import os
import sys
import pandas as pd
import re
from datetime import datetime

# Adiciona o diretório raiz do projeto ao sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

# Importa a classe
from Repositories.Repositorio_TabelasFretesLatam import ProcessarTabelaLatam

# --- CONFIGURAÇÃO DE EXIBIÇÃO DO PANDAS PARA O TERMINAL ---
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 200)
pd.set_option('display.precision', 2)

# Caminho para o arquivo Excel de entrada
caminho_do_arquivo = os.path.join(project_root, "Debug/Archives/LATAM/Cópia de ACORDO LUFT CNPJ 52134798001563.xlsx")

print(f"Iniciando o processamento do arquivo: {caminho_do_arquivo}\n")

try:
    # 1. Instancia o processador
    processador = ProcessarTabelaLatam(caminho_do_arquivo)

    # 2. Processa os "Serviços Base"
    print("--- 1. Processando 'Serviços Base' (JUN E RES)... ---")
    df_servicos_base = processador.processar_servicos_bases()

    # 3. Processa o "Serviço Veloz"
    print("\n--- 2. Processando 'Serviço Veloz' (PROXIMOVOO)... ---")
    df_servico_veloz = processador.processar_servico_veloz()

    # --- EXIBIÇÃO NO TERMINAL ---
    print("\n\n--- ANÁLISE DO DATAFRAME: SERVIÇO VELOZ ---")
    if not df_servico_veloz.empty:
        print(f"Total de {len(df_servico_veloz)} linhas carregadas.")
        print("Amostra das 5 primeiras linhas:")
        print(df_servico_veloz.head())

        # --- PREPARAÇÃO PARA O EXCEL COM TODAS AS COLUNAS ORDENADAS ---

        # Define as colunas de identificação
        id_cols = ['Tipo_Servico_Sigla', 'Tipo_Servico', 'Origem', 'Destino', 'Data_Efetivacao_Tarifa']
        
        # Pega todas as colunas de faixa de peso
        tariff_cols = [col for col in df_servico_veloz.columns if re.match(r'^\d+(p\d+)?\+$', col)]
        
        # **CHAVE DA CORREÇÃO**: Ordena as colunas de tarifa numericamente
        # Converte '1p5+' para 1.5 para poder ordenar corretamente
        tariff_cols_sorted = sorted(
            tariff_cols, 
            key=lambda x: float(x.replace('+', '').replace('p', '.'))
        )
        
        # Combina as colunas de ID com TODAS as colunas de tarifa, agora ordenadas
        all_cols_ordered = id_cols + tariff_cols_sorted
        
        # Cria o DataFrame final para exportação com a ordem correta
        df_to_export = df_servico_veloz[all_cols_ordered]

        # --- GERAÇÃO DO ARQUIVO EXCEL DE DEBUG ---
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f'debug_servico_veloz_COMPLETO_{timestamp}.xlsx'
        output_path = os.path.join(project_root, 'Debug', 'Archives', output_filename)

        print(f"\n--- Gerando arquivo Excel com TODAS as faixas de peso ordenadas ---")
        
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            df_to_export.to_excel(writer, sheet_name='Servico_Veloz_Completo', index=False)
            
            workbook  = writer.book
            worksheet = writer.sheets['Servico_Veloz_Completo']
            
            # Autoajusta a largura das colunas
            for i, col in enumerate(df_to_export.columns):
                column_len = max(df_to_export[col].astype(str).map(len).max(), len(col))
                worksheet.set_column(i, i, column_len + 2)
        
        print(f"✅ Arquivo salvo com sucesso em: {output_path}")

    else:
        print("O DataFrame do Serviço Veloz está vazio. Nenhum arquivo foi gerado.")

except (FileNotFoundError, ValueError, IOError) as e:
    print(f"\nERRO: Ocorreu um problema durante o processamento.")
    print(f"Detalhes: {e}")