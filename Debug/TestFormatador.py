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
from Services.Latam.FormatadorTabelaLatam import FormatadorTabelaLatam
from Config import Appconfig

# --- CONFIGURAÇÃO DE EXIBIÇÃO DO PANDAS PARA O TERMINAL ---
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 200)
pd.set_option('display.precision', 2) # Isso vai formatar 26.59

# Caminho para o arquivo Excel de entrada
caminho_do_arquivo = os.path.join(project_root, "Debug/Archives/Cópia de ACORDO LUFT CNPJ 52134798001563.xlsx")

print(f"Iniciando o processamento do arquivo: {caminho_do_arquivo}\n")

try:
    # 1. Instancia o formatador
    # --- ALTERAÇÃO AQUI ---
    # Passamos o 'project_root' para o Appconfig
    formatador = FormatadorTabelaLatam(Appconfig(project_root))
    # --- FIM DA ALTERAÇÃO ---
    
    df_formatado = formatador.formatar_tabela(caminho_do_arquivo)
    
    print(f"Total de {len(df_formatado)} linhas formatadas.")
    print("Amostra das 5 primeiras linhas:")
    
    # --- ALTERAÇÃO AQUI ---
    # Usamos to_string(index=False) para remover o índice da impressão
    print(df_formatado.head().to_string(index=False))
    # --- FIM DA ALTERAÇÃO ---

    # --- NOVO TRECHO PARA GERAR O EXCEL ---
    
    # 1. Define o caminho e o nome do arquivo de saída
    #    (Vamos salvá-lo dentro da pasta 'Debug')
    caminho_saida = os.path.join(project_root, "Debug", "Archives", "Saida_TabelaLatamFormatada.xlsx")
    
    # 2. Salva o DataFrame completo no arquivo Excel
    #    index=False evita que o índice do pandas (0, 1, 2...) seja salvo
    df_formatado.to_excel(caminho_saida, index=False)
    
    # 3. Imprime uma mensagem de confirmação
    print(f"\nArquivo Excel gerado com sucesso em:")
    print(caminho_saida)
    
    # --- FIM DO NOVO TRECHO ---

except Exception as e:
    print(f"Erro durante o processamento: {e}")