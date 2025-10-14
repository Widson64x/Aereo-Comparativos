import pandas as pd
import sys
import os
from pathlib import Path

# Adiciona o diretório raiz ao path para garantir que os imports funcionem
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from Repositories.Repositorio_TabelasFretes import TabelaFretesRepository

class SimulationService:
    """
    Serviço responsável por carregar os dados de fretes e executar simulações de custo.
    """
    def __init__(self, agreement_filepath: str | Path):
        """
        Inicializa o serviço de simulação.

        Args:
            agreement_filepath (str | Path): O caminho completo para o arquivo Excel
                                             contendo as tabelas de fretes.
        """
        # 1. Cria uma instância do repositório, passando o caminho do arquivo
        self.fretes_repo = TabelaFretesRepository(xlsx_path=agreement_filepath)
        
        # 2. Define um DataFrame vazio para armazenar os dados de frete
        self.dados_de_frete: pd.DataFrame = pd.DataFrame()
        
        # 3. Carrega e prepara os dados de frete assim que o serviço é criado
        #    Isso garante que os dados estarão prontos para a simulação.
        self.preparar_dados_simulacao()

    def preparar_dados_simulacao(self):
        """
        Usa o repositório para carregar os dados de frete e os armazena na instância do serviço.
        """
        # Carrega todos os fretes normalizados a partir do arquivo Excel
        self.dados_de_frete = self.fretes_repo.carregar_fretes_normalizados()
        print("Dados de frete carregados e prontos para a simulação.")
        
        # Garante que as colunas essenciais existem e estão com os tipos corretos
        required_cols = {'Origem', 'Destino', 'Valor_Frete'}
        if not required_cols.issubset(self.dados_de_frete.columns):
            raise ValueError(f"O DataFrame de fretes não contém as colunas necessárias: {required_cols}")
            
        self.dados_de_frete['Valor_Frete'] = pd.to_numeric(self.dados_de_frete['Valor_Frete'], errors='coerce')

    def _find_best_options_for_shipment(self, origem: str, destino: str, peso: float):
        """
        Encontra todas as tarifas aplicáveis para uma única remessa (origem, destino, peso)
        e calcula os custos.
        """
        # Filtra o DataFrame de acordos para o par Origem/Destino
        # CORRIGIDO: Usando 'self.dados_de_frete' e 'Valor_Frete'
        matches = self.dados_de_frete[
            (self.dados_de_frete['Origem'] == origem) &
            (self.dados_de_frete['Destino'] == destino)
        ].copy()

        if matches.empty:
            return {
                "options": [],
                "best_option": {"error": f"Nenhuma tarifa encontrada para a rota {origem}-{destino}."}
            }

        # Calcula o custo simulado para cada opção
        # CORRIGIDO: Multiplicando o peso pelo 'Valor_Frete'
        matches['Custo_Simulado'] = matches['Valor_Frete'] * peso
        
        # Ordena para encontrar a melhor opção (mais barata)
        matches = matches.sort_values(by='Custo_Simulado', ascending=True)

        # Formata a saída
        options = matches.to_dict('records')
        best_option = options[0] if options else None

        return {
            "options": options,
            "best_option": best_option
        }

    def run_simulation(self, shipments: list[dict]):
        """
        Executa a simulação para uma lista de remessas.

        Args:
            shipments: Uma lista de dicionários, cada um com {'Origem', 'Destino', 'Peso_Taxado'}.
        
        Returns:
            Uma lista com os resultados para cada remessa.
        """
        if self.dados_de_frete.empty:
            raise RuntimeError("Os dados de frete não foram carregados. A simulação não pode ser executada.")

        results = []
        for shipment in shipments:
            origem = shipment.get('Origem')
            destino = shipment.get('Destino')
            peso = float(shipment.get('Peso_Taxado', 0))

            if not all([origem, destino, peso > 0]):
                analysis = {"error": "Dados da remessa incompletos ou inválidos."}
            else:
                # Para cada remessa, encontra as melhores opções
                analysis = self._find_best_options_for_shipment(origem, destino, peso)
            
            results.append({
                "shipment_info": shipment,
                **analysis
            })
            
        return results