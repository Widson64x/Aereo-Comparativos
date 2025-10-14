from flask import Blueprint, render_template, request, flash, redirect, url_for
from Services.Latam.SimulationService import SimulationService
import os

simulador_bp = Blueprint('simulador', __name__)

# O caminho para a pasta onde os arquivos de acordo são salvos
ACORDOS_DIR = "uploads/acordos" 

@simulador_bp.route('/simulador', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        
        # ==================== PARTE ATUALIZADA ====================
        # 1. Coletar dados dinamicamente do formulário
        # Usamos .getlist() para pegar todos os campos com o mesmo nome (um para cada linha)
        origens = request.form.getlist('origem')
        destinos = request.form.getlist('destino')
        pesos = request.form.getlist('peso_taxado')

        shipments_data = []
        # O 'zip' junta as três listas para que possamos iterar por cada linha facilmente
        for origem, destino, peso_str in zip(origens, destinos, pesos):
            # Processamos apenas as linhas que foram preenchidas
            if origem and destino and peso_str:
                try:
                    # Limpa e formata os dados para garantir consistência
                    shipment = {
                        'Origem': origem.strip().upper(),
                        'Destino': destino.strip().upper(),
                        # Converte o peso para número, aceitando vírgula ou ponto
                        'Peso_Taxado': float(peso_str.replace(',', '.'))
                    }
                    shipments_data.append(shipment)
                except ValueError:
                    flash(f"Valor de peso inválido '{peso_str}' na linha {origem}-{destino}. A linha foi ignorada.", "warning")
                    continue
        
        # Se nenhuma linha válida foi preenchida, volta para o formulário com um erro
        if not shipments_data:
            flash("Nenhuma remessa válida foi inserida. Por favor, preencha pelo menos uma linha.", "danger")
            return redirect(url_for('simulador.index'))
        # ==================== FIM DA ATUALIZAÇÃO ====================


        # 2. Encontrar o arquivo de acordo mais recente (lógica mantida)
        try:
            files = sorted(
                [os.path.join(ACORDOS_DIR, f) for f in os.listdir(ACORDOS_DIR)],
                key=os.path.getmtime,
                reverse=True
            )
            latest_agreement = files[0] if files else None
        except FileNotFoundError:
            flash(f"O diretório de acordos '{ACORDOS_DIR}' não foi encontrado.", "danger")
            return redirect(url_for('simulador.index'))

        if not latest_agreement:
            flash("Nenhum arquivo de acordo foi encontrado na pasta de uploads.", "danger")
            return redirect(url_for('simulador.index'))

        # 3. Executar a simulação (lógica mantida)
        try:
            service = SimulationService(agreement_filepath=latest_agreement)
            simulation_results = service.run_simulation(shipments_data)
        except Exception as e:
            flash(f"Ocorreu um erro ao processar a simulação: {e}", "danger")
            return redirect(url_for('simulador.index'))

        # 4. Renderizar a página de resultados (lógica mantida)
        return render_template('Tools/Simulador_Resultados.html', results=simulation_results)

    # Para o método GET, apenas mostra a página do formulário
    return render_template('Tools/Simulador.html')