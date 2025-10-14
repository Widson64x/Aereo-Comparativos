# Adicione esta função a um arquivo que tenha acesso ao 'engine' de conexão do DB
from Db import engine # Importar sua conexão de banco de dados
import pandas as pd

def get_first_ctc_motivodoc(noca: str) -> str | None:
    """
    Busca o 'motivodoc' do CTC associado ao 'codawb' mais antigo
    para o 'nOca' fornecido.
    
    Atenção: A 'filialctc' é usada como o CTC.
    A query SQL foi projetada para pegar o *primeiro* registro
    de CTC que é ligado ao AWB.
    """
    if not noca:
        return None

    # Consulta SQL para buscar o motivodoc do primeiro CTC
    # Usamos TOP 1 e ORDER BY para garantir que pegamos o primeiro CTC associado ao AWB
    sql_query = f"""
    SELECT TOP 1
        t2.motivodoc
    FROM 
        tb_airAWB t1
    JOIN 
        tb_airAWBnota t_nota ON t1.codawb = t_nota.codawb
    JOIN
        tb_ctc_esp t2 ON t_nota.filialctc = t2.filialctc
    WHERE 
        t1.nOca = '{noca}'
    ORDER BY 
        t2.data
    """
    
    try:
        # Usa o engine para executar a query e ler o resultado
        with engine.connect() as conn:
            result = pd.read_sql(sql_query, conn)
            
            if not result.empty:
                # Retorna o motivodoc (esperado que seja 'DEV', 'ENT', etc.)
                return str(result['motivodoc'].iloc[0]).strip().upper()
        return None
    
    except Exception as e:
        print(f"Erro ao buscar motivodoc para nOca {noca}: {e}")
        return None

# Modificação na função get_ctcs para aceitar uma lista de nOcas
def get_ctcs(noca_list: list[str]) -> pd.DataFrame:
    """
    Busca os CTCs e seus respectivos motivodocs para uma lista de nOcas (Documento).
    Retorna um DataFrame com colunas 'nOca' e 'ctc_e_motivo'.
    """
    if not noca_list:
        return pd.DataFrame()

    # Formata a lista de nOcas para uso na cláusula IN ('noca1', 'noca2', ...)
    noca_str = ", ".join(f"'{noca}'" for noca in noca_list)

    # Consulta SQL otimizada para buscar todos os CTCs de todos os nOcas de uma vez
    sql_query = f"""
    SELECT 
        t1.nOca,
        CONCAT(t2.motivodoc,' - ', t2.filialctc,  ' | (', t2.modal,') | ') AS ctc_e_motivo
    FROM 
        tb_airAWB t1
    JOIN 
        tb_airAWBnota t_nota ON t1.codawb = t_nota.codawb
    JOIN
        tb_ctc_esp t2 ON t_nota.filialctc = t2.filialctc
    WHERE 
        t1.nOca IN ({noca_str})
    """
    
    try:
        # Usa o engine para executar a query e ler o resultado
        with engine.connect() as conn:
            result = pd.read_sql(sql_query, conn)
            
            if not result.empty:
                # Otimização: Agrupa os resultados por nOca e concatena os CTCs
                ctc_map_df = result.groupby('nOca')['ctc_e_motivo'].agg(lambda x: ', '.join(x.unique())).reset_index()
                ctc_map_df.columns = ['Documento', 'CTCs'] # Renomeia para facilitar o merge
                print("CTCs encontrados:", ctc_map_df)
                return ctc_map_df
        print("Nenhum CTC encontrado para as nOcas fornecidas.")
        return pd.DataFrame()
    
    except Exception as e:
        print(f"Erro ao buscar CTCs em volume: {e}")
        return pd.DataFrame()

def get_ctc_peso(noca_list: list[str]) -> pd.DataFrame:
    """
    Busca os pesos (taxado, bruto) e determina o peso que será usado pela Cia. Aérea 
    (o maior entre eles) para uma lista de nOcas.

    Retorna um DataFrame com as colunas 'Documento', 'Peso_Taxado_CTC', 
    'Peso_Bruto_CTC', 'PesoUsado_CIA', e 'TipoPeso_CIA'.
    """
    if not noca_list:
        return pd.DataFrame()

    valid_noca_list = [n for n in set(noca_list) if n and isinstance(n, str)]
    if not valid_noca_list:
        return pd.DataFrame()
        
    noca_str = ", ".join(f"'{noca}'" for noca in valid_noca_list)

    sql_query = f"""
    SELECT
        t1.nOca AS Documento,
        SUM(c.pesotax) AS Peso_Taxado_CTC,
        SUM(c.peso) AS Peso_Bruto_CTC,
        CASE 
            WHEN SUM(c.pesotax) > SUM(c.peso) THEN SUM(c.pesotax)
            ELSE SUM(c.peso)
        END AS PesoUsado_CIA,
        CASE 
            WHEN SUM(c.pesotax) > SUM(c.peso) THEN 'Peso Taxado CTC'
            ELSE 'Peso Bruto CTC'
        END AS TipoPeso_CIA
    FROM 
        tb_airAWB t1
    JOIN 
        tb_airAWBnota b ON t1.codawb = b.codawb
    JOIN
        tb_ctc_esp c ON b.filialctc = c.filialctc
    WHERE 
        t1.nOca IN ({noca_str})
    GROUP BY 
        t1.nOca
    """
    
    try:
        with engine.connect() as conn:
            result = pd.read_sql(sql_query, conn)
            
            if not result.empty:
                # Converte as colunas de peso para float
                result['Peso_Taxado_CTC'] = pd.to_numeric(result['Peso_Taxado_CTC'], errors='coerce')
                result['Peso_Bruto_CTC'] = pd.to_numeric(result['Peso_Bruto_CTC'], errors='coerce')
                result['PesoUsado_CIA'] = pd.to_numeric(result['PesoUsado_CIA'], errors='coerce')
                # A LINHA ABAIXO FOI REMOVIDA - TipoPeso_CIA é texto
                # result['TipoPeso_CIA'] = pd.to_numeric(result['TipoPeso_CIA'], errors='coerce')

                print("Dados de peso CTC encontrados:", result)
                return result.dropna(subset=['PesoUsado_CIA'])

        print("Nenhum dado de peso CTC encontrado para as nOcas fornecidas.")
        return pd.DataFrame()
    
    except Exception as e:
        print(f"Erro ao buscar dados de peso do CTC: {e}")
        return pd.DataFrame()
    # Peso_Taxado_CTC
    
def get_tipo_servico(noca_list: list[str]) -> pd.DataFrame:
    """
    Busca o 'tipo_servico' de todos os CTCs 
    para uma lista de nOcas.
    Retorna um DataFrame com colunas 'Documento' e 'Tipo_Servico'.
    """
    if not noca_list:
        return pd.DataFrame()

    # Formata a lista de nOcas para uso na cláusula IN ('noca1', 'noca2', ...)
    # Filtra valores vazios e únicos para a string do SQL
    valid_noca_list = [n for n in set(noca_list) if n and isinstance(n, str)]
    if not valid_noca_list:
         return pd.DataFrame()
         
    noca_str = ", ".join(f"'{noca}'" for noca in valid_noca_list)
    
    sql_query = f"""
    SELECT 
        t1.nOca AS Documento,
        t1.Tipo_Servico
    FROM 
        tb_airAWB t1
    WHERE 
        t1.nOca IN ({noca_str})
    """
    try:
        with engine.connect() as conn:
            # pd.read_sql já traz o resultado do agrupamento
            result = pd.read_sql(sql_query, conn)
            
            if not result.empty:
                result["Tipo_Servico"] = result["Tipo_Servico"].str.upper().str.strip()
                print("Tipo de Serviço encontrado:", result)
                return result
        print("Nenhum tipo de serviço encontrado para as nOcas fornecidas.")
        return pd.DataFrame()
    
    except Exception as e:
        print(f"Erro ao buscar tipo de serviço em volume: {e}")
        return pd.DataFrame()

# get_ctcs(['95705988286', '95706171620'])  # Exemplo de uso
# get_ctc_peso(['95705988286', '95706171620'])  # Exemplo de uso
# get_tipo_servico(['95705988286', '95706171620'])  # Exemplo de uso