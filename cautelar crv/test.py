import psycopg2
import requests
import time
import os
import platform

db_config = {
    'dbname': 'data_ims',
    'user': 'a5solutions',
    'password': '@5s0lut10ns',
    'host': '18.229.4.57',
    'port': '5432'
}

ddd_grupos = {
    'MG1': ['31'], 'MG2': ['32', '33', '34', '35', '37', '38'],
    'BA1': ['73'], 'BA2': ['71', '74', '75', '77'],
    'CE1': ['88'], 'CE2': ['85'],
    'PI1': ['89'], 'PI2': ['86'],
    'MA1': ['99'], 'MA2': ['98'],
    'PE1': ['87'], 'PE2': ['81'],
    'RJ1': ['22'], 'RJ2': ['21', '24'],
    'ES1': ['28'], 'ES2': ['27'],
    'PA1': ['98'], 'PA2': ['91'],
    'AM1': ['97'], 'AM2': ['92'],
    'SP': ['11', '12', '13', '14', '15', '16', '17', '18', '19'],
    'GO1': ['64'], 'GO2': ['61', '62', '63'],
    'MT1': ['66'], 'MT2': ['65'],
    'MS': ['67'],
    'PR1': ['42'], 'PR2': ['41', '43', '44', '45', '46'],
    'SC1': ['48'], 'SC2': ['47', '49'],
    'RS1': ['53'], 'RS2': ['51', '54', '55'],
    'OUTROS': ['27', '28', '61', '62', '63', '65']
}

def conectar_ao_banco(config):
    try:
        conn = psycopg2.connect(**config)
        print("Conexão com o banco de dados estabelecida com sucesso.")
        return conn
    except psycopg2.OperationalError as op_error:
        print(f"Erro operacional ao conectar ao banco de dados: {op_error}")
    except psycopg2.DatabaseError as db_error:
        print(f"Erro de banco de dados ao conectar: {db_error}")
    except Exception as error:
        print(f"Erro desconhecido ao conectar ao banco de dados: {error}")
    return None

def buscar_dados(conn, query):
    try:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        return rows
    except Exception as error:
        print(f"Erro ao executar a query: {error}")
        return None

def obter_quantidade_ligacoes_por_ddd(conn):
    query = """
    SELECT SUBSTRING(called, 3, 2) AS ddd, COUNT(*) 
    FROM ingenium.cdr 
    WHERE datestart::date = CURRENT_DATE 
    GROUP BY ddd;
    """
    return buscar_dados(conn, query)

def obter_quantidade_produtivas_por_ddd(conn):
    query = """
    SELECT SUBSTRING(called, 3, 2) AS ddd, COUNT(*) 
    FROM ingenium.cdr 
    WHERE DATE(datestart) = CURRENT_DATE AND disconnectionreason IN (1, 2) AND durationcall > 6
    GROUP BY ddd;
    """
    return buscar_dados(conn, query)

def obter_quantidade_shortcalls_por_ddd(conn):
    query = """
    SELECT SUBSTRING(called, 3, 2) AS ddd, COUNT(*) 
    FROM ingenium.cdr 
    WHERE DATE(datestart) = CURRENT_DATE AND durationcall < 7
    GROUP BY ddd;
    """
    return buscar_dados(conn, query)

def calcular_percentuais(total, cont_prod, cont_short):
    if total > 0:
        percentual_produtivas = (cont_prod / total) * 100
        percentual_improdutivas = ((total - cont_prod) / total) * 100
        percentual_shortcalls = (cont_short / total) * 100
    else:
        percentual_produtivas = 0
        percentual_improdutivas = 0
        percentual_shortcalls = 0
    return percentual_produtivas, percentual_improdutivas, percentual_shortcalls

def parar_discagem():
    url = "http://18.229.4.57:9101/camp/stop"
    payload = {}
    headers = {}

    try:
        response = requests.post(url, headers=headers, data=payload)
        print(f"Parando discagem: {response.text}")
        return response.text
    except Exception as error:
        print(f"Erro ao parar a discagem: {error}")
        return None

def iniciar_discagem():
    url = "http://18.229.4.57:9101/camp/start"
    payload = {}
    headers = {}

    try:
        response = requests.post(url, headers=headers, data=payload)
        print(f"Iniciando discagem: {response.text}")
        return response.text
    except Exception as error:
        print(f"Erro ao iniciar a discagem: {error}")
        return None

def atualizar_target_audience(conn, ddd):
    try:
        cur = conn.cursor()
        update_query = f"""
        UPDATE markings.target_audience
        SET status = 'stopped'
        WHERE ddd = '{ddd}';
        """
        cur.execute(update_query)
        conn.commit()
        print(f"Atualização realizada na tabela target_audience para DDD {ddd}.")
        cur.close()
    except Exception as error:
        print(f"Erro ao atualizar a tabela target_audience para DDD {ddd}: {error}")

def limpar_terminal():
    sistema = platform.system()
    if sistema == "Windows":
        os.system("cls")
    else:
        os.system("clear")

def verificar_e_parar_campanha():
    while True:
        try:
            print("Iniciando verificação e parada de campanha...")
            conn = conectar_ao_banco(db_config)
            if conn:
                print("Conectado ao banco de dados")
                ligacoes = obter_quantidade_ligacoes_por_ddd(conn)
                produtivas = obter_quantidade_produtivas_por_ddd(conn)
                shortcalls = obter_quantidade_shortcalls_por_ddd(conn)

                print("Dados brutos:")
                print("Ligações:", ligacoes)
                print("Produtivas:", produtivas)
                print("Shortcalls:", shortcalls)

                ligacoes_dict = {row[0]: row[1] for row in ligacoes}
                produtivas_dict = {row[0]: row[1] for row in produtivas}
                shortcalls_dict = {row[0]: row[1] for row in shortcalls}

                print("Dados processados:")
                print("Ligações:", ligacoes_dict)
                print("Produtivas:", produtivas_dict)
                print("Shortcalls:", shortcalls_dict)

                ddds_a_parar = set()
                for ddd in ligacoes_dict.keys():
                    total = ligacoes_dict.get(ddd, 0)
                    cont_prod = produtivas_dict.get(ddd, 0)
                    cont_short = shortcalls_dict.get(ddd, 0)

                    print(f"\nDDD {ddd}")
                    print(f"Total ligações: {total}")
                    print(f"Total produtivas: {cont_prod}")
                    print(f"Total shortcalls: {cont_short}")

                    percentual_produtivas, percentual_improdutivas, percentual_shortcalls = calcular_percentuais(total, cont_prod, cont_short)

                    print(f"DDD {ddd}: Total: {total}, Produtivas: {cont_prod}, Improdutivas: {total - cont_prod}, Shortcalls: {cont_short}")
                    print(f"DDD {ddd}: Percentual Produtivas: {percentual_produtivas}%, Percentual Improdutivas: {percentual_improdutivas}%, Percentual Shortcalls: {percentual_shortcalls}%")

                    if percentual_improdutivas > 75 and total > 100:
                        print(f'Travando Originador - Discagem Parada para DDD {ddd}')
                        for grupo, ddds in ddd_grupos.items():
                            if ddd in ddds:
                                ddds_a_parar.update(ddds)

                #if ddds_a_parar:
                    #print(f'DDDs a serem parados: {ddds_a_parar}')
                    #parar_discagem()
                    #for ddd in ddds_a_parar:

                        #atualizar_target_audience(conn, ddd)
                    #iniciar_discagem()

                conn.close()
            else:
                print("Não foi possível conectar ao banco de dados")
            time.sleep(300)  # Espera 5 minutos (300 segundos) antes da próxima verificação
            limpar_terminal()
        except Exception as error:
            print(f"Erro inesperado: {error}")
            time.sleep(300)

if __name__ == '__main__':
    verificar_e_parar_campanha()
