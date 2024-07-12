import psycopg2
import requests
import time
import os

db_config = {
    'dbname': 'data_ims',
    'user': 'a5solutions',
    'password': '@5s0lut10ns',
    'host': '18.229.4.57',
    'port': '5432'
}

def conectar_ao_banco(config):
    try:
        #print("Conectando ao banco de dados...")
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

def obter_quantidade_ligacoes_por_estado(conn):
    query = """
    SELECT SUBSTRING(called, 3, 2) AS ddd, COUNT(*) 
    FROM ingenium.cdr 
    WHERE datestart::date = CURRENT_DATE 
    GROUP BY ddd;
    """
    return buscar_dados(conn, query)

def obter_quantidade_produtivas_por_estado(conn):
    query = """
    SELECT SUBSTRING(called, 3, 2) AS ddd, COUNT(*) 
    FROM ingenium.cdr 
    WHERE DATE(datestart) = CURRENT_DATE AND disconnectionreason IN (1, 2) AND durationcall > 6
    GROUP BY ddd;
    """
    return buscar_dados(conn, query)

def obter_quantidade_shortcalls_por_estado(conn):
    query = """
    SELECT SUBSTRING(called, 3, 2) AS ddd, COUNT(*) 
    FROM ingenium.cdr 
    WHERE DATE(datestart) = CURRENT_DATE AND durationcall < 7
    GROUP BY ddd;
    """
    return buscar_dados(conn, query)

def calcular_percentuais(total, produtivas, shortcalls):
    if total == 0:
        return 0, 0, 0
    percentual_produtivas = round((produtivas / total) * 100, 2)
    percentual_improdutivas = round(((total - produtivas) / total) * 100, 2)
    percentual_shortcalls = round((shortcalls / total) * 100, 2)
    return percentual_produtivas, percentual_improdutivas, percentual_shortcalls

def parar_discagem(ddd):
    url = f"http://18.229.4.57:9101/camp/stop/{ddd}"
    payload = {}
    headers = {}

    try:
        response = requests.post(url, headers=headers, data=payload)
        print(f"Parando discagem para DDD {ddd}: {response.text}")
        return response.text
    except Exception as error:
        print(f"Erro ao parar a discagem para DDD {ddd}: {error}")
        return None

def verificar_e_parar_campanha():
    while True:
        try:
            print("Iniciando verificação e parada de campanha...")
            conn = conectar_ao_banco(db_config)
            if conn:
                print("Conectado ao banco de dados")
                ligacoes = obter_quantidade_ligacoes_por_estado(conn)
                produtivas = obter_quantidade_produtivas_por_estado(conn)
                shortcalls = obter_quantidade_shortcalls_por_estado(conn)

                if ligacoes is None or produtivas is None or shortcalls is None:
                    print("Erro ao buscar dados, tentando novamente...")
                    conn.close()
                    time.sleep(300)
                    continue

                ligacoes_dict = {row[0]: row[1] for row in ligacoes}
                produtivas_dict = {row[0]: row[1] for row in produtivas}
                shortcalls_dict = {row[0]: row[1] for row in shortcalls}

                for ddd, total in ligacoes_dict.items():
                    cont_prod = produtivas_dict.get(ddd, 0)
                    cont_short = shortcalls_dict.get(ddd, 0)
                    percentual_produtivas, percentual_improdutivas, percentual_shortcalls = calcular_percentuais(total, cont_prod, cont_short)

                    print(f"DDD {ddd}: Total: {total}, Produtivas: {cont_prod}, Improdutivas: {total - cont_prod}, Shortcalls: {cont_short}")
                    print(f"DDD {ddd}: Percentual Produtivas: {percentual_produtivas}%, Percentual Improdutivas: {percentual_improdutivas}%, Percentual Shortcalls: {percentual_shortcalls}%")

                    if percentual_improdutivas > 80:
                        print(f'Travando Originador - Discagem Parada para DDD {ddd}')
                        #parar_discagem(ddd)

                conn.close()
            else:
                print("Não foi possível conectar ao banco de dados")
            time.sleep(300)  # Espera 5 minutos (300 segundos) antes da próxima verificação
        except Exception as error:
            print(f"Erro inesperado: {error}")
            time.sleep(300)

if __name__ == '__main__':
    verificar_e_parar_campanha()
