import psycopg2
import requests
import time

# Configurações do banco de dados
db_config = {
    'dbname': 'data_ims',
    'user': 'a5solutions',
    'password': '@5s0lut10ns',
    'host': '18.229.4.57',
    'port': '5432'
}

# Função para conectar ao banco de dados
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

# Função para buscar dados no banco de dados
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

# Função para obter quantidades por estado
def obter_quantidades_por_estado(conn):
    queries = {
        'SP': """
            SELECT 'SP' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) IN ('11', '12', '13', '14', '15', '16', '17', '18', '19')
            """,
        'MG1': """
            SELECT 'MG1' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) = '32'
            """,
        'MG2': """
            SELECT 'MG2' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) NOT IN ('32', '11', '12', '13', '14', '15', '16', '17', '18', '19')
            """,
        'BA1': """
            SELECT 'BA1' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) = '73'
            """,
        'BA2': """
            SELECT 'BA2' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) NOT IN ('73')
            """,
        'CE1': """
            SELECT 'CE1' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) = '88'
            """,
        'CE2': """
            SELECT 'CE2' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) = '85'
            """,
        'PI1': """
            SELECT 'PI1' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) = '89'
            """,
        'PI2': """
            SELECT 'PI2' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) = '86'
            """,
        'MA1': """
            SELECT 'MA1' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) = '99'
            """,
        'MA2': """
            SELECT 'MA2' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) = '98'
            """,
        'PE1': """
            SELECT 'PE1' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) = '87'
            """,
        'PE2': """
            SELECT 'PE2' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) NOT IN ('87')
            """,
        'RJ1': """
            SELECT 'RJ1' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) = '22'
            """,
        'RJ2': """
            SELECT 'RJ2' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) NOT IN ('22')
            """,
        'ES1': """
            SELECT 'ES1' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) = '28'
            """,
        'ES2': """
            SELECT 'ES2' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) = '27'
            """,
        'PA1': """
            SELECT 'PA1' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) = '93'
            """,
        'PA2': """
            SELECT 'PA2' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) NOT IN ('93')
            """,
        'AM1': """
            SELECT 'AM1' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) = '97'
            """,
        'AM2': """
            SELECT 'AM2' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) = '92'
            """,
        'GO1': """
            SELECT 'GO1' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) = '64'
            """,
        'GO2': """
            SELECT 'GO2' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) NOT IN ('64')
            """,
        'MT1': """
            SELECT 'MT1' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) = '66'
            """,
        'MT2': """
            SELECT 'MT2' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) NOT IN ('66')
            """,
        'MS': """
            SELECT 'MS' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) IN ('67')
            """,
        'PR1': """
            SELECT 'PR1' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) = '42'
            """,
        'PR2': """
            SELECT 'PR2' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) NOT IN ('42')
            """,
        'SC1': """
            SELECT 'SC1' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) = '48'
            """,
        'SC2': """
            SELECT 'SC2' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) NOT IN ('48')
            """,
        'RS1': """
            SELECT 'RS1' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) = '53'
            """,
        'RS2': """
            SELECT 'RS2' AS estado,
                   COUNT(*) AS total,
                   SUM(CASE WHEN disconnectionreason IN (1, 2) AND durationcall > 6 THEN 1 ELSE 0 END) AS produtivas,
                   SUM(CASE WHEN durationcall < 7 THEN 1 ELSE 0 END) AS shortcalls
            FROM ingenium.cdr 
            WHERE DATE(datestart) = CURRENT_DATE AND SUBSTRING(called, 3, 2) NOT IN ('53')
            """
    }

    resultados = {}
    for estado, query in queries.items():
        dados_estado = buscar_dados(conn, query)
        resultados[estado] = dados_estado

    return resultados

# Função para calcular percentuais
def calcular_percentuais(total, produtivas, shortcalls):
    if total == 0:
        return 0, 0, 0, 0
    
    percentual_produtivas = round((produtivas / total) * 100, 2)
    percentual_improdutivas = round(((total - produtivas - shortcalls) / total) * 100, 2)
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

# Função principal para executar o processo
def executar_processo():
    conn = conectar_ao_banco(db_config)
    if conn:
        dados_por_estado = obter_quantidades_por_estado(conn)
        if dados_por_estado:
            print("Resultados por estado:")
            for estado, dados in dados_por_estado.items():
                for row in dados:
                    estado = row[0]
                    total = row[1]
                    produtivas = row[2]
                    shortcalls = row[3]

                    percentual_produtivas, percentual_shortcalls = calcular_percentuais(total, produtivas, shortcalls)
                    
                    print(f"Estado: {estado}, Total: {total}, Produtivas: {produtivas}, Shortcalls: {shortcalls}")
                    print(f"Percentual de chamadas produtivas: {percentual_produtivas:.2f}%")
                    print(f"Percentual de shortcalls: {percentual_shortcalls:.2f}%")
                    print("-" * 50)
        else:
            print("Não foram encontrados dados para processar.")
        conn.close()
    else:
        print("Não foi possível conectar ao banco de dados.")

# Execução do processo principal
executar_processo()
