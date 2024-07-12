from flask import Flask, render_template, jsonify
import psycopg2
from datetime import datetime
import os
import requests
import threading
import time

app = Flask(__name__)

db_config = {
    'dbname': 'data_ims',
    'user': 'a5solutions',
    'password': '@5s0lut10ns',
    'host': '18.229.4.57',
    'port': '5432'
}

def connect_to_db(config):
    try:
        conn = psycopg2.connect(**config)
        return conn
    except Exception as error:
        print(f"Erro ao conectar ao banco de dados: {error}")
        return None

def fetch_data(conn, query):
    try:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        return rows
    except Exception as error:
        print(f"Erro ao executar a query: {error}")
        return None

def get_ligacoes_count(conn):
    query = "SELECT COUNT(*) FROM ingenium.cdr WHERE datestart::date = CURRENT_DATE;"
    result = fetch_data(conn, query)
    if result:
        #print(f"Resultado Ligacoes: {result}")  # Log do resultado
        return int(result[0][0])
    return 0

def get_produtivas_count(conn):
    query = "SELECT COUNT(*) AS count FROM ingenium.cdr WHERE DATE(datestart) = CURRENT_DATE AND disconnectionreason IN (1, 2, 5, 12);"
    result = fetch_data(conn, query)
    if result:
        #print(f"Resultado Produtivas: {result}")  # Log do resultado
        return int(result[0][0])
    return 0

def get_shortcalls_count(conn):
    query = "SELECT COUNT(*) AS count FROM ingenium.cdr WHERE DATE(datestart) = CURRENT_DATE AND durationcall < 7;"
    result = fetch_data(conn, query)
    if result:
        #print(f"Resultado Shortcalls: {result}")  # Log do resultado
        return int(result[0][0])
    return 0

def calculate_percentages(total, produtivas, improdutivas, shortcalls):
    if total == 0:
        return 0, 0, 0
    porcentagem_prod = round((produtivas / total) * 100, 2)
    porcentagem_improd = round((improdutivas / total) * 100, 2)
    porcentagem_short = round((shortcalls / total) * 100, 2)
    return porcentagem_prod, porcentagem_improd, porcentagem_short

def pararDiscagem():
    url = "http://18.229.4.57:9101/camp/stop"

    payload = {}
    headers = {}

    response = requests.post(url, headers=headers, data=payload)
    print(response.text)
    return response.text

def check_and_stop_campaign():
    while True:
        conn = connect_to_db(db_config)
        if conn:
            cont_lig = get_ligacoes_count(conn)
            print(f"Quantidade de Ligacoes: {cont_lig}")  # Log

            if cont_lig > 1000:
                cont_prod = get_produtivas_count(conn)
                improdutivas = cont_lig - cont_prod

                porcentagem_prod, porcentagem_improd, porcentagem_short = calculate_percentages(cont_lig, cont_prod, improdutivas,0)
                print(f"Porcentagem Produtivas: {porcentagem_prod}%")  # Log do resultado

                if porcentagem_prod <= 20:
                    print('Travando Originador - Discagem Parada')
                    #pararDiscagem()  # Para parar a discagem

            conn.close()
        time.sleep(10)  # Espera 5 minutos (300 segundos) antes da próxima verificação

@app.route('/')
def index():
    conn = connect_to_db(db_config)
    data = {
        'cont_lig': 0,
        'cont_prod': 0,
        'improdutivas': 0,
        'cont_shortcall': 0,
        'porcentagem_prod': 0,
        'porcentagem_improd': 0,
        'porcentagem_short': 0,
        'status': 'Sem conexão com o Banco de Dados'
    }

    if conn:
        cont_lig = get_ligacoes_count(conn)
        print(f"Quantidade de Ligacoes: {cont_lig}")  # Log do resultado

        if cont_lig > 1000:
            cont_prod = get_produtivas_count(conn)
            cont_shortcall = get_shortcalls_count(conn)
            improdutivas = cont_lig - cont_prod

            porcentagem_prod, porcentagem_improd, porcentagem_short = calculate_percentages(cont_lig, cont_prod, improdutivas, cont_shortcall)
            print(f"Porcentagem Produtivas: {porcentagem_prod}%, Porcentagem Improdutivas: {porcentagem_improd}%, Porcentagem Shortcalls: {porcentagem_short}%")  # Log do resultado

            if porcentagem_prod > 20:
                data['status'] = 'Discagem OK'
                print(data['status'])
            else:
                data['status'] = 'Travando Originador - Discagem Parada'
                print(data['status'])
                # pararDiscagem()  # Para parar a discagem (comentei aqui porque já tem a verificação no thread)

            data.update({
                'cont_lig': cont_lig,
                'cont_prod': cont_prod,
                'improdutivas': improdutivas,
                'cont_shortcall': cont_shortcall,
                'porcentagem_prod': porcentagem_prod,
                'porcentagem_improd': porcentagem_improd,
                'porcentagem_short': porcentagem_short,
            })
        else:
            data['status'] = 'Discagem recente'

        conn.close()

    return render_template('index.html', data=data)

if __name__ == '__main__':
    threading.Thread(target=check_and_stop_campaign, daemon=True).start()
    app.run(debug=True)
