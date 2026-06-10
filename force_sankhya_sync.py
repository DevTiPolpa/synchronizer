#!/usr/bin/env python3

import requests
import pyodbc
import json
from datetime import datetime
import time

# --- Configuration --- #

SANKHYA_AUTH_URL = "https://api.sankhya.com.br/authenticate"
SANKHYA_VIEW_URL = "https://api.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadView&outputType=json"
CLIENT_ID = "3fcaeed4-542d-4326-8e72-fe56866aa3be"
CLIENT_SECRET = "jUVF6cuRF3Q0mvygtyXpzCb1rK82nYqX"
SANKHYA_APP_TOKEN = "9c5ded4b-13c3-4451-b084-b59a7050ad66"

DB_SERVER = "192.168.153.163"
DB_NAME = "BI_PolpaBrasil"
DB_USER = "sa"
DB_PASSWORD = "Ppbrti@25"
DB_TABLE = "INDICADORESCOMERCIAIS"

# --- Helpers --- #

def safe_int(value, field_name=""):
    try:
        if value is None: return None
        # Converte para float primeiro para lidar com strings como "9010100003.0"
        v = int(float(str(value).replace(',', '.')))
        
        # Se for REFERENCIA, permitimos BIGINT (sem trava de 32 bits)
        if field_name == "REFERENCIA":
            return v
            
        # Para outros campos INT padrão, mantemos a trava de 32-bit se necessário, 
        # mas como o SQL Server pode ter BIGINT em outros lugares, vamos ser mais permissivos
        # Se houver erro de overflow no SQL, o próprio banco avisará.
        return v
    except:
        return None

def safe_float(value):
    try:
        if value is None: return 0.0
        return float(str(value).replace(',', '.'))
    except:
        return 0.0

# --- Functions --- #

def get_access_token():
    print("Attempting to get access token...")
    headers = {
        "X-Token": SANKHYA_APP_TOKEN,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    }
    response = requests.post(SANKHYA_AUTH_URL, headers=headers, data=payload, timeout=30)
    response.raise_for_status()
    print("Access token obtained successfully.")
    return response.json().get("access_token")

def get_all_view_data(access_token):
    all_entities = []
    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-Token": SANKHYA_APP_TOKEN,
        "Content-Type": "application/json"
    }

    current_min_nunota = 0
    step = 50000
    max_nunota_to_check = 3000000 

    print(f"Starting data fetch using NUNOTA ranges...")

    while current_min_nunota < max_nunota_to_check:
        current_max_nunota = current_min_nunota + step
        where_clause = f"NUNOTA >= {current_min_nunota} AND NUNOTA < {current_max_nunota}"

        payload = {
            "serviceName": "CRUDServiceProvider.loadView",
            "requestBody": {
                "query": {
                    "viewName": "AD_VW_EXPORTA_BI_INDICADORESCOMERCIAIS",
                    "where": {"$": where_clause},
                    "fields": {"field": {"$": "*"}}
                }
            }
        }

        try:
            print(f"Buscando faixa: {current_min_nunota} - {current_max_nunota}")
            response = requests.post(SANKHYA_VIEW_URL, headers=headers, json=payload, timeout=90)
            response.raise_for_status()
            view_data = response.json()

            if "responseBody" in view_data:
                records_obj = view_data["responseBody"].get("records", {})
                entities = records_obj.get("record", [])

                if isinstance(entities, dict):
                    entities = [entities]

                if entities:
                    all_entities.extend(entities)
                    print(f"  -> {len(entities)} registros | Total acumulado: {len(all_entities)}")

            current_min_nunota = current_max_nunota
            time.sleep(0.2)

        except Exception as e:
            print(f"[ERRO API] Faixa {current_min_nunota}: {e}")
            current_min_nunota = current_max_nunota
            time.sleep(2)

    print(f"Total coletado: {len(all_entities)}")
    return all_entities

def transform_data(entities):
    print("Transforming data...")
    transformed = []

    for entity in entities:
        record = {}
        for key, value_obj in entity.items():
            value = value_obj.get("$") if isinstance(value_obj, dict) else value_obj

            # Datas (DATE)
            if key in ["DTNEG", "AD_DT_EMBARQUE", "DTPREVENT", "AD_DT_ORIGINAL", "DTMOV", "AD_DTENTREGAREAL"] and value:
                for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                    try:
                        record[key] = datetime.strptime(str(value), fmt).date()
                        break
                    except: continue
                else: record[key] = None

            # Data/Hora (DATETIME2)
            elif key in ["DTATUAL", "DTALTER"] and value:
                for fmt in ("%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                    try:
                        record[key] = datetime.strptime(str(value), fmt)
                        break
                    except: continue
                else: record[key] = None

            # Decimais
            elif key in ["QTDENTREGUE", "QTDNEG", "PERCDESCBONIF", "VLRICMS", "QTDNEG_CALC",
                         "QTDPENDENTE", "PESOLIQ", "VALORPENDENTE", "VLR_PIS", "VLR_COFINS",
                         "VLR_ST", "AD_VLFRETE", "QTD_VOA"]:
                record[key] = safe_float(value)

            # Inteiros (incluindo suporte a BIGINT para REFERENCIA)
            elif key in ["CODPARC", "NUNOTA", "CODTIPOPER", "REFERENCIA", "AD_REEMISSAO", "CODPROD", "SEQUENCIA"]:
                record[key] = safe_int(value, key)

            else:
                record[key] = value

        transformed.append(record)
    return transformed

def sync_to_sql_server(data):
    if not data:
        print("Sem dados para processar.")
        return

    print(f"Conectando ao SQL Server ({DB_SERVER})...")
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DB_SERVER};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASSWORD}"
    
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        print(f"Limpando tabela {DB_TABLE}...")
        cursor.execute(f"DELETE FROM {DB_TABLE}")

        sql_table_cols = [
            "TIPMOV", "QTDENTREGUE", "QTDNEG", "CODPARC", "RAZAOSOCIAL", "NUNOTA", "NUMNOTA",
            "TIPPESSOA", "DTNEG", "AD_DT_EMBARQUE", "DTPREVENT", "AD_DT_ORIGINAL", "CODTIPOPER",
            "DESCROPER", "REFERENCIA", "DESCRPROD", "NCM", "PERCDESCBONIF", "VLRICMS",
            "QTDNEG_CALC", "CONTROLE", "QTDPENDENTE", "PESOLIQ", "ValorPendente", "VLR_PIS",
            "VLR_COFINS", "VLR_ST", "PROJETO", "PERFILPARC", "AD_MERCADO_VENDAS", "VENDEDOR",
            "DESCRGRUPOPROD", "CONFIRMADA", "NOMECID", "UF", "DESCRICAO",
            "NECESSIDADE_AGENDAMENTO_ENTREGA", "REPRESENTANTE", "DESCRICAO_SABOR", "FORMATO_PRODUTO",
            "QTD_VOA", "DTATUAL", "DTALTER", "DTMOV", "AD_DTENTREGAREAL", "AD_VLFRETE",
            "AD_REEMISSAO", "AD_MOTIVO_REPROG_EMB" , "OBSERVACAO"
        ]

        column_mapping = {
            "QTDNEG_AJUSTADO": "QTDNEG_CALC",
            "PAIS": "DESCRICAO",
            "VALORPENDENTE": "ValorPendente"
        }

        placeholders = ", ".join(["?"] * len(sql_table_cols))
        insert_sql = f"INSERT INTO {DB_TABLE} ({', '.join(sql_table_cols)}) VALUES ({placeholders})"

        total = len(data)
        print(f"Iniciando inserção de {total} registros...")

        batch_size = 1000
        for i in range(0, total, batch_size):
            batch = data[i:i + batch_size]
            batch_values = []
            for record in batch:
                row = []
                for col in sql_table_cols:
                    api_key = next((k for k, v in column_mapping.items() if v == col), col)
                    val = record.get(api_key)
                    row.append(val)
                batch_values.append(tuple(row))
            
            cursor.executemany(insert_sql, batch_values)
            print(f"Progresso: {min(i + batch_size, total)}/{total}")

        conn.commit()
        print("Sincronização concluída com sucesso!")

    except Exception as e:
        print(f"[ERRO SQL] {e}")
        if 'conn' in locals(): conn.rollback()
    finally:
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    start_time = time.time()
    try:
        token = get_access_token()
        raw = get_all_view_data(token)
        clean = transform_data(raw)
        sync_to_sql_server(clean)
        end_time = time.time()
        print(f"Tempo total de execução: {round(end_time - start_time, 2)} segundos.")
    except Exception as e:
        print(f"\nFalha crítica: {e}")