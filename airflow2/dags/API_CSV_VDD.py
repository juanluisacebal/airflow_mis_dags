from urllib.parse import quote
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import csv
import os
import time
import subprocess
import json
import random
import logging
import pandas as pd
from airflow.models import Variable



default_args = Variable.get("default_args", deserialize_json=True)
default_args["start_date"] = datetime.strptime(default_args["start_date"], "%Y-%m-%d")
default_args["retry_delay"] = timedelta(minutes=default_args.pop("retry_delay_minutes"))
catchup = default_args.pop("catchup")



ARTISTAS = [
    "Shakira" ,"Beyoncé", "Wyclef Jean"
    ,"Alejandro Sanz", "JAY‐Z", "Lady Gaga",
    "Juanes", "David Guetta"
]
OUTPUT_DIR = "/tmp/musicbrainz_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_artist_mbid(nombre_artista, max_retries=5):
    base_url = "https://musicbrainz.org/ws/2/artist/"
    query = f"?query={quote(nombre_artista)}&fmt=json"
    url = base_url + query
    for intento in range(max_retries):
        try:
            logging.info(f"🔎 Intento {intento + 1}: buscando MBID para {nombre_artista}")
            data = curl_get_json(url)
            if data.get("artists"):
                return data["artists"][0]["id"]
            logging.warning(f"❗ No se encontró MBID para {nombre_artista}")
            return None
        except Exception as e:
            if intento < max_retries - 1:
                espera = 2 ** intento + random.uniform(0, 1)
                logging.warning(f"⚠️ Error al obtener MBID (intento {intento + 1}): {e}. Reintentando en {espera:.2f}s")
                time.sleep(espera)
            else:
                logging.error(f"❌ Fallo final tras {max_retries} intentos para MBID de {nombre_artista}: {e}")
                raise


def curl_get_json(url, user_agent="airflow-musicbrainz/1.0 (yo@juanluisacebal.com)"):
    try:
        result = subprocess.run(
            ["curl", "-s", "-H", f"User-Agent: {user_agent}", url],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            text=True,
        )
        if result.stderr:
            logging.warning(f"[curl stderr] {result.stderr.strip()}")
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        logging.error(f"[curl error] {e.stderr.strip()}")
        raise
    except json.JSONDecodeError:
        logging.error("[json error] No se pudo parsear la salida de curl")
        raise

def get_artist_relations(mbid_artista, max_retries=5):
    url = f"https://musicbrainz.org/ws/2/artist/{mbid_artista}?inc=artist-rels&fmt=json"
    for intento in range(max_retries):
        try:
            logging.info(f"🔄 Intento {intento + 1}: obteniendo relaciones de {mbid_artista}")
            data = curl_get_json(url)
            return data.get("relations", [])
        except Exception as e:
            if intento < max_retries - 1:
                espera = 2 ** intento + random.uniform(0, 1)
                logging.warning(f"⚠️ Error al obtener datos (intento {intento + 1}): {e}. Reintentando en {espera:.2f}s")
                time.sleep(espera)
            else:
                logging.error(f"❌ Fallo final tras {max_retries} intentos para {mbid_artista}: {e}")
                raise

def get_recording_collaborations(mbid_artista, max_retries=5):
    recordings = []
    offset = 0
    limit = 100
    while True:
        url = (f"https://musicbrainz.org/ws/2/recording?"
               f"artist={mbid_artista}&inc=artist-credits&limit={limit}&offset={offset}&fmt=json")
        data = curl_get_json(url)
        recs = data.get("recordings", [])
        recordings.extend(recs)
        if len(recs) < limit:
            break
        offset += limit
        time.sleep(1)
    return recordings

def extract_musicbrainz_data():
    artist_info_list = []
    tags_list = []
    recordings_list = []
    releases_list = []
    works_list = []

    nodos = []
    aristas = []

    processed_collaborators = set()

    for nombre_artista in ARTISTAS:
        mbid_artista = get_artist_mbid(nombre_artista)
        if not mbid_artista:
            continue

        try:
            artist_url = f"https://musicbrainz.org/ws/2/artist/{mbid_artista}?fmt=json&inc=aliases+tags+ratings+annotation+artist-rels+recording-rels+release-rels+work-rels+url-rels"
            artist_data = curl_get_json(artist_url)
            json_path = os.path.join(OUTPUT_DIR, f"{nombre_artista.replace(' ', '_')}-artist.json")
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(artist_data, f, ensure_ascii=False, indent=2)

        except Exception as e:
            logging.warning(f"❗ No se pudo parsear correctamente el JSON para {nombre_artista}: {e}")

        artist_info_list.append({
            "mbid": artist_data.get("id"),
            "name": artist_data.get("name"),
            "country": artist_data.get("country", ""),
            "gender": artist_data.get("gender", ""),
            "area": artist_data.get("area", {}).get("name", ""),
            "begin_date": artist_data.get("life-span", {}).get("begin", ""),
            "end_date": artist_data.get("life-span", {}).get("end", ""),
            "disambiguation": artist_data.get("disambiguation", ""),
            "rating": artist_data.get("rating", {}).get("value", None),
            "rating_count": artist_data.get("rating", {}).get("votes-count", 0)
        })
        artist_info_list[-1]["aliases"] = artist_data.get("aliases", [])
        artist_info_list[-1]["isnis"] = artist_data.get("isnis", [])
        artist_info_list[-1]["relations"] = artist_data.get("relations", [])

        # tags
        for tag in artist_data.get("tags", []):
            tags_list.append({
                "mbid": artist_data.get("id"),
                "tag": tag.get("name"),
                "count": tag.get("count", 0)
            })
        for rel in artist_data.get("recording-rels", []):
            rec = rel.get("recording", {})
            recordings_list.append({
                "mbid": artist_data.get("id"),
                "recording_id": rec.get("id"),
                "title": rec.get("title", ""),
                "type": rec.get("type", "")
            })
        for rel in artist_data.get("release-rels", []):
            relinfo = rel.get("release", {})
            releases_list.append({
                "mbid": artist_data.get("id"),
                "release_id": relinfo.get("id"),
                "title": relinfo.get("title", ""),
                "status": relinfo.get("status", "")
            })
        for rel in artist_data.get("work-rels", []):
            work = rel.get("work", {})
            works_list.append({
                "mbid": artist_data.get("id"),
                "work_id": work.get("id"),
                "title": work.get("title", ""),
                "type": work.get("type", "")
            })

        nodos.append({
            "mbid": mbid_artista,
            "name": nombre_artista
        })

        for rel in artist_data.get("relations", []):
            if rel.get("target-type") == "artist":
                target = rel["artist"]
                aristas.append({
                    "source": mbid_artista,
                    "target": target["id"],
                    "target_name": target["name"],
                    "type": rel.get("type", ""),
                    "relation_begin": rel.get("begin", ""),
                    "relation_end": rel.get("end", "")
                })
                if target["id"] not in processed_collaborators:
                    nodos.append({
                        "mbid": target["id"],
                        "name": target["name"]
                    })
                    processed_collaborators.add(target["id"])

        recs = get_recording_collaborations(mbid_artista)
        for rec in recs:
            for credit in rec.get("artist-credit", []):
                art = credit.get("artist", {})
                collab_id = art.get("id")
                collab_name = art.get("name")
                if collab_id and collab_id != mbid_artista:
                    aristas.append({
                        "source": mbid_artista,
                        "target": collab_id,
                        "target_name": collab_name,
                        "type": "recording",
                        "recording_id": rec.get("id")
                    })
                    if collab_id not in processed_collaborators:
                        nodos.append({
                            "mbid": collab_id,
                            "name": collab_name
                        })
                        processed_collaborators.add(collab_id)

        time.sleep(1)

    for row in artist_info_list:
        for key in ["aliases", "isnis", "relations"]:
            row.pop(key, None)

    from datetime import datetime
    now_str = datetime.utcnow().isoformat()
    for row in artist_info_list:
        row["data_extracted_at"] = now_str

    nodos_file = os.path.join(OUTPUT_DIR, "nodos.csv")
    fieldnames = ["mbid", "name", "country", "gender", "area", "begin_date", "end_date", "disambiguation", "rating", "rating_count", "data_extracted_at"]
    with open(nodos_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(artist_info_list)

    aristas_file = os.path.join(OUTPUT_DIR, "aristas.csv")
    with open(aristas_file, mode="w", newline="", encoding="utf-8") as f:
        fieldnames = ["source", "target", "target_name", "type", "recording_id", "relation_begin", "relation_end"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in aristas:
            for k in ["recording_id", "relation_begin", "relation_end"]:
                row.setdefault(k, "")
        writer.writerows(aristas)

    from collections import Counter

    conteo = Counter(a["source"] for a in aristas)
    top_10 = conteo.most_common(10)

    top_file = os.path.join(OUTPUT_DIR, "top10_nodos.csv")
    with open(top_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["mbid", "name", "num_relaciones"])
        for mbid, count in top_10:
            nombre = next((n["name"] for n in nodos if n["mbid"] == mbid), "")
            writer.writerow([mbid, nombre, count])
   
def top10_aristas():
    df = pd.read_csv('/tmp/musicbrainz_data/aristas.csv')
    top=df['target_name'].value_counts().head(10).to_frame().reset_index()
    top.to_csv('/tmp/musicbrainz_data/top10_aristas.csv', header=['target_name','count'], index=False)
    print("Top 10 Aristas:")
    print(top.to_string(index=False))  

def top10_nodos():
    df = pd.read_csv(os.path.join(OUTPUT_DIR, 'aristas.csv'))
    df2 = pd.read_csv(os.path.join(OUTPUT_DIR, 'nodos.csv'))

    top = (
        df.groupby('source')
          .size()
          .reset_index(name='count')
          .sort_values(by='count', ascending=False)
          .head(10)
    )

    top = top.merge(df2[['mbid', 'name']], left_on='source', right_on='mbid', how='left')
    top = top[['source', 'name', 'count']] 

    salida = os.path.join(OUTPUT_DIR, 'top10_nodos.csv')
    top.to_csv(salida, index=False)

    print("Top 10 Nodos:")
    print(top.to_string(index=False))

def nada():
    print

with DAG(
    dag_id="API_CSV_VDD_PEC4_musicbrainz_artist_collaborations",
    default_args=default_args,
    catchup = catchup,
    #schedule_interval=default_args["schedule_interval"],
    #schedule_interval="*/5 * * * *"
    schedule_interval="@hourly"


) as dag:
    cleanup_musicbrainz = PythonOperator(
        task_id='clean_musicbrainz_data',
        python_callable=nada,
        #bash_command='rm -rf /tmp/musicbrainz_data/*'
    )

    extract_data = PythonOperator(
        task_id="extract_musicbrainz_data",
        python_callable=extract_musicbrainz_data
    )
    task_top10_aristas = PythonOperator(
        task_id='top10_aristas',
        python_callable=top10_aristas,
    )
    task_top10_nodos = PythonOperator(
        task_id='top10_nodos',
        python_callable=top10_nodos,
    )

    cleanup_musicbrainz >> extract_data >> [task_top10_aristas, task_top10_nodos]