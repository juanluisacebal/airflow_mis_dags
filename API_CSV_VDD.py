from urllib.parse import quote
from airflow import DAG
from airflow.operators.python import PythonOperator
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

# Lista de artistas de interés
ARTISTAS = [
    "Shakira", "Rosalía", "Luis Fonsi", "Enrique Iglesias", "Ricky Martin",
    "Alejandro Sanz", "Maluma", "Carlos Vives", "Juanes", "Pablo Alborán",
    "Marc Anthony", "J Balvin", "Daddy Yankee", "Bad Bunny", "Karol G",
    "Anitta", "Nicky Jam", "Thalía", "Gloria Trevi", "Reik",
    "Sebastián Yatra", "Camila Cabello", "Becky G", "Ozuna", "C. Tangana",
    "Rauw Alejandro", "Myke Towers", "CNCO", "Wisin", "Yandel",
    "Jennifer Lopez", "Natalia Lafourcade", "Lali", "TINI", "Maná",
    "Romeo Santos", "Prince Royce", "La India", "Pedro Capó", "Gente de Zona"
]

# Ruta para guardar los archivos CSV
OUTPUT_DIR = "/tmp/musicbrainz_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Función para obtener el MBID de un artista
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


# Función auxiliar para obtener JSON usando curl y subprocess
def curl_get_json(url, user_agent="airflow-musicbrainz/1.0 (juanlu@example.com)"):
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

# Función para obtener relaciones de un artista con reintentos y logs
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

# Función principal para extraer y guardar datos
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

            # Flatten artist_data to CSV
            try:
                flat_df = pd.json_normalize(artist_data)
                flat_csv_path = os.path.join(OUTPUT_DIR, f"{nombre_artista.replace(' ', '_')}-artist_flat.csv")
                flat_df.to_csv(flat_csv_path, index=False, encoding='utf-8')
                logging.info(f"✅ Flattened CSV saved: {flat_csv_path}")
            except Exception as e:
                logging.warning(f"❗ Error flattening artist_data for {nombre_artista}: {e}")
        except Exception as e:
            logging.warning(f"❗ No se pudo parsear correctamente el JSON para {nombre_artista}: {e}")

        # Collect basic artist info with enriched fields
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
        # Collect tags
        for tag in artist_data.get("tags", []):
            tags_list.append({
                "mbid": artist_data.get("id"),
                "tag": tag.get("name"),
                "count": tag.get("count", 0)
            })
        # Collect recording relations
        for rel in artist_data.get("recording-rels", []):
            rec = rel.get("recording", {})
            recordings_list.append({
                "mbid": artist_data.get("id"),
                "recording_id": rec.get("id"),
                "title": rec.get("title", ""),
                "type": rec.get("type", "")
            })
        # Collect release relations
        for rel in artist_data.get("release-rels", []):
            relinfo = rel.get("release", {})
            releases_list.append({
                "mbid": artist_data.get("id"),
                "release_id": relinfo.get("id"),
                "title": relinfo.get("title", ""),
                "status": relinfo.get("status", "")
            })
        # Collect work relations
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

        # Artist-to-artist relations from artist_data
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
                    # add collaborator node stub (will be enriched later)
                    nodos.append({
                        "mbid": target["id"],
                        "name": target["name"]
                    })
                    processed_collaborators.add(target["id"])

        # Recording-based collaborations
        recs = get_recording_collaborations(mbid_artista)
        for rec in recs:
            for credit in rec.get("artist-credit", []):
                art = credit.get("artist", {})
                collab_id = art.get("id")
                collab_name = art.get("name")
                if collab_id and collab_id != mbid_artista:
                    # add edge for this collaboration
                    aristas.append({
                        "source": mbid_artista,
                        "target": collab_id,
                        "target_name": collab_name,
                        "type": "recording",
                        "recording_id": rec.get("id")
                    })
                    # add collaborator node if new
                    if collab_id not in processed_collaborators:
                        nodos.append({
                            "mbid": collab_id,
                            "name": collab_name
                        })
                        processed_collaborators.add(collab_id)

        time.sleep(1)  # Respetar el límite de 1 solicitud por segundo

    # Write enriched nodes CSV
    nodos_file = os.path.join(OUTPUT_DIR, "nodos.csv")
    fieldnames = ["mbid", "name", "country", "gender", "area", "begin_date", "end_date", "disambiguation", "rating", "rating_count"]
    with open(nodos_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(artist_info_list)

    # Guardar aristas en CSV
    aristas_file = os.path.join(OUTPUT_DIR, "aristas.csv")
    with open(aristas_file, mode="w", newline="", encoding="utf-8") as f:
        fieldnames = ["source", "target", "target_name", "type", "recording_id"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        # Fill missing keys with empty string for rows without recording_id
        for row in aristas:
            if "recording_id" not in row:
                row["recording_id"] = ""
        writer.writerows(aristas)

    from collections import Counter

    # Contar aristas salientes por nodo
    conteo = Counter(a["source"] for a in aristas)
    top_10 = conteo.most_common(10)

    # Guardar top 10 en CSV
    top_file = os.path.join(OUTPUT_DIR, "top10_nodos.csv")
    with open(top_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["mbid", "name", "num_relaciones"])
        for mbid, count in top_10:
            nombre = next((n["name"] for n in nodos if n["mbid"] == mbid), "")
            writer.writerow([mbid, nombre, count])

    # Write artist_info.csv
    info_file = os.path.join(OUTPUT_DIR, "artist_info.csv")
    with open(info_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["mbid", "name", "country", "gender", "area",
                                              "begin_date", "end_date", "disambiguation",
                                              "rating", "rating_count"])
        writer.writeheader()
        writer.writerows(artist_info_list)

    # Write tags.csv
    tags_file = os.path.join(OUTPUT_DIR, "tags.csv")
    with open(tags_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["mbid", "tag", "count"])
        writer.writeheader()
        writer.writerows(tags_list)

    # Write recordings.csv
    recordings_file = os.path.join(OUTPUT_DIR, "recordings.csv")
    with open(recordings_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["mbid", "recording_id", "title", "type"])
        writer.writeheader()
        writer.writerows(recordings_list)

    # Write releases.csv
    releases_file = os.path.join(OUTPUT_DIR, "releases.csv")
    with open(releases_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["mbid", "release_id", "title", "status"])
        writer.writeheader()
        writer.writerows(releases_list)

    # Write works.csv
    works_file = os.path.join(OUTPUT_DIR, "works.csv")
    with open(works_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["mbid", "work_id", "title", "type"])
        writer.writeheader()
        writer.writerows(works_list)

# Definición del DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 5, 6),
    "retries": 1,
    "retry_delay": timedelta(seconds=30)
}

with DAG(
    dag_id="API_CSV_VDD_PEC4_musicbrainz_artist_collaborations",
    default_args=default_args,
    schedule_interval="*/2 * * * *",
    #schedule_interval="@daily",
    catchup=False
) as dag:

    extract_data = PythonOperator(
        task_id="extract_musicbrainz_data",
        python_callable=extract_musicbrainz_data
    )