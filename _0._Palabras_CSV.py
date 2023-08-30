import os
import pandas as pd

CAMPO_KEYWORD = "Keywords"
CARPETA = "_. Palabras CSV"
ARCHIVO_GUARDAR = "2. Keywords.txt"

def extraer_keywords(carpeta_keywords):
    keywords = []
    for archivo in os.listdir(carpeta_keywords):
        if archivo.endswith(".csv"):
            ruta_archivo = os.path.join(carpeta_keywords, archivo)
            df = pd.read_csv(ruta_archivo)
            if CAMPO_KEYWORD in df.columns:
                keywords.extend(df[CAMPO_KEYWORD].tolist())
    return keywords

def guardar_keywords_a_archivo(keywords, ruta_archivo_guardar):
    with open(ruta_archivo_guardar, "w", encoding="utf-8") as archivo_keywords:
        for keyword in keywords:
            archivo_keywords.write(str(keyword) + "\n")

if __name__ == "__main__":
    ruta_actual = os.getcwd()
    ruta_carpeta_keywords = os.path.join(ruta_actual, CARPETA)

    keywords = extraer_keywords(ruta_carpeta_keywords)
    ruta_archivo_guardar = os.path.join(ruta_actual, ARCHIVO_GUARDAR)
    guardar_keywords_a_archivo(keywords, ruta_archivo_guardar)
    print(f"Extraídas: {len(keywords)}")