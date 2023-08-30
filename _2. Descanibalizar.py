import unidecode
import spacy
from nltk import word_tokenize
from nltk.corpus import stopwords
import pandas as pd
from scipy.spatial.distance import cdist
import os
import concurrent.futures

N_LINEAS = 30000
N_HILOS = 1

nlp = spacy.load('es_core_news_lg')
palabras_parada = set(stopwords.words('spanish'))

def remover_acentos(linea: str) -> str:
    return unidecode.unidecode(linea)

def remover_palabras_parada(linea: str) -> str:
    palabras_tokenizadas = word_tokenize(linea, language="spanish")
    frase_filtrada = [palabra for palabra in palabras_tokenizadas if palabra.casefold() not in palabras_parada]
    return " ".join(frase_filtrada)

def obtener_contar_palabras(linea: str) -> int:
    return len(linea.split())

def significado(linea: str) -> str:
    doc = nlp(linea)
    return "".join(sorted([token.lemma_ for token in doc]))

def remover_duplicados(lineas: list) -> list:
    lineas_unicas = []
    significados_unicos = set()
    for linea in lineas:
        linea_sin_acentos = remover_acentos(linea)
        linea_sin_palabras_parada = remover_palabras_parada(linea_sin_acentos)
        linea_significado = significado(linea_sin_palabras_parada)
        if linea_significado not in significados_unicos:
            lineas_unicas.append(linea)
            significados_unicos.add(linea_significado)
    return lineas_unicas

def remover_semanticamente_similares(df: pd.DataFrame, umbral: float) -> pd.DataFrame:
    nlp_lineas = [(linea, nlp(linea)) for linea in df['Original']]
    embeddings = [doc.vector for _, doc in nlp_lineas]
    contar_palabras = df['Contar palabras'].values
    matriz_similitud = 1 - cdist(embeddings, embeddings, metric='cosine')
    n = len(matriz_similitud)
    lineas_similares = [set() for _ in range(n)]
    for i in range(n):
        for j in range(i + 1, n):
            if matriz_similitud[i, j] > umbral:
                lineas_similares[i].add(j)
                lineas_similares[j].add(i)
    lineas_a_remover = set()
    for i in range(n):
        if lineas_similares[i]:
            lineas_similares[i].add(i)
            lineas_a_mantener = max(lineas_similares[i], key=lambda j: contar_palabras[j])
            lineas_a_remover.update(lineas_similares[i] - {lineas_a_mantener})
    return df[~df.index.isin(lineas_a_remover)]

def dividir_archivo(ruta_archivo: str, n_lineas: int) -> list:
    archivos_temporales = []
    with open(ruta_archivo, "r", encoding="utf-8") as f:
        lineas = [linea.rstrip() for linea in f]
        n_archivos = len(lineas) // n_lineas + 1
        for i in range(n_archivos):
            archivo_temporal = f"temp_{i}.txt"
            lineas_temporales = lineas[i * n_lineas : (i + 1) * n_lineas]
            with open(archivo_temporal, "w", encoding="utf-8") as temp_file:
                for linea in lineas_temporales:
                    temp_file.write(f"{linea}\n")
            archivos_temporales.append(archivo_temporal)
    return archivos_temporales

def juntar_archivos_temporales(archivos_temporales: list, archivo_salida: str):
    with open(archivo_salida, "w", encoding="utf-8") as output_file:
        for archivo_temporal in archivos_temporales:
            with open(archivo_temporal, "r", encoding="utf-8") as temp_file:
                output_file.write(temp_file.read())
            os.remove(archivo_temporal)

def procesar_archivo(ruta_archivo: str):
    with open(ruta_archivo, "r", encoding="utf-8") as f:
        lineas = [linea.rstrip() for linea in f]

    print(f"Iniciales: {len(lineas)}")

    lineas_unicas = remover_duplicados(lineas)
    df = pd.DataFrame(lineas_unicas, columns=['Original'])
    df['Contar palabras'] = df['Original'].str.split().str.len()
    df = remover_semanticamente_similares(df, 0.9)

    with open(ruta_archivo, "w", encoding="utf-8") as f:
        for linea in df['Original']:
            f.write(f"{linea}\n")

    print(f"Eliminadas: {len(lineas) - len(df['Original'])}")
    print(f"Finales: {len(df['Original'])}")

def main():
    ruta_archivo = "3. Intencionadas.txt"
    if os.path.isfile(ruta_archivo):
        if os.path.getsize(ruta_archivo) > N_LINEAS:
            archivos_temporales = dividir_archivo(ruta_archivo, N_LINEAS)

            with concurrent.futures.ThreadPoolExecutor(max_workers=N_HILOS) as ejecutor:
                futuros = [ejecutor.submit(procesar_archivo, archivo_temporal) for archivo_temporal in archivos_temporales]
                for futuro in concurrent.futures.as_completed(futuros):
                    pass

            juntar_archivos_temporales(archivos_temporales, ruta_archivo)
        else:
            procesar_archivo(ruta_archivo)
    else:
        print("El archivo no existe.")

if __name__ == '__main__':
    main()
