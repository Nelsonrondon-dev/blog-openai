import csv
import openai
import concurrent.futures
import threading
import time

N_HILOS = 2000

with open("0._GPTs.txt", "r", encoding="utf-8") as f:
    apis = [linea.strip() for linea in f]
with open("3._Intencionadas.txt", "r", encoding="utf-8") as f:
    keywords = [linea.strip() for linea in f]
with open("0._Sistema/1._Título.txt", "r", encoding="utf-8") as f:
    titulo_sistema = f.read().strip()
with open("1._Usuario/1._Título.txt", "r", encoding="utf-8") as f:
    titulo_usuario = f.read().strip()

if len(keywords) < N_HILOS:
    N_HILOS = len(keywords)

clave_api_actual = 0

def chatGPT(sistema, usuario):
    global clave_api_actual
    respuesta = ""
    
    while True:
        try:
            clave_api = apis[clave_api_actual]
            openai.api_key = clave_api
            
            respuesta = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": sistema},
                    {"role": "user", "content": usuario}
                ],
                temperature=0.7
            )
            
            texto_respuesta = respuesta.choices[0].message["content"].strip()
            print(f"> Usuario: \n{usuario}")
            print()
            print(f"< ChatGPT: \n{texto_respuesta}")
            print()
            print("############################################################")
            
            clave_api_actual = (clave_api_actual + 1) % len(apis)
            
            return texto_respuesta
            
        except Exception as e:
            print(f"Excepción en chatGPT: {e}")  # Agregado: Imprimir detalles de la excepción
             # Rotar a la siguiente clave API en caso de error
            clave_api_actual = (clave_api_actual + 1) % len(apis)
            time.sleep(1)  # Agregar un retraso antes de intentar la siguiente solicitud

            pass

nombre_archivo = "4._Titulos.csv"
bloqueo = threading.Lock()

def procesar_keyword(keyword):
    titulo = chatGPT(titulo_sistema, titulo_usuario.format(keyword=keyword))
    titulo = titulo.rstrip(".")
    if titulo.startswith('"') and titulo.endswith('"'):
        titulo = titulo[1:-1]
        titulo = titulo.rstrip(".")
    intentos = 0
    while len(titulo) > 70 and intentos < 3:
        titulo = chatGPT(titulo_sistema, f"Haz más pequeño el título: \"{titulo}\"")
        titulo = titulo.rstrip(".")
        if titulo.startswith('"') and titulo.endswith('"'):
            titulo = titulo[1:-1]
            titulo = titulo.rstrip(".")
        intentos += 1
    resultado = [keyword, titulo]
    with bloqueo:
        with open(nombre_archivo, "a", newline="", encoding="utf-8") as archivo_csv:
            escritor = csv.writer(archivo_csv)
            escritor.writerow(resultado)

with open(nombre_archivo, "w", newline="", encoding="utf-8") as archivo_csv:
    escritor = csv.writer(archivo_csv)
    escritor.writerow(["Keyword", "Titulo"])

with concurrent.futures.ThreadPoolExecutor(max_workers=N_HILOS) as ejecutor:
    futuros = [ejecutor.submit(procesar_keyword, keyword) for keyword in keywords]
    for futuro in concurrent.futures.as_completed(futuros):
        pass