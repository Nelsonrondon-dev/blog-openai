import openai
import concurrent.futures
import threading
import time


N_HILOS = 2000

with open("0._GPTs.txt", "r", encoding="utf-8") as f:
    apis = [linea.strip() for linea in f]
with open("2._Keywords.txt", "r", encoding="utf-8") as f:
    keywords = [linea.strip() for linea in f]
with open("0._Sistema/0._Intención.txt", "r", encoding="utf-8") as f:
    titulo_sistema = f.read().strip()
with open("1._Usuario/0._Intención.txt", "r", encoding="utf-8") as f:
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
            print(f"Excepción en chatGPT: {e}")
             # Rotar a la siguiente clave API en caso de error
            clave_api_actual = (clave_api_actual + 1) % len(apis)
            time.sleep(1)  
            pass

nombre_archivo = "3._Intencionadas.txt"
bloqueo = threading.Lock()

def procesar_keyword(keyword):
    titulo = chatGPT(titulo_sistema, titulo_usuario.format(keyword=keyword)).replace("?","").replace("¿","")
    titulo = titulo.rstrip(".")
    if titulo.startswith('"') and titulo.endswith('"'):
        titulo = titulo[1:-1]
        titulo = titulo.rstrip(".")
    with bloqueo:
        with open(nombre_archivo, "a", encoding="utf-8") as archivo_txt:
            archivo_txt.write(titulo + "\n")

with open(nombre_archivo, "w", encoding="utf-8"):
    pass

with concurrent.futures.ThreadPoolExecutor(max_workers=N_HILOS) as ejecutor:
    futuros = [ejecutor.submit(procesar_keyword, keyword) for keyword in keywords]
    for futuro in concurrent.futures.as_completed(futuros):
        pass
