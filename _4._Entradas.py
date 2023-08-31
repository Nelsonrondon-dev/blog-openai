import os
import csv
import openai
import re
import concurrent.futures
import threading
import markdown  # Agregado: Importar la librería markdown

N_HILOS = 2000

with open("0._GPTs.txt", "r", encoding="utf-8") as f:
    gpts = [line.strip() for line in f]
with open("4._Titulos.csv", newline='', encoding='utf-8') as f:
    csv_titulos = csv.DictReader(f)
    titulos = list(csv_titulos)
with open("0._Sistema/2._Imagen.txt", "r", encoding="utf-8") as f:
    imagen_sistema = f.read().strip()
with open("1._Usuario/2._Imagen.txt", "r", encoding="utf-8") as f:
    imagen_usuario = f.read().strip()
with open("0._Sistema/4._Estructura.txt", "r", encoding="utf-8") as f:
    estructura_sistema = f.read().strip()
with open("1._Usuario/4._Estructura.txt", "r", encoding="utf-8") as f:
    estructura_usuario = f.read().strip()
with open("0._Sistema/5._Sección.txt", "r", encoding="utf-8") as f:
    seccion_sistema = f.read().strip()
with open("1._Usuario/5._Sección.txt", "r", encoding="utf-8") as f:
    seccion_usuario = f.read().strip()
with open("0._Sistema/8._Metadescripción.txt", "r", encoding="utf-8") as f:
    metadescripcion_sistema = f.read().strip()
with open("1._Usuario/8._Metadescripción.txt", "r", encoding="utf-8") as f:
    metadescripcion_usuario = f.read().strip()
with open("0._Sistema/9._Categoría.txt", "r", encoding="utf-8") as f:
    categoria_sistema = f.read().strip()
with open("1._Usuario/9._Categoría.txt", "r", encoding="utf-8") as f:
    categoria_usuario = f.read().strip()

if len(titulos) < N_HILOS:
    N_HILOS = len(titulos)

clave_api_actual = 0

def chatGPT(sistema, usuario):
    global clave_api_actual
    respuesta = ""
    
    while True:
        try:
            clave_api = gpts[clave_api_actual]
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
            
            clave_api_actual = (clave_api_actual + 1) % len(gpts)
            
            return texto_respuesta
            
        except Exception as e:
            print(f"Excepción en chatGPT: {e}")  # Agregado: Imprimir detalles de la excepción
            pass

def obtener_portada(titulo):
    imagen = chatGPT(imagen_sistema, imagen_usuario.format(titulo=titulo))
    imagen = imagen.rstrip(".")
    if imagen.startswith('"') and imagen.endswith('"'):
        imagen = imagen[1:-1]
        imagen = imagen.rstrip(".")
    return imagen

def obtener_estructura(titulo):
    estructura = chatGPT(estructura_sistema, estructura_usuario.format(titulo=titulo))
    estructura = re.sub('\n+', '\n', estructura)
    estructura = estructura.replace("- ", "")
    estructura = estructura.split("\n")
    return estructura

def obtener_seccion(titulo, estructura):
    seccion = chatGPT(seccion_sistema, seccion_usuario.format(titulo=titulo, estructura=estructura))
    seccion = seccion.replace("# Introducción", "")
    seccion = seccion.replace("## Conclusiones", "")
    seccion = seccion.replace("## Conclusión", "")
    seccion = re.sub(r'En conclusión, (\w)', lambda match: match.group(1).upper(), seccion)
    seccion = re.sub(r'En resumen, (\w)', lambda match: match.group(1).upper(), seccion)
    seccion = seccion.replace("En conclusión, ", "")
    seccion = seccion.replace("En resumen, ", "")
    seccion = re.sub(r'<strong>En conclusión,</strong> (\w)', lambda match: match.group(1).upper(), seccion)
    seccion = re.sub(r'<strong>En resumen,</strong> (\w)', lambda match: match.group(1).upper(), seccion)
    seccion = seccion.replace("<strong>En conclusión,</strong> ", "")
    seccion = seccion.replace("<strong>En resumen,</strong> ", "")
    return seccion

def obtener_metadescripcion(titulo):
    metadescripcion = chatGPT(metadescripcion_sistema, metadescripcion_usuario.format(titulo=titulo))
    if metadescripcion.startswith('"') and metadescripcion.endswith('"'):
        metadescripcion = metadescripcion[1:-1]
    intentos = 0
    while len(metadescripcion) > 150 and intentos < 3:
        metadescripcion = chatGPT(metadescripcion_sistema, f"Haz más pequeña la metadescripción: \"{metadescripcion}\"")
        if metadescripcion.startswith('"') and metadescripcion.endswith('"'):
            metadescripcion = metadescripcion[1:-1]
        intentos += 1
    return metadescripcion

def obtener_categoria(titulo):
    categoria = chatGPT(categoria_sistema, categoria_usuario.format(titulo=titulo))
    categoria = categoria.rstrip(".")
    if categoria.startswith('"') and categoria.endswith('"'):
        categoria = categoria[1:-1]
        categoria = categoria.rstrip(".")
    return categoria


def guardar_resultado_en_markdown(titulo, resultado):
    archivo_md = f"resultados_markdown/{titulo}.md"
    with open(archivo_md, "w", encoding="utf-8") as md_file:
        md_file.write(resultado)

# Modificado: Eliminar la parte de escritura en archivo CSV

# Modificado: Función para procesar un título y guardar resultado en archivo Markdown
def procesar_titulo(titulo):
    keyword = titulo["Keyword"]
    titulo_articulo = titulo["Titulo"]
   
    portada = obtener_portada(titulo)
    
    estructura = obtener_estructura(titulo)
    
    articulo = obtener_seccion(titulo, estructura)

    metadescripcion = obtener_metadescripcion(titulo)
    
    categoria = obtener_categoria(titulo)
    
    # Resto del código para obtener datos generados
    
    resultado_md = f"""
    ---
    title: '{titulo_articulo}'
    date: '2019-10-11'
    tags: ['{categoria}']
    draft: false
    summary: '{metadescripcion}'
    ---
    {articulo}
    """
    # resultado_html = markdown.markdown(resultado_md)
    
    guardar_resultado_en_markdown(titulo_articulo, resultado_md)

    # Crear directorio para archivos Markdown si no existe
    ruta_resultados = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resultados_markdown")

    if not os.path.exists(ruta_resultados):
        os.makedirs(ruta_resultados)

with concurrent.futures.ThreadPoolExecutor(max_workers=N_HILOS) as ejecutor:
    futuros = [ejecutor.submit(procesar_titulo, titulo) for titulo in titulos]
    for futuro in concurrent.futures.as_completed(futuros):
        resultado = futuro.result()