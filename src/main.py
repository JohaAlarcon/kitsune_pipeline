import requests
from bs4 import BeautifulSoup, Tag
import pandas as pd
from datetime import datetime
import time
import logging

class CongresoChileScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }

    def setup_logging(self):
        logging.basicConfig(
            filename= f'scraper_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def obtener_datos_senado(self, fecha_inicio='01/01/2023', fecha_fin='31/01/2023'):
        """Obtiene los datos de la pagina del senado"""
        logging.info('Obteniendo datos del senado...')
        proyectos = []
        base_url = 'https://tramitacion.senado.cl/appsenado/index.php'
        params = {
            'mo': 'tramitacion',
            'ac': 'busquedaavanzada',
            'cadena':f'0~0~0~0~{fecha_inicio}~{fecha_fin}~~~0~0~~~~~',
            'etc': str(int(time.time() * 1000))
        }

        try:
            response = requests.get(base_url, headers=self.headers, params=params)
            response.raise_for_status()

            print(F"obteniendo datos de ley para el rango de fechas {fecha_inicio} - {fecha_fin}")

            soup = BeautifulSoup(response.text, 'html.parser')

            #buscar la tabla de proyectos
            tabla_proyectos = soup.find('table', {'id':'grid_nivel2'})

            if tabla_proyectos and isinstance(tabla_proyectos, Tag):
                #obtener encabezados
                encabezados =[]
                filas= tabla_proyectos.find_all('tr')

                #procesar cada fila de proyectos
                for idFila, fila in enumerate(filas):
                    datos_fila={}
                    columnas = fila.find_all('td')

                    if idFila == 0:
                        #Extraer encabezados
                        encabezados.append(columna.text.strip())
                    else:
                        for idColumna, columna in enumerate(columnas):
                            #extraer datos si existen
                            if encabezados[idColumna] != 'Fecha_sort' and encabezados[idColumna] != 'bol oculto':
                                if encabezados[idColumna] == 'Estado':
                                    datos_fila['Tipo'] = columna.text.strip()
                                elif encabezados[idColumna] == 'N° Boletín':
                                     datos_fila['Número'] = columna.text.strip()
                                else:
                                    datos_fila[encabezados[idColumna]] = columna.text.strip()
                        datos_fila['Organismo'] = 'Senado'
                        proyectos.append(datos_fila)

            return pd.DataFrame(proyectos)

        except Exception as e:
                logging.error(f"Error al obtener datos del senado: {e}")
                return pd.DataFrame(proyectos)

    def obtener_datos_bnc(self, fecha_inicio='01/01/2023', fecha_fin='31/01/2023',
    items_por_pagina=1000):

        """obtiene los datos de la pagina de la BNC usando la API con manejo de paginacion"""
        logging.info ('Obteniendo datos de la BNC...')

        url = 'https://nuevo.leychile.cl/servicios/Consulta/listaresultadosavanzada'
        base_params = {
            'stringBusqueda': f'-1#normal#on||4#normal#{fecha_inicio}#{fecha_fin}||44#normal#{fecha_inicio}#{fecha_fin}||117#normal#on||48#normal#on',
            'tipoNormaBA': '',
            'itemsporpagina': items_por_pagina,
            'orden': 2,
            'tipoviene': 4,
            'seleccionado': 0,
            'taxonomia': '',
            'valor_taxonomia': '',
            'o': 'experta',
            'r': ''
        }

        proyectos = []
        pagina_actual = 1
        total_items = None

        try:
            while True:
                #actualizar numero de pagina en los parametros
                params= base_params.copy()
                params['npagina'] = pagina_actual

                #Realizar la petición
                response = requests.get(url,params=params, headers=self.headers)
                response.raise_for_status()
                data = response.json()

                #verficar que tenemos datos validos
                if not data or len(data) < 2:
                    logging.error('Formato de respuesta inesperado de la API de la BNC')
                    break

                #extraer los items de la primera posicion
                items = data[0]

                #Extraer informacion de paginacion de la segunda posicion
                info_paginacion = data[1]
                total_items = info_paginacion.get('totalitems',0)

                #porcesar los items de la paginacion actual
                for item in items:
                    proyecto = {
                        'Número': item.get('IDNORMA', ''),
                        'Título': item.get('TITULO_NORMA', ''),
                        'Fecha': item.get('FECHA_PUBLICACION', ''),
                        'Tipo': item.get('TIPO', ''),
                        'Organismo': item.get('ORGANISMO', 'No especificado')
                    }
                    proyectos.append(proyecto)

                #Registrar progreso
                items_obtenidos = len(proyectos)
                logging.info(f'procesada pagina {pagina_actual} items obtenidos hasta ahora {items_obtenidos} / {total_items}')
                print(f'Procesando datos de BNC: {items_obtenidos} / {total_items}')

                #verificar si hay mas paginas
                if items_obtenidos >= total_items:
                    break

                #preparar para la siguiente pagina
                pagina_actual += 1

                #pausa para no sobrecargar el servidor
                time.sleep(0.5)

            logging.info(f'Extracion de BNC completada. Total de items obtenidos: {len(proyectos)}')
            return pd.DataFrame(proyectos)

        except Exception as e:
            logging.error(f'Error al obtener datos de la BNC: {e}')
            #Si tenemos proyectos, los devolvemos apesar del error
            if proyectos:
                logging.warning(f'devolviendo {len(proyectos)} proyectos obtenidos antes del error')
                return pd.DataFrame(proyectos)
            return pd.DataFrame()



def main():
    # crear una instancia de scraper
    scraper = CongresoChileScraper()
    print('Iniciando extraxxion de datos...')

    # Obtener fuentes datos de la fuente del senado
    df_senado = DataFrame = scraper.obtener_datos_senado()
    print(f"Datos obtenidos del senado: {len(df_senado)} registros ")

    #obtener datos de la BNC
    df_bnc = scraper.obtener_datos_bnc()
    print(f"Datos obtenidos de la BNC: {len(df_bnc)} registros ")

if __name__ == "__main__":
    main()
