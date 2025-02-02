import os
import requests
from bs4 import BeautifulSoup, Tag
import pandas as pd
from datetime import datetime
import time
import logging
from unidecode import unidecode

class CongresoChileScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            filename= f'scraper_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def obtener_datos_senado(self, fecha_inicio='01/01/2023', fecha_fin='31/12/2023'):
        """Obtiene los datos de la pagina del senado"""
        logging.info('Obteniendo datos del senado...')
        proyectos = []
        base_url = 'https://tramitacion.senado.cl/appsenado/index.php'
        params = {
            'mo': 'tramitacion',
            'ac': 'avanzada_resultado',
            'cadena':f'0~0~0~0~{fecha_inicio}~{fecha_fin}~~~0~0~~~~~',
            'etc': str(int(time.time() * 1000))
        }

        try:
            response = requests.get(base_url, headers=self.headers, params=params)
            response.raise_for_status()

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
                        for columna in columnas:
                            encabezados.append(columna.text.strip())
                    else:
                        for idColumna, columna in enumerate(columnas):
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

    def obtener_datos_bnc(self, fecha_inicio='2023-01-01', fecha_fin='2023-01-10',
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


    def combinar_datos(self, df_senado, df_bnc):
        """Combina los datos de ambas fuentes en un solo DataFrame"""
        logging.info('Combinando datos de ambas fuentes...')
        #combinar los DataFrames
        df_final = pd.DataFrame()

        if not df_senado.empty:
            df_final = df_senado

        if not df_bnc.empty:
            if df_final.empty:
                df_final = df_bnc
            else:
                df_final = pd.concat([df_final, df_bnc], axis=0, ignore_index=True)

        return df_final

    def limpiar_estandarizar_datos(self, df_final: pd.DataFrame) -> pd.DataFrame:
        try:
            df_clean = df_final.copy()

            # Estandariza nombres de columnas (minúsculas y sin acentos)
            df_clean.columns = [normalize_text(col) for col in df_clean.columns]
            logging.info("Nombres de columnas normalizados")

            if 'fecha' in df_clean.columns:
                def convert_date(date_str):
                    try:
                        months_esp = {
                            'ENE': '01', 'FEB': '02', 'MAR': '03', 'ABR': '04',
                            'MAY': '05', 'JUN': '06', 'JUL': '07', 'AGO': '08',
                            'SEP': '09', 'OCT': '10', 'NOV': '11', 'DIC': '12'
                        }

                        if isinstance(date_str, str) and '-' in date_str and any(month in date_str.upper() for month in months_esp):
                            day, month, year = date_str.upper().split('-')
                            month = months_esp[month]
                            return f"{year}-{month}-{day.zfill(2)}"

                        return pd.to_datetime(date_str, dayfirst=True).strftime('%Y-%m-%d')

                    except Exception as e:
                        logging.warning(f"Error processing date {date_str}: {str(e)}")
                        return date_str

                df_clean['fecha'] = df_clean['fecha'].apply(convert_date)
                logging.info("Fechas estandarizadas al formato yyyy-mm-dd")

                # Convert specified columns to lowercase
                for col in ['titulo', 'organismo', 'tipo']:
                    if col in df_clean.columns:
                        df_clean[col] = df_clean[col].str.lower()

                # Add quotes to the 'titulo' field
                if 'titulo' in df_clean.columns:
                    df_clean['titulo'] = df_clean['titulo'].apply(
                        lambda x: f'"{x}"' if not pd.isna(x) else x
                    )
                    logging.info("Títulos procesados con comillas")
            return df_clean

        except Exception as e:
            logging.error(f"Error en limpieza de datos: {str(e)}")
            raise

    def guardar_datos(self, df, nombre_base):
        """guarda los datos en diferentes formatos dentro del directorio files"""
        # Crear directorio 'files' si no existe
        files_dir = 'files'
        os.makedirs(files_dir, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        nombre_archivo = str(f'{nombre_base}_{timestamp}')

        # Construir rutas completas
        ruta_base = os.path.join(files_dir, nombre_archivo)

        try:
            # Guardar en CSV
            df.to_csv(f'{ruta_base}.csv', index=False, encoding='utf-8-sig')
            logging.info(f"Datos guardados en CSV: {ruta_base}.csv")

            # Guardar en Excel
            # df.to_excel(f'{ruta_base}.xlsx', index=False)
            # logging.info(f"Datos guardados en Excel: {ruta_base}.xlsx")

            # Guardar en JSON
            df.to_json(f'{ruta_base}.json', orient='records', force_ascii=False)
            logging.info(f"Datos guardados en JSON: {ruta_base}.json")

        except Exception as e:
            logging.error(f"Error al guardar archivos: {str(e)}")
            raise

        # Imprimir estadísticas básicas
        print("\nEstadísticas de los datos:")
        print(f"Total de registros: {len(df)}")
        print("\nDistribución por organismo:")
        print(df['organismo'].value_counts())

def normalize_text(text: str) -> str:
    """Normaliza texto: remueve acentos y convierte a minúsculas"""
    return unidecode(text).lower().strip()

def main():
    # crear una instancia de scraper
    scraper = CongresoChileScraper()
    print('Iniciando extraccion de datos...')

    # Obtener fuentes datos de la fuente del senado
    df_senado = DataFrame = scraper.obtener_datos_senado()
    print(f"Datos obtenidos del senado: {len(df_senado)} registros ")

    #obtener datos de la BNC
    df_bnc = scraper.obtener_datos_bnc()
    print(f"Datos obtenidos de la BNC: {len(df_bnc)} registros ")

    #combinar los datos en un solo DataFrame
    df_final = scraper.combinar_datos(df_senado, df_bnc)
    print(f"Datos combinados: {len(df_final)} registros")

    #limpiar y estandarizar los datos
    df_final = scraper.limpiar_estandarizar_datos(df_final)
    print(f"Datos limpios y estandarizados: {len(df_final)} registros")

    #Guardar los datos
    scraper.guardar_datos(df_final, 'datos_legislacion_chile')
    print('Proceso completo, revisa los rchivos generados')


if __name__ == "__main__":
    main()
