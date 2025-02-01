import requests
from bs4 import BeautifulSoup, Tag
import pandas as pd
from datetime import datetime
import time
import logging

class CongresoChileScraper:
    def __init__(self):
        self.url = 'https://www.senado.cl/appsenado/templates/tramitacion/index.php'
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
            response = requests.get(self.url, headers=self.headers, params=params)
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

def main():
    # crear una instancia de scraper
    scraper = CongresoChileScraper()
    print('Iniciando extraxxion de datos...')

    # Obtener fuentes datos de la fuente del senado
    df_senado = DataFrame = scraper.obtener_datos_senado()
    print(f"Datos obtenidos del senado: {len(df_senado)} registros ")

if __name__ == "__main__":
    main()
