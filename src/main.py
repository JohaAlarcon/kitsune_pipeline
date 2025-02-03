import os
import requests
import pandas as pd
import time
import logging
import sqlalchemy as  sa
from openai import OpenAI
from sqlalchemy_cratedb.support import insert_bulk
from bs4 import BeautifulSoup, Tag
from datetime import datetime
from unidecode import unidecode

# Normalize text: remove accents and convert to lowercase
def normalize_text(text: str) -> str:
    """Normalize text: remove accents and convert to lowercase"""
    return unidecode(text).lower().strip()

# Convert date to yyyy-mm-dd format
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

# Initilize OpenAI API to generate categories
client = OpenAI()
def generate_category(title) :
    """Generate a category  based on the content of the title using the OpenAI API"""
    logging.info(f"Generating category for: {title}")
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "Eres un experto en categorizar temas a partir de un texto, por ejemplo, un texto que habla sobre petroleo y sus derivados, sera categorizado en hidrocarburos, devuelve la categoria en español y la menor cantidad de palabras posibles"
                },
                {
                    "role": "user",
                    "content": title
                }
            ]
        )
        category = completion.choices[0].message.content
        logging.info(f"Generated category: {category}")

        # Return None if category is empty, else replace spaces with underscores
        return "_".join(category.split()) if category else None

    except Exception as e:
        logging.error(f"Error generating category: {str(e)}")
        return 'No category'

# Main scraper class
class ChileCongressScraper:
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

    def get_senado_data(self, fecha_inicio='01/01/2023', fecha_fin='31/12/2023'):
        """Get data from the Senate website"""
        logging.info('Getting Senate data...')
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

            # Find the projects table
            tabla_proyectos = soup.find('table', {'id':'grid_nivel2'})

            if tabla_proyectos and isinstance(tabla_proyectos, Tag):
                # Get headers
                encabezados =[]
                filas= tabla_proyectos.find_all('tr')

                # Process each project row
                for idFila, fila in enumerate(filas):
                    datos_fila={}
                    columnas = fila.find_all('td')

                    if idFila == 0:
                        # Extract headers
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
                logging.error(f"Error getting Senate data: {e}")
                return pd.DataFrame(proyectos)

    def get_bnc_data(self, fecha_inicio='2023-01-01', fecha_fin='2023-12-31',
    items_por_pagina=1000):

        """Get data from the BNC website using the API with pagination"""
        logging.info ('Getting BNC data...')

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
                # Update page number in parameters
                params= base_params.copy()
                params['npagina'] = pagina_actual

                # Make the request
                response = requests.get(url,params=params, headers=self.headers)
                response.raise_for_status()
                data = response.json()

                # Verify that we have valid data
                if not data or len(data) < 2:
                    logging.error('Unexpected response format from the BNC API')
                    break

                # Extract items from the first position
                items = data[0]

                # Extract pagination info from the second position
                info_paginacion = data[1]
                total_items = info_paginacion.get('totalitems',0)

                # Process items on the current page
                for item in items:
                    proyecto = {
                        'Número': item.get('IDNORMA', ''),
                        'Título': item.get('TITULO_NORMA', ''),
                        'Fecha': item.get('FECHA_PUBLICACION', ''),
                        'Tipo': item.get('TIPO', ''),
                        'Organismo': item.get('ORGANISMO', 'No especificado')
                    }
                    proyectos.append(proyecto)

                # Log progress
                items_obtenidos = len(proyectos)
                logging.info(f'Processed page {pagina_actual} items obtained so far {items_obtenidos} / {total_items}')
                print(f'Processing BNC data: {items_obtenidos} / {total_items}')

                # Check if there are more pages
                if items_obtenidos >= total_items:
                    break

                # Prepare for the next page
                pagina_actual += 1

                # Pause to avoid overloading the server
                time.sleep(0.5)

            logging.info(f'BNC extraction completed. Total items obtained: {len(proyectos)}')
            return pd.DataFrame(proyectos)

        except Exception as e:
            logging.error(f'Error getting BNC data: {e}')
            # If we have projects, return them despite the error
            if proyectos:
                logging.warning(f'Returning {len(proyectos)} projects obtained before the error')
                return pd.DataFrame(proyectos)
            return pd.DataFrame()

    def data_combine(self, df_senado, df_bnc):
        """Combine data from both sources into a single DataFrame"""
        logging.info('Combining data from both sources...')
        # Combine DataFrames
        df_final = pd.DataFrame()

        if not df_senado.empty:
            df_final = df_senado

        if not df_bnc.empty:
            if df_final.empty:
                df_final = df_bnc
            else:
                df_final = pd.concat([df_final, df_bnc], axis=0, ignore_index=True)

        return df_final

    def clean_standardize_data(self, df_final: pd.DataFrame) -> pd.DataFrame:
        try:
            df_clean = df_final.copy()

            # Standardize column names (lowercase and without accents)
            df_clean.columns = [normalize_text(col) for col in df_clean.columns]
            logging.info("Column names normalized")

            # Standardize date format
            if 'fecha' in df_clean.columns:
                df_clean['fecha'] = df_clean['fecha'].apply(convert_date)
                logging.info("Dates standardized to yyyy-mm-dd format")

            # Convert specified columns to lowercase
            for col in ['titulo', 'organismo', 'tipo']:
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].str.lower()

            # Add quotes to the 'titulo' field
            if 'titulo' in df_clean.columns:
                df_clean['titulo'] = df_clean['titulo'].apply(
                    lambda x: f'"{x}"' if not pd.isna(x) else x
                )
                logging.info("Titles processed with quotes")

            # Generate a category based on the title
            if 'titulo' in df_clean.columns:
                df_clean['categoria'] = df_clean['titulo'].apply(generate_category)
                logging.info("Categories generated based on titles")

            return df_clean

        except Exception as e:
            logging.error(f"Error cleaning data: {str(e)}")
            raise

    def save_data(self, df, nombre_base):
        """Save data in different formats inside the 'files' directory"""
        # Create 'files' directory if it doesn't exist
        files_dir = 'files'
        os.makedirs(files_dir, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        nombre_archivo = str(f'{nombre_base}_{timestamp}')

        # Build full paths
        ruta_base = os.path.join(files_dir, nombre_archivo)

        try:
            # Save as CSV
            df.to_csv(f'{ruta_base}.csv', index=False, encoding='utf-8-sig')
            logging.info(f"Data saved as CSV: {ruta_base}.csv")

            # Save as JSON
            df.to_json(f'{ruta_base}.json', orient='records', force_ascii=False)
            logging.info(f"Data saved as JSON: {ruta_base}.json")

            # Load data into CrateDB
            dburi = "crate://admin:<password>@kitsune-test.aks1.westeurope.azure.cratedb.net:4200?ssl=true"
            engine = sa.create_engine(dburi, echo=True)
            CHUNKSIZE = 1000
            df.to_sql(name='crate', con=engine, if_exists='replace', index=False, chunksize=CHUNKSIZE, method=insert_bulk)
            logging.info(f"Data loaded into CrateDB")

        except Exception as e:
            logging.error(f"Error saving files: {str(e)}")
            raise

        # Print basic statistics
        print("\nData statistics:")
        print(f"Total records: {len(df)}")
        print("\nDistribution by organism:")
        print(df['organismo'].value_counts())

def main():
    # Create an instance of the scraper
    scraper = ChileCongressScraper()
    print('Starting data extraction...')

    # Get data from the Senate source
    df_senado = scraper.get_senado_data()
    print(f"Data obtained from the Senate: {len(df_senado)} records")

    # Get data from the BNC
    df_bnc = scraper.get_bnc_data()
    print(f"Data obtained from the BNC: {len(df_bnc)} records")

    # Combine data into a single DataFrame
    df_combined = scraper.data_combine(df_senado, df_bnc)
    print(f"Combined data: {len(df_combined)} records")

    # Clean and standardize the data
    df_cleaned = scraper.clean_standardize_data(df_combined)
    print(f"Cleaned and standardized data: {len(df_cleaned)} records")

    # Save the data
    scraper.save_data(df_cleaned, 'chile_legislation_data')
    print('Process complete, check the generated files')

if __name__ == "__main__":
    main()
