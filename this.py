import logging
import argparse
import pyodbc
from apache_beam import CombineGlobally, Create, ParDo, Pipeline
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


def process_element(element,db_host,db_name,db_user,db_password):
    # Aquí puedes realizar la lógica de procesamiento para conectarte a la base de datos SQL
    # y realizar las operaciones necesarias con los datos

    # Ejemplo de conexión a una base de datos SQL mediante pyodbc
    connection_string = f"Driver={{SQL Server}};Server={db_host};Database={db_name};User={db_user};Password={db_password};"
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()

    # Ejemplo de consulta a la base de datos
    cursor.execute("SELECT * FROM LMS_ACTVIDAD")
    results = cursor.fetchall()

    # Ejemplo de procesamiento de los datos obtenidos de la consulta
    total = 0
    for row in results:
        total += row[0]

    # Ejemplo de impresión de los resultados
    logging.info('Sum from SQL query is %s', total)

    # Aquí puedes realizar cualquier otra operación o lógica de procesamiento necesaria

    # Cierre de la conexión a la base de datos
    cursor.close()
    connection.close()

    return total


def main():
    # Configuración de los argumentos de línea de comandos utilizando argparse
    parser = argparse.ArgumentParser(description='Process data from SQL')
    parser.add_argument('--runner', dest='runner', required=True)
    parser.add_argument('--project', dest='project', required=True)
    parser.add_argument('--region', dest='region', required=True)
    parser.add_argument('--job_name', dest='job_name', required=True)
    parser.add_argument('--staging_location', dest='staging_location', required=True)
    parser.add_argument('--temp_location', dest='temp_location', required=True)
    
    parser.add_argument('--db_host', required=True, help='Database host')
    parser.add_argument('--db_name', required=True, help='Database name')
    parser.add_argument('--db_user', required=True, help='Database username')
    parser.add_argument('--db_password', required=True, help='Database password')
    parser.add_argument('--setup_file', required=True, help='setup file')
    args = parser.parse_args()

    # Configuración de las opciones del pipeline de Apache Beam
    options = PipelineOptions()
    options.view_as(SetupOptions).save_main_session = True

    # Creación del pipeline
    p = Pipeline(options=options)

    # Creación de la fuente de datos utilizando el rango especificado
    start = 1
    end = 2
    source = p | 'From {} to {}'.format(start, end) >> Create(list(range(start, end + 1)))

    processed_data = source | 'Process' >> ParDo(process_element, args.db_host, args.db_name, args.db_user, args.db_password)
    total_sum = processed_data | 'Sum' >> CombineGlobally(sum)

    # Ejecución del pipeline
    p.run() 


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
