import io
import requests
import pandas as pd
from sqlalchemy import create_engine, text


def actualizar_datos_1():
    print("Inicio del proceso de actualización de datos.")

    # Variables de conexión a MariaDB
    conexion_url = "mysql+mysqlconnector://root:VuSkVBlHCGGnYTpbFdKKMiqrBVrXFjRN@roundhouse.proxy.rlwy.net:18142/railway"

    # URL del archivo en Google Drive
    url = 'https://drive.google.com/uc?export=download&id=1d1xCExiZWinOcoQPgcqIhxzDaqIAN1VG'

    try:
        # Realizar la solicitud GET para obtener el archivo
        print("Descargando archivo desde Google Drive...")
        response = requests.get(url)
        response.raise_for_status()  # Asegura que la solicitud fue exitosa
        print("Archivo descargado correctamente.")

        # Leer el archivo Excel desde la respuesta en memoria
        excel_file = io.BytesIO(response.content)
        df = pd.read_excel(excel_file)

        # Eliminar la primera fila y seleccionar las columnas 1, 2, 5, 11 y 16
        df = df.drop(index=0).iloc[:, [0, 1, 4, 10, 15]]  # Corregido: 0, 1, 4, 10, 15
        print("Datos leídos y columnas seleccionadas.")

        # Cambiar los nombres de las columnas para que coincidan con los de la base de datos
        df.columns = ['N', 'CLIENTE', 'EXPEDIENTE_SOLICITUD', 'SEGUIMIENTO', 'ESTADO_FINAL']  # Corregido: EXPEDIENTE_SOLICITUD
        print("Datos procesados y preparados para comparación.")

        # Conexión a MariaDB
        engine = create_engine(conexion_url)
        print("Conectando a la base de datos...")

        # Leer los datos actuales de la tabla 'resumen_clientes' en la base de datos
        with engine.connect() as conn:
            db_data = pd.read_sql('SELECT * FROM resumen_clientes', conn)

        # Comparar los datos para verificar si hay diferencias
        print("Comparando datos...")
        if not df.equals(db_data):
            print("Se encontraron diferencias. Actualizando la base de datos...")
            # Borrar los datos actuales en la tabla 'resumen_clientes'
            with engine.begin() as conn:
                conn.execute(text("TRUNCATE TABLE resumen_clientes"))

            # Insertar el DataFrame en la tabla 'resumen_clientes' de MariaDB
            df.to_sql('resumen_clientes', con=engine, if_exists='append', index=False)
            print("Datos actualizados en la base de datos.")
        else:
            print("No se encontraron cambios. La base de datos ya está actualizada.")
    except Exception as e:
        print("Error durante la ejecución:", e)


if __name__ == "__main__":
    actualizar_datos_1()
