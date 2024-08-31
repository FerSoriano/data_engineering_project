# Preentrega No. 3

### Introducción

El objetivo de esta preentrega era dockerizar el codigo de la preentrega 2 y crear un DAG en Airflow. Uno de los principales retos fue el de modularizar mi codigo, tuve algunos problemas con las rutas relativas. Tambien tuve que crear nuevos archivos para poder manejar mejor mi codigo.

### Estructura

- Descargué el docker-compose de Airflow 2.10
- Creé un Dockerfile para manejar los requerimientos de mi codigo de Python.
- En mi docker-compose.yaml agregué un nuevo servicio para python.
- En el archivo **dag_etl.py** creé mi DAG con 3 tareas:
  - 2 de ellas con el operador Python:
    - Tarea 1 - Crear los objetos de SQL
    - Tarea 2 - Ejecutar el proceso de ETL.
  - 1 con el operador de Bash solo para mostrar un mensaje de que el proceso finalizó.

### Requesitos

Para que se pueda ejecutar el **docker-compose.yaml** es necesario el archivo .env.

Para levantar airflow debemos agregar el user id ejecutando el siguiente comando:

- `echo -e "AIRFLOW_UID=$(id -u)" >> ./.env`

Para poder correr el DAG es necesario las variables de entorno. Si se quiere probar debemos de agregar las siguientes variables al .env:

```
REDSHIFT_USERNAME=
REDSHIFT_PASSWORD=
REDSHIFT_HOST=
REDSHIFT_PORT=
REDSHIFT_DBNAME=
REDSHIFT_SCHEMA=
```
