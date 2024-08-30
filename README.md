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
