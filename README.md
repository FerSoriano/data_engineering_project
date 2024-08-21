# Preentrega No. 2

### Introducción

Para esta nueva entrega se hicieron varios cambios en la arquitectura del desarrollo. Originalmente estaba dividido en un archivo **main.py** y dos carpetas carpetas: **db** y **etl**.

- **db**:
  - connect.py: en este archivo habia una clase que administraba la conexion a la base de datos, creaba las tablas, las truncaba e insertaba registros. Todo en una misma clase. Esto complicaba al mantenimiento y la escalabilidad.
- **etl**:
  - olympics_medals.py: este archivo almacena una clase llamada **Medals()** donde está almacenada la lógica para la extracción de los datos.

### Nueva estructura

Decidi separar la clase **DatabaseConection()** y seguir buenas practicas de la programacion orientada a objetos. Ahora dentro de la carpeta **db** tengo 4 archivos:

- **conection.py**
  - Aqui quedo la clase que manejará los métodos para hacer la conexión y cierre de la base de datos.
- **create_tables.py**
  - En este modulo solo existe una clase que contiene metodos para crear las diferentes tablas / vistas
- **dml.py**
  - Este módulo se creó para manejar todo el DML (_Data Manipulation Language_), se crearon metodos para insertar, actualizar y eliminar registros.
- **database.py**
  - Este modulo hereda todos los metodos de las clases previas. Es el que se usará en el main.

Por otro lado, se agregó un nuevo archivo a la modulo ETL: **country_codes.py**

- En este nuevo archivo se extrae la informacion de los paises junto con sus codigos mundiales. Esto ayudará a poder sacar analisis por region en un futuro.

### Validaciones

- Creé nuevas validaciones para evitar la duplicidad de los datos. Dentro de mis consultas primero se valida que el registro no exista antes de insertarlo. Tambien tiene nuevas banderas de fechas que validan antes de correr el proceso de ETL.
