# Proyecto Final

### Introducción

Para este proyecto final se agregaron las nuevas funcionalidades de envio de notificaciones por email, se enviará una notificacion cuando:

- Alguna tarea este reintentando su ejecución.
- Si el DAG falló.

### Requesitos

Para que se pueda ejecutar el **docker-compose.yaml** es necesario el archivo .env.

Para levantar airflow debemos agregar el user id ejecutando el siguiente comando:

- `echo -e "AIRFLOW_UID=$(id -u)" >> ./.env`

Para poder correr el DAG es necesario las variables de entorno. Si se quiere probar debemos de agregar las siguientes variables al .env.

#### Variables Redshift

```
REDSHIFT_USERNAME=
REDSHIFT_PASSWORD=
REDSHIFT_HOST=
REDSHIFT_PORT=
REDSHIFT_DBNAME=
REDSHIFT_SCHEMA=
```

#### Variables Airflow - SMTP

```
# Airflow
AIRFLOW_VAR_SUBJECT_MAIL=DAG was executed successfully  # cambiar subject opcional
AIRFLOW_VAR_EMAIL=
AIRFLOW_VAR_EMAIL_PASSWORD=
AIRFLOW_VAR_TO_ADDRESS=
# SMTP
AIRFLOW__SMTP__SMTP_HOST=
AIRFLOW__SMTP__SMTP_PORT=
AIRFLOW__SMTP__SMTP_USER=
AIRFLOW__SMTP__SMTP_PASSWORD=
AIRFLOW__SMTP__SMTP_MAIL_FROM=
```
