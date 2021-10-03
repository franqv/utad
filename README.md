# CREACIÓN DE CUBOS OLAP CON APACHE KAFKA Y APACHE SPARK
Este código forma parte del Proyecto Fin de Master en la UTAD.

## Flujo de datos del proyecto

![image](https://user-images.githubusercontent.com/89654447/135769905-3bf4e172-3aea-42dd-9ec6-9ccd6e6c3e49.png)

Este proyecto utiliza como dataset el registro de viajes de taxis de Nueva York. 
Los datos se pueden descargar por meses en la siguiente web:
https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

En el siguiente enlace se encuentra el diccionario de las variables del dataset.
* https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

Para poder usar datos de varios meses es necesario juntar los csv en uno único y meter la ruta correspondiente en el producer.

El objetivo es:
1.	A partir del csv generar un steam de datos con Kafka.
2.	Recibir esos datos en spark .
3.	Almacenar los datos en bruto en HDFS.
4.	Procesar los datos y almacenarlos en Cassandra formando el cubo de OLAP deseado.
5.	Comparación de tiempos entre procesado en streaming y batch.

Una vez tenemos los datos, hay que ejecutar el Docker-compose desde la terminal en modo root para levantar los Docker que se han preparado. 
* docker-compose up -d

Una vez levantado el Docker, nos metemos en Cassandra con el siguiente comando.
* docker-compose exec cassandra bash

A continuación entramos en la terminal cqlsh con el usuario y contraseña cassandra.
- cd bin
- cqlsh -u cassandra -p cassandra

Una vez dentro de la terminal de cassandra, tenemos que crear tanto un keyspace, como la tabla en la que cargará los datos el consumer.
- CREATE KEYSPACE IF NOT EXISTS tfm WITH REPLICATION ={ 'class':'SimpleStrategy','replication_factor':'1'};
- USE tfm;
- CREATE TABLE IF NOT EXISTS tfm.olap_cube (dia_hora text PRIMARY KEY, travel_time DOUBLE, avg_passengers DOUBLE, avg_trip_distance DOUBLE, type_payments text, avg_total_amount * - DOUBLE, n_travel Int) ;

Es interesante destacar que la clave primaria es la variable ‘dia_hora’, variable por la cual se va a particionar la tabla. 
