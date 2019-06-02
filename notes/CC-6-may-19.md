# Hadoop

Empezamos la clase con el manu
https://github.com/manuparra
https://github.com/manuparra/MasterDatCom_BDCC_Practice

## conectarse a la maquina hadoop de la ugr

user mp<DNI>
pass: XX<DNI>

ssh mp71722388@hadoop.ugr.es

la he actualziado con `passwd` a la de siempre

luego ejecutamos `bash` pa entrar a la terminal wena


`hdfs dfs -ls /user/mp2019` pa ver todos los usuarios

----

creamos un fichero local

`echo "hello hdfs" > fichero.txt`

lo subimos
`hdfs dfs -put fichero.txt  /user/mp2019/mp71722388`

creamos una carpeta
`hdfs dfs -mkdir /user/mp2019/mp71722388/test`

movemos el archivo

``hdfs dfs -mv /user/mp2019/mp71722388/fichero.txt /user/mp2019/mp71722388/test/fichero.txt``

``hdfs dfs -cat /user/mp2019/mp71722388/test/fichero.txt``

> se puede evitar poner toa esa ruta con un alias

## contar palabras

nos bajamos el ficher de texto del tio (es un lorem ipsum larguete)
wget http://tiny.cc/b8795y
y lo guardamos en lorem.txt

ahora lo movemos al distribuido

`hdfs dfs -put lorem.txt /user/mp2019/mp71722388/lorem.txt`

ahora para contar las palabras con map reduce, como el archivo está distribuido en varios nodos (15 creo) se usa cada nodo para contar una parte del archivo y luego se suma todo para obtener el total

**y aqui termina la clase**
