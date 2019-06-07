# Small reminder of hadoop commands

### Connect

```
ssh mp71722388@hadoop.ugr.es
```

### Remove previous data

```
> DevSTD.java
rm -rf aux_dir
hdfs dfs -rm -r /user/mp71722388/mapreduce_results
nano DevSTD.java (Alt+Space, e, p == Paste)
```

### Compile

```
mkdir aux_dir
javac -classpath `yarn classpath` -d aux_dir DevSTD.java
jar -cvf DevSTD.jar -C aux_dir / .
```

### A glance at the dataset

```
hdfs dfs -cat /user/mp2019/ECBDL14_10tst.data | head
```

### Execute

#### Test

```
hadoop jar DevSTD.jar DevSTD /user/mp2019/mp71722388/prueba.txt /user/mp71722388/mapreduce_results
```

#### Final dataset

```
hadoop jar DevSTD.jar DevSTD /user/mp2019/ECBDL14_10tst.data /user/mp71722388/mapreduce_results
```

### Display results

```
hdfs dfs -cat /user/mp71722388/mapreduce_results/part-00000
```
