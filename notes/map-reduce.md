# Small reminder of hadoop commands

### Connect

```
ssh mp71722388@hadoop.ugr.es
```

### Remove previous data

```
> MyMapReduce.java
rm -rf aux_dir
hdfs dfs -rm -r /user/mp71722388/mapreduce_results
nano MyMapReduce.java (Alt+Space, e, p == Paste)
```

### Compile

```
mkdir aux_dir
javac -classpath `yarn classpath` -d aux_dir MyMapReduce.java
jar -cvf MyMapReduce.jar -C aux_dir / .
```

### A glance at the dataset

```
hdfs dfs -cat /user/mp2019/ECBDL14_10tst.data | head
```

### Execute

#### Test

```
hadoop jar MyMapReduce.jar MyMapReduce /user/mp2019/mp71722388/prueba.txt /user/mp71722388/mapreduce_results
```

#### Final dataset

```
hadoop jar MyMapReduce.jar MyMapReduce /user/mp2019/ECBDL14_10tst.data /user/mp71722388/mapreduce_results
```

### Display results

```
hdfs dfs -cat /user/mp71722388/mapreduce_results/part-00000
```
