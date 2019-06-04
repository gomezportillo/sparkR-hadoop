# PRINCIPALES REFERENCIAS
# https://github.com/manuparra/taller_SparkR/blob/master/Parte%202.%20S06.%20Mineria%20de%20datos%20y%20Machine%20Learning%20con%20sparklyr.ipynb
# https://github.com/manuparra/TallerH2S#create-the-spark-environment

# install.packages('sparklyr')
# install.packages('dplyr')
# install.packages('ROSE')

if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "/opt/spark-2.2.0/")
}

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "1g"), enableHiveSupport=FALSE)

# sparkR.version()
# spark_install(version = "2.2.0")

library(sparklyr)
library(dplyr)
library(ROSE)

sc <- spark_connect(master = "local", version = "2.2.0")

# FILE = "5000_ECBDL14_10tst.data"
# FILE = "20000_ECBDL14_10tst.data"
# FILE = "500000_ECBDL14_10tst.data"
FILE = "ECBDL14_10tst.data"
FILEPATH = paste("hdfs://hadoop-master/user/mp2019/", FILE, sep="")
tttm <- spark_read_csv(sc, name="tttm", path=FILEPATH, delimiter = ",", header=TRUE, overwrite = TRUE)
# df5000 <- read.df("hdfs://hadoop-master/user/mp2019/5000_ECBDL14_10tst.data", source="csv")

small_tr <- select(tttm, f1, f2, f3, f4, f5, class)
spark_write_csv(small_tr, path="hdfs://hadoop-master/user/mp2019/mp71722388/ECDB-2012.small.training", delimiter = ",", header=TRUE,  mode = 'overwrite')

# Ahora equilibramos el fichero para que  tenga el mismo número de registros de las clases 0 y 1

num_regs_pre <- as.integer(collect(count(small_tr)))
summarize( group_by(small_tr, class), count = n(), percent= n()/num_regs_pre*100.0)
# Source: spark<?> [?? x 3]
#   class   count percent
#   <int>   <dbl>   <dbl>
# 1     0 2849280   98.3
# 2     1   48637    1.68

small_tr_eq <- ovun.sample(class ~ ., data = small_tr, p = 0.5, seed = 1, method = "under")$data

num_regs_post <- as.integer(collect(count(small_tr_eq)))
summarize( group_by(small_tr_eq, class), count = n(), percent= n()/num_regs_post*100.0)
# A tibble: 2 x 3
#   class count percent
#   <int> <int>   <dbl>
# 1     0 48645    50.0
# 2     1 48637    50.0

small_tr_eq_tbl <- copy_to(sc, small_tr_eq)

## Guardar los resultados sobreescribiendo el fichero
BALANCED_FILEPATH = "hdfs://hadoop-master/user/mp2019/mp71722388/ECDB-2012.small.training"
spark_write_csv(small_tr_eq_tbl, path=BALANCED_FILEPATH, delimiter = ",", header=TRUE,  mode = 'overwrite')

## Si ya tenemos el fichero guardado podemos saltarnos todo lo anterior y empezar desde aqui tras leerlo
# small_tr_eq_tbl <- spark_read_csv(sc, name="small_tr_eq_tbl", path="hdfs://hadoop-master/user/mp2019/mp71722388/ECDB-2012.small.training", delimiter = ",", header=TRUE, overwrite = TRUE)

# Creamos un conjunto de entrenamiento con el 75% de los datos

# REF. https://stackoverflow.com/a/35343912/3594238
partitions <- small_tr_eq_tbl %>% sdf_random_split(training = 0.75, test = 0.25, seed = 123)

# Aplicamos 3 clasificadores
my_features = c("f1","f2","f3","f4","f5")

## Regresion Lineal
tiempo_pre = Sys.time()
reli_model <- ml_linear_regression(partitions$training, f1~f5, features="class")
reli_predicted <- ml_predict(reli_model, newdata = partitions$test)
RELI_ACCURACY = ml_multiclass_classification_evaluator(reli_predicted, metric_name="accuracy")
tiempo_reli = Sys.time() - tiempo_pre
# Error: java.lang.IllegalArgumentException: Field "label" does not exist

## Regresion Logistica
tiempo_pre = Sys.time()
relo_model <- partitions$training %>% ml_logistic_regression(response = "class", features = my_features)
relo_predicted <- ml_predict(relo_model, newdata = partitions$test)
RELO_ACCURACY = ml_multiclass_classification_evaluator(relo_predicted, metric_name="accuracy")
tiempo_relo = Sys.time() - tiempo_pre
# [1] 0.6018519

## Random Forest
tiempo_pre = Sys.time()
training_cv <- partitions$training %>% select(f1,f2,f3,f4,f5,class) %>% mutate(class1=as.character(class)) %>% select(f1,f2,f3,f4,f5,class=class1)
randfor_model <- ml_random_forest(training_cv, response="class", features=my_features)
randfor_predicted <- ml_predict(randfor_model, partitions$test)
RF_ACCURACY = ml_multiclass_classification_evaluator(randfor_predicted, metric_name="accuracy")
tiempo_rf = Sys.time() - tiempo_pre
# [1] 0.6315789

## K-means
tiempo_pre = Sys.time()
k_means_model <- partitions$training %>% select(f1,f2,f3,f4,f5,class) %>% ml_kmeans(features = "class", centers = 3)
k_means_predicted <- ml_predict(k_means_model, newdata = partitions$test)
K_MEANS_ACCURACY = ml_multiclass_classification_evaluator(k_means_predicted, metric_name="accuracy")
tiempo_k_means = Sys.time() - tiempo_pre
# Error: java.lang.IllegalArgumentException: requirement failed: Column prediction must be of type DoubleType but was actually IntegerType.


# Mostrar los resultados
base = "Accuray de la ejecución del modelo de"
base_tiempo = "y ha tardado"

# print(paste(base, "Regresión Lineal", RELI_ACCURACY, base_tiempo, tiempo_reli))
print(paste(base, "Regresión Logística", RELO_ACCURACY, base_tiempo, tiempo_relo))
print(paste(base, "Random Forest", RF_ACCURACY, base_tiempo, tiempo_rf))
# print(paste(base, "K-means", K_MEANS_ACCURACY, base_tiempo, tiempo_k_means))

# [1] "Accuray de la ejecución del modelo de Regresión Logística 0.537398507340277 y ha tardado 4.71406221389771"
# [1] "Accuray de la ejecución del modelo de Random Forest 0.576645664068977 y ha tardado 6.35683274269104"

sparkR.session.stop()
