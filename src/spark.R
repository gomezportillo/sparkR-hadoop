# https://github.com/manuparra/taller_SparkR/blob/master/Parte%202.%20S06.%20Mineria%20de%20datos%20y%20Machine%20Learning%20con%20sparklyr.ipynb

# install.packages('sparklyr')
# install.packages('dplyr')
# install.packages('caret')
# install.packages('ROSE')

# https://github.com/manuparra/TallerH2S#create-the-spark-environment

if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "/opt/spark-2.2.0/")
}

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "1g"), enableHiveSupport=FALSE)

# sparkR.version()
# spark_install(version = "2.2.0")

library(sparklyr)
library(dplyr)
library(caret)
library(ROSE)

sc <- spark_connect(master = "local", version = "2.2.0")

tttm <- spark_read_csv(sc, name="tttm", path="hdfs://hadoop-master/user/mp2019/5000_ECBDL14_10tst.data", delimiter = ",", header=TRUE, overwrite = TRUE)
# df5000 <- read.df("hdfs://hadoop-master/user/mp2019/5000_ECBDL14_10tst.data", source="csv")

small_tr <- select(tttm, f1, f2, f3, f4, f5, class)
spark_write_csv(small_tr, path="hdfs://hadoop-master/user/mp2019/mp71722388/ECDB-2012.small.training", delimiter = ",", header=TRUE,  mode = 'overwrite')

# ahora equilibramos el fichero para que  tenga el mismo número de registros de las clases 0 y 1

num_regs_pre <- as.integer(collect(count(small_tr)))
summarize( group_by(small_tr, class), count = n(), percent= n()/num_regs_pre*100.0)
# A tibble: 1 x 3
#   class count percent
#   <int> <dbl>   <dbl>
# 1     0  4873   97.5
# 2     1   127    2.54

small_tr_eq <- ovun.sample(class ~ ., data = small_tr, p = 0.5, seed = 1, method = "under")$data

num_regs_post <- as.integer(collect(count(small_tr_eq)))
summarize( group_by(small_tr_eq, class), count = n(), percent= n()/num_regs_post*100.0)
# A tibble: 2 x 3
#   class count percent
#   <int> <int>   <dbl>
# 1     0   127      50
# 2     1   127      50

small_tr_eq_tbl <- copy_to(sc, small_tr_eq)
spark_write_csv(small_tr_eq_tbl, path="hdfs://hadoop-master/user/mp2019/mp71722388/ECDB-2012.small.training", delimiter = ",", header=TRUE,  mode = 'overwrite')

# creamos un conjunto de entrenamiento con el 90% de los datos

# REF. https://stackoverflow.com/a/35343912/3594238
partition_index <- createDataPartition(y=small_tr_eq$class, p=0.85, list=FALSE)
partition_training <- small_tr_eq[partition_index,]
partition_test <- small_tr_eq[-partition_index,]

# convertir de R.data.frame a tbl_spark para poder trabajar con los datos
partition_training_tbl <- copy_to(sc, partition_training)
partition_test_tbl <- copy_to(sc, partition_test)

# aplicamos 3 clasificadores
my_features = c("f1","f2","f3","f4","f5")

## regresion lineal
reli_model <- ml_linear_regression(partition_training_tbl, f1~f5)
reli_predicted <- predict(reli_model, newdata = partition_test_tbl)
summary(reli_predicted)

## regresion logistica
relo_model <- partition_training_tbl %>% ml_logistic_regression(response = "class", features = my_features)
relo_predicted <- predict(reli_model, newdata = partition_test_tbl)

## random forest
training_cv <- partition_training_tbl %>% select(f1,f2,f3,f4,f5,class) %>% mutate(class1=as.character(class)) %>% select(f1,f2,f3,f4,f5,class=class1)
randfor_model <- ml_random_forest(training_cv, response="class", features=my_features)
randfor_predicted <- ml_predict(randfor_model, training_cv)

# con el dataset entero
# ml_rf <- ml_random_forest(small_tr_eq_tbl, response="class", features=my_features, num.trees = 20, type = "classification")
