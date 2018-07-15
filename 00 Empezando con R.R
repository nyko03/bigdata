##Empezando con R 
getwd()
list.files(rxGetOption("sampleDataDir"))

# Establecer la ubicaci?n para los datos de ejemplo
bigDataDirRoot <- "/example/data"

# Crear una carpeta local para almacenar los datos temporales
source <- "/tmp/AirOnTimeCSV2012"
dir.create(source)

# Descargar datos de ejemplo
remoteDir <- "https://packages.revolutionanalytics.com/datasets/AirOnTimeCSV2012"
download.file(file.path(remoteDir, "airOT201201.csv"), file.path(source, "airOT201201.csv"))
download.file(file.path(remoteDir, "airOT201202.csv"), file.path(source, "airOT201202.csv"))
download.file(file.path(remoteDir, "airOT201203.csv"), file.path(source, "airOT201203.csv"))
download.file(file.path(remoteDir, "airOT201204.csv"), file.path(source, "airOT201204.csv"))
download.file(file.path(remoteDir, "airOT201205.csv"), file.path(source, "airOT201205.csv"))
download.file(file.path(remoteDir, "airOT201206.csv"), file.path(source, "airOT201206.csv"))
download.file(file.path(remoteDir, "airOT201207.csv"), file.path(source, "airOT201207.csv"))
download.file(file.path(remoteDir, "airOT201208.csv"), file.path(source, "airOT201208.csv"))
download.file(file.path(remoteDir, "airOT201209.csv"), file.path(source, "airOT201209.csv"))
download.file(file.path(remoteDir, "airOT201210.csv"), file.path(source, "airOT201210.csv"))
download.file(file.path(remoteDir, "airOT201211.csv"), file.path(source, "airOT201211.csv"))
download.file(file.path(remoteDir, "airOT201212.csv"), file.path(source, "airOT201212.csv"))

# Establecer el directorio en  bigDataDirRoot para cargar los datos
inputDir <- file.path(bigDataDirRoot,"AirOnTimeCSV2012")

# Crear el directorio
rxHadoopMakeDir(inputDir)

# Copiar los datos del origen 
rxHadoopCopyFromLocal(source, bigDataDirRoot)

# Definir el sitema de ficheros como HDFS
hdfsFS <- RxHdfsFileSystem()

# Crear una lista de informaci?n para los datos de aerol?neas
airlineColInfo <- list(
  DAY_OF_WEEK = list(type = "factor"),
  ORIGIN = list(type = "factor"),
  DEST = list(type = "factor"),
  DEP_TIME = list(type = "integer"),
  ARR_DEL15 = list(type = "logical"))

# Obtener todos los nombres de columnas
varNames <- names(airlineColInfo)

# Definir el origen de datos como Texto
airOnTimeData <- RxTextData(inputDir, colInfo = airlineColInfo, varsToKeep = varNames, fileSystem = hdfsFS)

# Definir el origen de datos en el sistema local
airOnTimeDataLocal <- RxTextData(source, colInfo = airlineColInfo, varsToKeep = varNames)

# Especificar la f?rmula a utiliar
formula = "ARR_DEL15 ~ ORIGIN + DAY_OF_WEEK + DEP_TIME + DEST"

# Definir el contexto de computaci?n como Spark
mySparkCluster <- RxSpark()

# Establecer el contexto de computaci?n
rxSetComputeContext(mySparkCluster)

# ejecutar una regresi?n log?stica
system.time(
  modelSpark <- rxLogit(formula, data = airOnTimeData)
)

# Mostrar un resumen
summary(modelSpark)