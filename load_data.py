'''

    Code of pyspark ETL for creation 

    1. Convert dataframe data from csv
    2. Convert dataframe data from response of api 

'''

from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, BooleanType, DateType
from pyspark.sql.types import Row

from pyspark.sql import SQLContext

spark = SparkSession \
    .builder \
    .appName("colombia_tratos") \
    .getOrCreate()


dataframe_tratados = spark.read.csv('Colombia_tratado/Tratados_internacionales_de_Colombia.csv',header=True)

#Create functions for date type 

func_to_date =  udf (lambda x: datetime.strptime(x, '%d/%m/%Y') if x != "(NO REGISTRA)" else None, DateType())

func_string_to_boolean = udf (lambda x: True if x == 'SI' else False , BooleanType())


#Make transformations of data 

dataframe_colombia_tratados = dataframe_tratados.withColumnRenamed("Nombre del Tratado","NombredelTratado") \
                                                .withColumn("Bilateral",func_string_to_boolean(col("Bilateral"))) \
                                                .withColumnRenamed("Lugar de Adopcion", "LugardeAdopcion") \
                                                .withColumn('Fecha de Adopcion', func_to_date(col('Fecha de Adopcion'))) \
                                                .withColumnRenamed("Fecha de Adopcion", "FechadeAdopcion") \
                                                .withColumnRenamed("Estados-Organismos", "EstadosOrganismos") \
                                                .withColumnRenamed("Naturaleza del Tratado","NaturalezadelTratado")\
                                                .withColumnRenamed("Suscribio Por Colombia", "SuscribioPorColombia")\
                                                .withColumn('Fecha Ley Aprobatoria', func_to_date(col('Fecha Ley Aprobatoria'))) \
                                                .withColumnRenamed("Fecha Ley Aprobatoria", "FechaLeyAprobatoria") \
                                                .withColumnRenamed("Numero Ley Aprobatoria", "NumeroLeyAprobatoria") \
                                                .withColumn('Sentencia Fecha Ley', func_to_date(col('Sentencia Fecha Ley'))) \
                                                .withColumnRenamed('Sentencia Fecha Ley', 'SentenciaFechaLey') \
                                                .withColumnRenamed("Sentencia Numero", "SentenciaNumero") \
                                                .withColumn('Decreto Fecha Diario Oficial', func_to_date(col('Decreto Fecha Diario Oficial'))) \
                                                .withColumnRenamed('Decreto Fecha Diario Oficial', "DecretoFechaDiarioOficial") \
                                                .withColumnRenamed('Decreto Numero Diario Oficial', "DecretoNumeroDiarioOficial")   


dataframe_info = spark.read.json('Colombia_tratado/all.json')


#Create functions for data of json countries 

func_get_name = udf ( lambda x: str(x[2]).upper(), StringType())
func_get_cod_call = udf ( lambda x: str(x[0]), StringType())
func_get_are_int = udf (lambda x: int(x) if x != None else x, IntegerType())
func_list_commas = udf (lambda x: func_list_to_commas(x),StringType())
func_get_borders = udf (lambda x: len(x) if x != None else x , IntegerType())



def func_list_to_commas(value):    
    list_test = []
    if value != None:
        for data in value:
            list_test.append(data[1])            
    return ','.join(list_test) 

#Modificate data for json info countries

dataframe_countries = dataframe_info.withColumn('translations', func_get_name(col('translations')))\
                                    .withColumnRenamed("translations","PaisdelTratado") \
                                    .withColumn('callingCodes', func_get_cod_call(col('callingCodes')))\
                                    .withColumnRenamed("callingCodes","Codigodellamadas") \
                                    .withColumnRenamed("capital", "Capital")\
                                    .withColumnRenamed("region", "Region") \
                                    .withColumnRenamed("subregion", "Subregion") \
                                    .withColumnRenamed("population", "Poblacion") \
                                    .withColumn("area", func_get_are_int(col("area"))) \
                                    .withColumnRenamed("timezones", "ZonaHoraria") \
                                    .withColumn('currencies', func_list_commas(col('currencies'))) \
                                    .withColumnRenamed("currencies", "Monedas") \
                                    .withColumn('languages', func_list_commas(col('languages'))) \
                                    .withColumnRenamed("languages", "Idiomas") \
                                    .withColumn('borders', func_get_borders(col('borders'))) \
                                    .withColumnRenamed("borders", "CantidadFronteras")



#Make Join for name of country make a Upper for table csv, take translate country name (Esp)

dataframe_countries = dataframe_countries.select(col('PaisdelTratado'), col ("Codigodellamadas"),col("Capital"), col("Region"), col("Subregion"), col("Poblacion"), col ("area"), col ("ZonaHoraria"), col("Monedas"), col ("Idiomas"), col('CantidadFronteras') )

dataframe_countries.show(5)
dataframe_colombia_tratados.show(5)


dataframe_colombia_tratados.join(dataframe_countries, dataframe_colombia_tratados.EstadosOrganismos ==  dataframe_countries.PaisdelTratado,"inner") \
     .show(truncate=False)



# Save file .avro 

dataframe_colombia_tratados.write.format("avro").mode("overwrite").save("Colombia_tratado/colombia_tratados.avro")
  

#Create table and save data into table postgresql
dataframe_colombia_tratados.write.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "colombia_tratados") \
    .option("user", "postgres").option("password", "EDac4550.*").save()

