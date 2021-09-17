from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pandas as 

import requests 

import findspark
findspark.init()

spark = SparkSession.builder.getOrCreate()

w 'http://repositorio.dados.gov.br/segrt/pensionistas/PENSIONISTAS_082021.zip'

df_pensionistas = (
    spark
    .read
    .format('csv')
    .option('sep',';')
    .option('header',True)
    .load('PENSIONISTAS_082021.csv')
)