from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pandas as pd

import requests 


import findspark
findspark.init()

spark = SparkSession.builder.getOrCreate()

## baixar arquivo direto da fonte
##zipUrl = 'http://repositorio.dados.gov.br/segrt/pensionistas/PENSIONISTAS_082021.zip'
##outPath = './raw/'

##req = requests.get(zipUrl)
##dirOut = ( outPath + 'PENSIONISTAS.zip' )
##with open(dirOut, 'wb') as f:
##            f.write(req.content)
##            f.close()
            

df_pensionistas = (
    spark
    .read
    .format('csv')
    .option('sep',';')
    .option('header',True)
    .load('PENSIONISTAS_082021.csv')
)


cols = ['NOME DO BENEFICIARIO', 
        'NOME DO ORGAO',
        'DATA DE NASCIMENTO',
        'UF DA UPAG DE VINCULACAO',
        'NATUREZA PENSAO',
        'DATA INICIO DO BENEFICIO',
        'DATA FIM DO BENEFICIO',
        'RENDIMENTO LIQUIDO',
        'PAGAMENTO SUSPENSO']


df = df_pensionistas.select(cols)

df = (df
 .withColumnRenamed('NOME DO BENEFICIARIO','nome')
 .withColumnRenamed('NOME DO ORGAO','orgao')
 .withColumnRenamed('DATA DE NASCIMENTO','dtnas')
 .withColumnRenamed('UF DA UPAG DE VINCULACAO','uf')
 .withColumnRenamed('NATUREZA PENSAO','natpensao')
 .withColumnRenamed('DATA INICIO DO BENEFICIO','dtiniben')
 .withColumnRenamed('DATA FIM DO BENEFICIO','dtfimben')
 .withColumnRenamed('RENDIMENTO LIQUIDO','rendLiquido')
 .withColumnRenamed('PAGAMENTO SUSPENSO','pagsuspenso')
)

df = df.toPandas()

df.to_csv('pensionistasfederal.csv', index=False)