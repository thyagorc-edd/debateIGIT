from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as FloatType

import pandas as pd
import requests
import zipfile
import os

# Local Datalake Paths
local_zones = {
    'landing': './data/landing_zone',
    'raw': './data/raw_zone',
    'staging': './data/staging_zone',
    'consumer': './data/consumer_zone',
}

# Link para download do arquivo de pensionistas
zipUrl = 'http://repositorio.dados.gov.br/segrt/pensionistas/PENSIONISTAS_082021.zip'



class jobPensionistas:

    def __init__(self, spark_session):

        self.spark = spark_session
        self.read_options = {
            'header': True, 
            'sep': ';'
        }


    def download_data(self):

        ### Landing Zone
        # Path do diretorio para gravação do arquivo
        outPath = f'{local_zones["landing"]}/pensionistas'
        os.makedirs(outPath, exist_ok=True)
            
        # baixar arquivo direto da fonte
        req = requests.get(zipUrl)
        zipOut = f'{outPath}/PENSIONISTAS.zip'
        print('Saving:', zipOut)
        with open(zipOut, 'wb') as f:
            f.write(req.content)
            f.close()


        ### Raw Zone
        outPath = f'{local_zones["raw"]}/pensionistas'
        os.makedirs(outPath, exist_ok=True)

        # Unzip - Extract all
        with zipfile.ZipFile(zipOut, 'r') as zipObj:
            # Extract all
            listOfFileNames = zipObj.namelist()
            # varre a lista de arquivos - file names
            for fileName in listOfFileNames:
                # Apenas .csv
                if fileName.endswith('.csv'):
                    print('Unzip:', fileName, ' -> ', outPath)
                    zipObj.extract(fileName, path=outPath)
                    self.fileName = fileName

    
    def data_cleaning(self):

        csvFile = f'{local_zones["raw"]}/pensionistas/{self.fileName}'

        # Carregando o Dataframe
        df_origem = (
            self.spark.read
            .format('csv')
            .option('sep',';')
            .option('header',True)
            .load(csvFile)
        )

        # Selecionando as Colunas representativas
        cols = ['NOME DO BENEFICIARIO', 
                'NOME DO ORGAO',
                'DATA DE NASCIMENTO',
                'UF DA UPAG DE VINCULACAO',
                'NATUREZA PENSAO',
                'DATA INICIO DO BENEFICIO',
                'DATA FIM DO BENEFICIO',
                'RENDIMENTO LIQUIDO',
                'PAGAMENTO SUSPENSO']

        # Renomeando das colunas
        self.df_pensionistas = (
            df_origem
            .select(cols)
            .withColumnRenamed('NOME DO BENEFICIARIO','nome')
            .withColumnRenamed('NOME DO ORGAO','orgao')
            .withColumnRenamed('DATA DE NASCIMENTO','dtnasc')
            .withColumnRenamed('UF DA UPAG DE VINCULACAO','uf')
            .withColumnRenamed('NATUREZA PENSAO','natpensao')
            .withColumnRenamed('DATA INICIO DO BENEFICIO','dtiniben')
            .withColumnRenamed('DATA FIM DO BENEFICIO','dtfimben')
            .withColumnRenamed('RENDIMENTO LIQUIDO','rendLiquido')
            .withColumnRenamed('PAGAMENTO SUSPENSO','pagsuspenso')
        )


        # Trantando e convertendo as colunas
        self.df_pensionistas = (
            self.df_pensionistas
            .select('nome', 'orgao', 'dtnasc', 'uf', 'natpensao', 'dtiniben', 'dtfimben', 'rendLiquido', 'pagsuspenso')
            .withColumn('dtnasc', f.to_date( f.col('dtnasc'),'ddMMyyyy') )
            .withColumn('dtiniben', f.to_date( f.col('dtiniben'),'ddMMyyyy') )
            .withColumn('dtfimben', f.to_date( f.col('dtfimben'),'ddMMyyyy') )
            .withColumn('rendLiquido', f.regexp_replace(f.regexp_replace(f.col('rendLiquido'), "\\.", ""), "\\,", "."))
            .withColumn('rendLiquido', f.col('rendLiquido').cast('float') )
            .withColumn('pagsuspenso', f.when(f.col('pagsuspenso') == 'NAO', False)
                                    .when(f.col('pagsuspenso') == 'SIM', True)
                                    .otherwise(None) )
        )

        del df_origem


    def data_transform(self):

        # Trantando e convertendo as colunas
        self.df_pensionistas = (
            self.df_pensionistas
            .select('nome', 'orgao', 'dtnasc', 'uf', 'natpensao', 'dtiniben', 'dtfimben', 'rendLiquido', 'pagsuspenso')
            .withColumn('limitMax35', f.expr('round(rendLiquido*0.35,2)').cast('float') )
            .withColumn('limitMax40', f.expr('round(rendLiquido*0.40,2)').cast('float') )
            .withColumn('codFaixaRenda', f.when(f.col('rendLiquido').between(0,3000),1)
                                    .when(f.col('rendLiquido').between(3001,7000),2)
                                    .when(f.col('rendLiquido').between(7001,15000),3)
                                    .when(f.col('rendLiquido').between(15001,30000),4)
                                    .otherwise(5) )
            .withColumn('descFaixaRenda', f.when(f.col('codFaixaRenda')==1,'Entre 0 e 3000')
                                    .when(f.col('codFaixaRenda')==2,'Entre 3001 e 7000')
                                    .when(f.col('codFaixaRenda')==3,'Entre 7001 e 15000')
                                    .when(f.col('codFaixaRenda')==4,'Entre 15001 e 30000')
                                    .otherwise('Acima de 30.000') )
            .withColumn('idade' , f.floor( f.datediff( f.current_date(), f.col('dtnasc') ) / 365))
            .withColumn('codFaixaIdade', f.when(f.col('idade').between(0,15),1)
                                    .when(f.col('idade').between(16,30),2)
                                    .when(f.col('idade').between(31,60),3)
                                    .when(f.isnull(f.col('idade')),0)
                                    .otherwise(4) )    
            .withColumn('descFaixaIdade', f.when(f.col('codFaixaIdade')==1,'Entre 0 e 15 anos')
                                    .when(f.col('codFaixaIdade')==2,'Entre 16 e 30 anos')
                                    .when(f.col('codFaixaIdade')==3,'Entre 31 e 60 anos')
                                    .when(f.col('codFaixaIdade')==0,'Idade nao informada')
                                    .otherwise('Acima de 60 anos') )
        )


    def write_data(self, outPath):
        print('Write Data:', outPath)
        (
            self.df_pensionistas
            .write
            .format('parquet')
            .save(outPath, mode='overwrite')
        )

    
    def run(self):

        self.download_data()
        self.data_cleaning()
        self.write_data(outPath=f'{local_zones["staging"]}/pensionistas')
        self.data_transform()
        self.write_data(outPath=f'{local_zones["consumer"]}/pensionistas')

