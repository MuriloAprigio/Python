#Desenvolvido para rodar no databricks
#Está divido em 'Células' como um Jupyter Notebook

%pip install openpyxl
import pandas as pd
# Caminho do arquivo Excel
file_path = "/Workspace/Users/murilo.ana/exemplo_sharepoint.xlsx"

# Leia o arquivo Excel usando Pandas
df_pandas = pd.read_excel(file_path)

# Converta o DataFrame do Pandas para um DataFrame do Spark
df_spark = spark.createDataFrame(df_pandas)

# Mostre o DataFrame do Spark
df_spark.show()

---------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.functions import expr, when, col

# Mapeamento de tipos de dados de Spark para SQL Server
type_mapping = {
    "int": "int",
    "nvarchar": "string",
    "datetime": "timestamp",
    "float": "double",
    "bit": "int",
    "varchar": "string",
    "bigint": "bigint"
}
# Aplicando a conversão usando a função when
# Iniciando com uma coluna nova que não tem valor definido
df = df_spark.withColumn("column_type_sql", F.lit(None))

for spark_type, sql_type in type_mapping.items():
    df = df.withColumn(
        "column_type_sql",
        F.when(col("column_type") == spark_type, sql_type).otherwise(col("column_type_sql"))
    )

# Exibir o DataFrame com os tipos convertidos para SQL Server
df.show(truncate=False)
----------------------------------------------------------
from pyspark.sql.functions import col, when, expr

# Verificando o tipo e aplicando a condição
df = df.withColumn(
    'query',
    when(
        col("column_type_sql").isin("int", "bigint"), 
        col("column_name")
    ).when(
        col("column_type_sql").isin("varbinary"), 
        expr('"\'\' as column_name"')
    ).when(
        col("column_name").isin("ORIGEM"), 
        expr('"\'SHAREPOINT_CUBO\' as ORIGEM"')
    ).when(
        (col("column_name") == "DT_CARGA") & (col("column_type_sql") == "string"),
        expr('"cast(_fivetran_synced as string) as DT_CARGA"')
    ).when(
        (col("column_name") == "DT_CARGA") & (col("column_type_sql") == "timestamp"),
        expr('"_fivetran_synced as DT_CARGA"')
    ).otherwise(
        expr('concat("cast(", column_name, " as ", column_type_sql, ") as ", column_name)')
    )
)

df.show(truncate=False)
---------------------------------------------------------------------
%pip install openpyxl
from pyspark.sql.functions import expr, concat_ws, collect_list
# Criando o novo DataFrame
new_df = df.groupBy("table_name") \
    .agg(concat_ws(", ", collect_list("query")).alias("query"))

# Convertendo o DataFrame do Spark para um DataFrame do Pandas
new_df_pandas = new_df.toPandas()

-------------------------------------------------------------------
import pandas as pd

new_df_pandas['table'] = new_df_pandas['table_name']\
    .str.replace('FILE_RAW_', '') \

tabelas = new_df_pandas['table']

def formatandoInput(table):
    task_name = table
    uc_table = table.lower()  # Converte para minúsculo
    # Busca o valor de 'table_name' correspondente a 'task_name'
    target_table = new_df_pandas.loc[new_df_pandas['table'] == task_name, 'table_name'].str.lower().iloc[0]
    sistema = 'sharepoint'
    
    # Obtém a coluna 'query' correspondente a 'task_name'
    columns = new_df_pandas.loc[new_df_pandas['table'] == task_name, 'query'].iloc[0]
