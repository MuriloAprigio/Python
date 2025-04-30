#Usado para automatizar alguns processos entre o databricks e o sql server

from pyspark.sql.types import *  
from pyspark.sql.utils import AnalysisException
from pyspark.sql import Row

# Esquema alvo
catalog = "exemplo_lakehouse"
schema = "exemplo_1"

# Mapeamento para SELECT (CASTs)
select_type_map = {
    StringType: "STRING",
    IntegerType: "INT",
    LongType: "BIGINT",
    ShortType: "INT",
    ByteType: "INT",
    DecimalType: "INT",
    DoubleType: "INT",
    FloatType: "DOUBLE",
    DateType: "DATE",
    TimestampType: "DATE",
    BooleanType: "STRING",
    BinaryType: "STRING"
}

# Palavras que indicam valor financeiro ou similar
palavras_valor = ["VALOR", "VLR", "VAL", "PRECO", "PERCENTUAL", "TAXA", "CUSTO"]

# Lista tabelas
tabelas = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").collect()

# Resultados
resultados = []

for tbl in tabelas:
    nome_tabela = tbl.tableName
    nome_completo = f"`{catalog}`.`{schema}`.`{nome_tabela}`"
    try:
        df = spark.table(nome_completo)
        esquema = df.schema

        # SELECT SQL — sobrescrevendo para DT_ como DATE
        select_casts = []
        for field in esquema:
            col_name = field.name
            col_upper = col_name.upper()
            data_type = type(field.dataType)

            if col_upper.startswith("DT_"):
                sql_type = "DATE"
            else:
                sql_type = select_type_map.get(data_type, "STRING")

            select_casts.append(f"CAST({col_name} AS {sql_type}) AS {col_name}")

        select_clause = ", ".join(select_casts)
        select_sql = f"SELECT {select_clause} FROM {nome_completo} ;"
        colunas = f"{select_clause}"

        # DDL SQL com regras avançadas por nome e tipo
        ddl_fields = []
        for field in esquema:
            col_name = field.name
            col_upper = col_name.upper()
            data_type = type(field.dataType)

            # Regras por nome
            if col_upper.startswith(("CD_", "NR_", "IE_")):
                ddl_type = "INT"
            elif col_upper.startswith("DT_"):
                ddl_type = "DATE"
            elif any(palavra in col_upper for palavra in palavras_valor):
                ddl_type = "FLOAT"
            elif data_type in (DateType, TimestampType):
                ddl_type = "DATE"
            elif data_type in (StringType, BooleanType, BinaryType):
                ddl_type = "VARCHAR(8000)"
            elif data_type in (DecimalType, DoubleType, FloatType):
                ddl_type = "FLOAT"
            else:
                ddl_type = "VARCHAR(8000)"

            ddl_fields.append(f"[{col_name}] {ddl_type}")

        # Modifica nome da tabela: troca prefixo e transforma em maiúsculo
        nome_ddl = f"[CATALOGO_EXEMPLO].[{nome_tabela.upper().replace('TBL_EXEMPLO_', 'EXEMPLO_')}]"

        ddl_sql = f"CREATE TABLE {nome_ddl} (\n  " + ",\n  ".join(ddl_fields) + "\n);"
        drop    = f"IF OBJECT_ID({nome_ddl}, 'U') IS NOT NULL DROP TABLE {nome_ddl};"



        resultados.append(Row(
            Tabela=nome_completo,
            Status="✅ OK",
            Erro="",
            SelectSQL=select_sql,
            colunas=colunas,
            nome_ddl=nome_ddl,
            CreateTableSQL=ddl_sql,
            drop=drop
        ))

    except AnalysisException as e:
        resultados.append(Row(
            Tabela=nome_completo,
            Status="❌ Erro",
            Erro=str(e).split('\n')[0],
            SelectSQL="",
            colunas="",
            nome_ddl=nome_ddl,
            CreateTableSQL="",
            drop=drop
        ))

# Exibir resultado
res_df = spark.createDataFrame(resultados)
res_df.display()
