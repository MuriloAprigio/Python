# Notebook: hash_function_notebook

import hashlib
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Função para gerar UUID a partir de múltiplos valores com tratamento de exceção
def oc_cria_sk(*values):
    try:
        # Concatenando os valores diretamente 
        concatenated_value = ''.join(map(str, values))  # Converte tudo para string e concatena
        
        # Tenta codificar com latin1 primeiro
        try:
            encoded_value = concatenated_value.encode('latin1')
        except UnicodeEncodeError:
            # Se falhar, tenta codificar com windows-1252
            encoded_value = concatenated_value.encode('windows-1252')
        
        # Gerando o hash MD5 com a codificação selecionada (latin1 ou windows-1252)
        md5_hash = hashlib.md5(encoded_value).hexdigest()

        # Formatando o hash MD5 em formato UUID (sem repetir os índices)
        formatted_uuid = f'{md5_hash[6:8]}{md5_hash[4:6]}{md5_hash[2:4]}{md5_hash[0:2]}' \
                         f'-{md5_hash[10:12]}{md5_hash[8:10]}' \
                         f'-{md5_hash[14:16]}{md5_hash[12:14]}' \
                         f'-{md5_hash[16:18]}{md5_hash[18:20]}' \
                         f'-{md5_hash[20:32]}'

        return formatted_uuid.upper()
    except Exception as e:
        # Se algum erro ocorrer, captura a exceção e retorna uma string indicando o erro
        return f"Error: {str(e)}"

# Registrando a função como UDF no Databricks
udf_oc_cria_sk = udf(funcao_cria_pk, StringType())

# Registrando a UDF para uso em SQL no Databricks
spark.udf.register("funcao_cria_pk", udf_oc_cria_sk)
