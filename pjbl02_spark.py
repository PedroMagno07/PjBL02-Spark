import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# no Windows o Spark procura por "python3" nos workers e não encontra
os.environ["PYSPARK_PYTHON"] = sys.executable

ARQUIVO = sys.argv[1] if len(sys.argv) > 1 else "data/operacoes_comerciais_inteira.csv"
SAIDA = "resultado.txt"

# posição de cada coluna depois do split
PAIS, ANO, CODIGO, COMMODITY, FLOW, PRECO, PESO, UNIDADE, QUANTIDADE, CATEGORIA = range(10)

spark = (SparkSession.builder
         .master("local[*]")
         .appName("PjBL02")
         .config("spark.driver.memory", "6g")
         .getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("ERROR")

saida = open(SAIDA, "w", encoding="utf-8")


def registrar(numero, enunciado, resposta):
    saida.write(f"Questão {numero} - {enunciado}\n")
    # as questões agrupadas devolvem uma lista, cada item vai em uma linha
    if isinstance(resposta, list):
        for item in resposta:
            saida.write(f"{item}\n")
    else:
        saida.write(f"{resposta}\n")
    saida.write("\n")
    saida.flush()
    print(f"questão {numero} ok")


def linha_valida(campos):
    # weight_kg e quantity têm valores vazios, mas a transação continua existindo
    # (várias são "No Quantity"). Então aqui só cobro as colunas usadas de 1 a 9
    # e o peso é tratado na questão 10, que é a única que depende dele.
    if len(campos) != 10:
        return False
    return all(campos[i].strip() != "" for i in (PAIS, ANO, FLOW, PRECO, CATEGORIA))


# as questões 1 a 9 partem do mesmo RDD, então vale deixar em memória
transacoes = (sc.textFile(ARQUIVO)
              .map(lambda x: x.split(";"))
              .filter(lambda x: x[PAIS] != "country_or_area")  # retira o cabeçalho
              .filter(linha_valida)
              .cache())


# 1 - transações envolvendo o Brasil (RDD)
q1 = transacoes.filter(lambda x: x[PAIS] == "Brazil").count()
registrar(1, "Número de transações envolvendo o Brasil (RDD)", q1)


# 2 - transações envolvendo o Brasil (PairRDD)
q2 = (transacoes
      .filter(lambda x: x[PAIS] == "Brazil")
      .map(lambda x: (x[PAIS], 1))
      .reduceByKey(lambda a, b: a + b)
      .collect())
registrar(2, "Número de transações envolvendo o Brasil (PairRDD)", q2)


# 3 - transações envolvendo o Brasil em 2016 (RDD)
q3 = transacoes.filter(lambda x: x[PAIS] == "Brazil" and x[ANO] == "2016").count()
registrar(3, "Número de transações envolvendo o Brasil em 2016 (RDD)", q3)


# 4 - transações envolvendo o Brasil em 2016 (PairRDD)
q4 = (transacoes
      .filter(lambda x: x[PAIS] == "Brazil" and x[ANO] == "2016")
      .map(lambda x: ((x[PAIS], int(x[ANO])), 1))
      .reduceByKey(lambda a, b: a + b)
      .collect())
registrar(4, "Número de transações envolvendo o Brasil em 2016 (PairRDD)", q4)


# 5 - transações por flow e por ano a partir de 2010
q5 = (transacoes
      .filter(lambda x: int(x[ANO]) >= 2010)
      .map(lambda x: ((int(x[ANO]), x[FLOW].upper()), 1))
      .reduceByKey(lambda a, b: a + b)
      .sortByKey()
      .collect())
registrar(5, "Número de transações por flow e por ano, a partir de 2010", q5)


saida.close()
spark.stop()
