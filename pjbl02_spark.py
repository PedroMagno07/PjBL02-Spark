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
    # as linhas com código TOTAL (commodity "ALL COMMODITIES") não são transações:
    # são a soma de tudo o que o país negociou no ano e no flow. Mantê-las contaria
    # os mesmos valores duas vezes (mesma decisão do PjBL1).
    if campos[CODIGO] == "TOTAL":
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


# 6 - média de Price em 2016 (RDD)
# junto o contador na mesma tupla para percorrer o RDD uma vez só
soma, qtd = (transacoes
             .filter(lambda x: x[ANO] == "2016")
             .map(lambda x: (float(x[PRECO]), 1))
             .reduce(lambda a, b: (a[0] + b[0], a[1] + b[1])))
registrar(6, "Média da coluna Price em 2016 (RDD)", soma / qtd)


# 7 - média de Price em 2016 (PairRDD)
q7 = (transacoes
      .filter(lambda x: x[ANO] == "2016")
      .map(lambda x: (int(x[ANO]), float(x[PRECO])))
      .mapValues(lambda x: (x, 1))
      .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
      .mapValues(lambda x: x[0] / x[1])
      .collect())
registrar(7, "Média da coluna Price em 2016 (PairRDD)", q7)


# 8 - preço máximo e mínimo por ano e categoria
# o enunciado pede ordenação por país, mas o país não faz parte da chave
# pedida (ano, categoria), por isso a ordenação ficou pela própria chave
q8 = (transacoes
      .map(lambda x: ((int(x[ANO]), x[CATEGORIA].upper()), float(x[PRECO])))
      .mapValues(lambda x: (x, x))
      .reduceByKey(lambda a, b: (max(a[0], b[0]), min(a[1], b[1])))
      .sortByKey()
      .collect())
registrar(8, "Preço máximo e mínimo por ano e categoria", q8)


# 9 - país com a maior exportação, somando o valor exportado em USD
q9 = (transacoes
      .filter(lambda x: x[FLOW] == "Export")
      .map(lambda x: ((x[PAIS], x[FLOW]), float(x[PRECO])))
      .reduceByKey(lambda a, b: a + b)
      .sortBy(lambda x: x[1], ascending=False)
      .first())  # só o primeiro vai para o driver, em vez de trazer todos com collect
registrar(9, "País com a maior exportação (PairRDD)", q9)

transacoes.unpersist()


# 10 - transação de exportação com o maior preço por kg (Spark SQL)
# quote vazio porque há descrições com aspas soltas, que quebrariam as colunas
df = spark.read.csv(ARQUIVO, sep=";", header=True, quote="")

# sem peso, ou com peso zero, não existe preço por kg
exportacoes = (df
               .withColumn("trade_usd", F.col("trade_usd").cast("double"))
               .withColumn("weight_kg", F.col("weight_kg").cast("double"))
               .dropna(subset=["trade_usd", "weight_kg"])
               .filter((F.col("flow") == "Export") & (F.col("weight_kg") > 0)
                       & (F.col("comm_code") != "TOTAL"))  # mesmo critério das questões 1 a 9
               .withColumn("preco_kg", F.col("trade_usd") / F.col("weight_kg")))

maior_preco = exportacoes.agg(F.max("preco_kg")).first()[0]

q10 = (exportacoes
       .filter(F.col("preco_kg") == maior_preco)
       .select("year", "country_or_area", "category", "commodity",
               "trade_usd", "weight_kg", "preco_kg")
       .collect())
registrar(10, "Transação de exportação com o maior preço por kg (Spark SQL)",
          [linha.asDict() for linha in q10])

saida.close()
spark.stop()
