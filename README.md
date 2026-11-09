# PjBL02 - Spark

Trabalho da disciplina de Big Data (BCC - PUCPR, prof. Joelton Gotz). Análise das transações comerciais internacionais usando Spark em Python, com RDD, PairRDD e Spark SQL.

- `pjbl02_spark.py`: código das 10 questões
- `resultado.txt`: resultado da execução na base completa

## Como executar

A base não está no repositório por causa do tamanho (1,2 GB). Coloque o arquivo em `data/operacoes_comerciais_inteira.csv`.

Precisa de Python 3.12 e Java instalado, com o `JAVA_HOME` configurado.

```
pip install -r requirements.txt
python pjbl02_spark.py
```

O arquivo `resultado.txt` é gerado de novo a cada execução.

## Decisões sobre os dados

- O cabeçalho é retirado com um `filter` no RDD e com `header=True` no DataFrame.
- Só existem valores vazios em `weight_kg` e `quantity`. As questões 1 a 9 não usam essas colunas, então essas linhas continuam valendo. Mesmo assim, o código descarta qualquer linha sem país, ano, flow, preço ou categoria.
- Na questão 10 ficam de fora as transações sem peso ou com peso zero, porque nelas não dá para calcular o preço por kg.
- Na questão 8 o enunciado pede ordenação por país, mas a chave de saída é (ano, categoria). O resultado foi ordenado por essa chave.
- Na questão 9, "maior exportação" foi considerada como a maior soma de `trade_usd` com flow igual a Export. O primeiro lugar é `EU-28`, que aparece na coluna de países da base, mas é um bloco. Considerando só países individuais, o primeiro seria a China.
