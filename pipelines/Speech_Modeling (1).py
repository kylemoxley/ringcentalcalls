# Databricks notebook source
import mlflow
import sparknlp
# import sparknlp_display 
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline
import pandas as pd
from sparknlp.training import CoNLL
import pyspark
from pyspark.sql import SparkSession
import types
from sparknlp.pretrained import PretrainedPipeline 


# COMMAND ----------

spark.sql("USE ringcentral")



comparision_df = spark.sql("SELECT * FROM 200_sample_final_comparison").drop("customer_text_azure","agent_text_azure")
display(comparision_df)

# COMMAND ----------

df = comparision_df.select('*').toPandas()
pd.DataFrame(df)
df.rename(columns = {"agent_text_open": "agent_text"},inplace=True)
df.rename(columns = {"customer_text_open": "customer_text"},inplace=True)
dfTest = spark.createDataFrame(df)
display(dfTest)

# COMMAND ----------

# DBTITLE 1, agent_text_open

entities = ['fit', 'size', 'length', 'height', 'huge', 'tiny', 'runs', 'short', 'long', 'loose', 'tight', 'Swimming']
with open ('/dbfs/temporary/kyles_pipeline_file/size_entities.txt', 'w+') as f:
    for i in entities:
        f.write(i+'\n')
entities = ['shiped', 'dhl', 'shipping', 'delivery', 'huge', 'tiny', 'runs', 'short', 'long', 'loose', 'tight', 'Swimming']
with open ('/dbfs/temporary/kyles_pipeline_file/ship_entities.txt', 'w+') as f:
    for i in entities:
        f.write(i+'\n')        
entities = ['available', 'out', 'stock']
with open ('/dbfs/temporary/kyles_pipeline_file/stock_entities.txt', 'w+') as f:
    for i in entities:
        f.write(i+'\n')
entities = ['website', 'description', 'errors', 'login', 'site']
with open ('/dbfs/temporary/kyles_pipeline_file/site_entities.txt', 'w+') as f:
    for i in entities:
        f.write(i+'\n') 
entities = ['refund', 'return','refunded']
with open ('/dbfs/temporary/kyles_pipeline_file/return_entities.txt', 'w+') as f:
    for i in entities:
        f.write(i+'\n')         
        
documentAssembler_a = DocumentAssembler()\
    .setInputCol("agent_text")\
    .setOutputCol("document_a")

tokenizer_a = Tokenizer() \
    .setInputCols(["document_a"]) \
    .setOutputCol("token_a")

sentence_a = SentenceDetector() \
  .setInputCols("document_a") \
  .setOutputCol("sentence_a")

embeddings_a = WordEmbeddingsModel.pretrained() \
  .setInputCols("sentence_a", "token_a") \
  .setOutputCol("bert_a")

nerTagger_a = NerDLModel.pretrained() \
  .setInputCols("sentence_a", "token_a", "bert_a") \
  .setOutputCol("ner_a")

normalizer_a = Normalizer() \
    .setInputCols(["token_a"]) \
    .setOutputCol("normal_a")

# lemmatizer = LemmatizerModel.pretrained('lemma_antbnc', 'en') \
#     .setInputCols(["normalizer"]) \
#     .setOutputCol("lemma")

# vivekn_a =  ViveknSentimentModel.pretrained() \
#     .setInputCols("document_a", "normal_a") \
#     .setOutputCol("result_sentiment_a")

use_a = UniversalSentenceEncoder.pretrained('tfhub_use', lang="en")\
  .setInputCols(["document_a"])\
  .setOutputCol("sentence_embeddings_a")


vivekn_a = SentimentDLModel().pretrained("sentimentdl_use_imdb")\
    .setInputCols(["sentence_embeddings_a"])\
    .setOutputCol("result_sentiment_a")

size_entity_extractor_a = TextMatcher() \
    .setInputCols("document_a",'token_a')\
    .setOutputCol("size_entities_a")\
    .setEntities("/temporary/kyles_pipeline_file/size_entities.txt")\
    .setCaseSensitive(False)
#     .setEntityValue('financial_entity')

ship_entity_extractor_a = TextMatcher() \
    .setInputCols(["document_a",'token_a'])\
    .setOutputCol("ship_entities_a")\
    .setEntities("/temporary/kyles_pipeline_file/ship_entities.txt")\
    .setCaseSensitive(False)#\
#    .setEntityValue('sport_entity')
stock_entity_extractor_a = TextMatcher() \
    .setInputCols(["document_a",'token_a'])\
    .setOutputCol("stock_entities_a")\
    .setEntities("/temporary/kyles_pipeline_file/stock_entities.txt")\
    .setCaseSensitive(False)
#     .setEntityValue('financial_entity')

# site_entity_extractor = TextMatcher() \
#     .setInputCols(["document",'token'])\
#     .setOutputCol("site_entities")\
#     .setEntities("/temporary/kyles_pipeline_file/site_entities.txt")\
#     .setCaseSensitive(False)\


# finisher_a= Finisher() \
#     .setInputCols(["ner_c","result_sentiment_c","size_entities_c","ship_entities_c", "stock_entities_c"]) \
#     .setOutputCols("prediction","prediction2","prediction3", "prediction4","prediction5")

# COMMAND ----------

# finisher = Finisher() \
#     .setInputCols(["ner_a","result_sentiment_a","size_entities_a","ship_entities_a", "stock_entities_a"]) \
#     .setOutputCols("prediction","prediction2","prediction3", "prediction4","prediction5")

# COMMAND ----------

# empty_df = spark.createDataFrame([['']]).toDF('agent_text')
# pipelineModel = nlpPipeline.fit(empty_df)

# COMMAND ----------

# result = pipelineModel.transform(dfTest.limit(10))
# result.show(5)

# COMMAND ----------

# nlpPipeline = Pipeline(stages=[

#   documentAssembler_a,
#   tokenizer_a,
#   sentence_a,
#   embeddings_a,
#   nerTagger_a,
#   normalizer_a,
#   use_a,
#   vivekn_a,
#   size_entity_extractor_a,
#   ship_entity_extractor_a,
#   stock_entity_extractor_a,
#   tokenizer_c,
#   sentence_c,
#   embeddings_c,
#   nerTagger_c,
#   normalizer_c,
#   use_c,
#   vivekn_c,
#   size_entity_extractor_c,
#   ship_entity_extractor_c,
#   stock_entity_extractor_c,
#   finisher
# ])

# COMMAND ----------

# DBTITLE 1,customer_text_open
entities = ['fit', 'size', 'length', 'height', 'huge', 'tiny', 'runs', 'short', 'long', 'loose', 'tight', 'Swimming']
with open ('/dbfs/temporary/kyles_pipeline_file/size_entities.txt', 'w+') as f:
    for i in entities:
        f.write(i+'\n')
entities = ['shiped', 'dhl', 'shipping', 'delivery', 'huge', 'tiny', 'runs', 'short', 'long', 'loose', 'tight', 'Swimming']
with open ('/dbfs/temporary/kyles_pipeline_file/ship_entities.txt', 'w+') as f:
    for i in entities:
        f.write(i+'\n')        
entities = ['available', 'out', 'stock']
with open ('/dbfs/temporary/kyles_pipeline_file/stock_entities.txt', 'w+') as f:
    for i in entities:
        f.write(i+'\n')
entities = ['website', 'description', 'errors', 'login', 'site']
with open ('/dbfs/temporary/kyles_pipeline_file/site_entities.txt', 'w+') as f:
    for i in entities:
        f.write(i+'\n') 
entities = ['refund', 'return','refunded']
with open ('/dbfs/temporary/kyles_pipeline_file/return_entities.txt', 'w+') as f:
    for i in entities:
        f.write(i+'\n')         
        
documentAssembler_c = DocumentAssembler()\
    .setInputCol("customer_text")\
    .setOutputCol("document_c")

tokenizer_c = Tokenizer() \
    .setInputCols(["document_c"]) \
    .setOutputCol("token_c")

sentence_c = SentenceDetector() \
  .setInputCols("document_c") \
  .setOutputCol("sentence_c")

embeddings_c = WordEmbeddingsModel.pretrained() \
  .setInputCols("sentence_c", "token_c") \
  .setOutputCol("bert_c")

nerTagger_c = NerDLModel.pretrained() \
  .setInputCols("sentence_c", "token_c", "bert_c") \
  .setOutputCol("ner_c")

normalizer_c = Normalizer() \
    .setInputCols(["token_c"]) \
    .setOutputCol("normal_c")

# lemmatizer = LemmatizerModel.pretrained('lemma_antbnc', 'en') \
#     .setInputCols(["normalizer"]) \
#     .setOutputCol("lemma")

use_c = UniversalSentenceEncoder.pretrained('tfhub_use', lang="en")\
  .setInputCols(["document_c"])\
  .setOutputCol("sentence_embeddings_c")


vivekn_c = SentimentDLModel().pretrained("sentimentdl_use_imdb")\
    .setInputCols(["sentence_embeddings_c"])\
    .setOutputCol("result_sentiment_c")

size_entity_extractor_c = TextMatcher() \
    .setInputCols(["document_c",'token_c'])\
    .setOutputCol("size_entities_c")\
    .setEntities("/temporary/kyles_pipeline_file/size_entities.txt")\
    .setCaseSensitive(False)
#     .setEntityValue('financial_entity')

ship_entity_extractor_c = TextMatcher() \
    .setInputCols(["document_c",'token_c'])\
    .setOutputCol("ship_entities_c")\
    .setEntities("/temporary/kyles_pipeline_file/ship_entities.txt")\
    .setCaseSensitive(False)\
#    .setEntityValue('sport_entity')
stock_entity_extractor_c = TextMatcher() \
    .setInputCols(["document_c",'token_c'])\
    .setOutputCol("stock_entities_c")\
    .setEntities("/temporary/kyles_pipeline_file/stock_entities.txt")\
    .setCaseSensitive(False)
#     .setEntityValue('financial_entity')

# site_entity_extractor = TextMatcher() \
#     .setInputCols(["document",'token'])\
#     .setOutputCol("site_entities")\
#     .setEntities("/temporary/kyles_pipeline_file/site_entities.txt")\
#     .setCaseSensitive(False)\

# finisher = Finisher() \
#     .setInputCols(["ner_a","result_sentiment_a","size_entities_a","ship_entities_a", "stock_entities_a"]) \
#     .setOutputCols("prediction","prediction2","prediction3", "prediction4","prediction5")
finisher_customer = Finisher() \
    .setInputCols(["ner_c",       "result_sentiment_c",       "size_entities_c",        "ship_entities_c",       "stock_entities_c"]) \
    .setOutputCols("ner_customer","result_sentiment_customer","size_entities_customer", "ship_entities_customer","stock_entities_customer")

finisher_agent = Finisher() \
    .setInputCols(["ner_a",    "result_sentiment_a",    "size_entities_a",     "ship_entities_a",    "stock_entities_a"]) \
    .setOutputCols("ner_agent","result_sentiment_agent","size_entities_agent", "ship_entities_agent","stock_entities_agent")
# finisher = Finisher() \
#     .setInputCols(["ner_c","result_sentiment_c","size_entities_c","ship_entities_c", "stock_entities_c","ner_a","result_sentiment_a","size_entities_a","ship_entities_a", "stock_entities_a"]) \
#     .setOutputCols("prediction","prediction2","prediction3", "prediction4","prediction5","prediction6","prediction7","prediction8", "prediction9","prediction10")

# nlpPipeline = Pipeline(stages=[
#   documentAssembler,
#   tokenizer,
#   sentence,
#   embeddings,
#   nerTagger,
#   normalizer,
#   vivekn,
#   size_entity_extractor,
#   ship_entity_extractor,
#   finisher
# ])

# COMMAND ----------

nlpPipeline_customer = Pipeline(stages=[

#   documentAssembler_a,
#   tokenizer_a,
#   sentence_a,
#   embeddings_a,
#   nerTagger_a,
#   normalizer_a,
#   use_a,
#   vivekn_a,
#   size_entity_extractor_a,
#   ship_entity_extractor_a,
#   stock_entity_extractor_a,
  documentAssembler_c,
  sentence_c,
  tokenizer_c,
  embeddings_c,
  nerTagger_c,
  normalizer_c,
  use_c,
  vivekn_c,
  size_entity_extractor_c,
  ship_entity_extractor_c,
  stock_entity_extractor_c,
  finisher_customer
])

# COMMAND ----------

nlpPipeline_agent = Pipeline(stages=[

  documentAssembler_a,
  sentence_a,
  tokenizer_a,
  embeddings_a,
  nerTagger_a,
  normalizer_a,
  use_a,
  vivekn_a,
  size_entity_extractor_a,
  ship_entity_extractor_a,
  stock_entity_extractor_a,
  finisher_agent
])

# COMMAND ----------

empty_df = spark.createDataFrame([['']]).toDF("customer_text")
pipelineModel_customer = nlpPipeline_customer.fit(empty_df)
# empty_df = spark.createDataFrame([['','']]).toDF("customer_text",'agent_text')
# pipelineModel = nlpPipeline.fit(empty_df)

# COMMAND ----------

empty_df = spark.createDataFrame([['']]).toDF("agent_text")
pipelineModel_agent = nlpPipeline_agent.fit(empty_df)
# empty_df = spark.createDataFrame([['','']]).toDF("customer_text",'agent_text')
# pipelineModel = nlpPipeline.fit(empty_df)

# COMMAND ----------

result_customer = pipelineModel_customer.transform(dfTest.limit(10))
display(result_customer)#.show(5)

# COMMAND ----------

result_agent = pipelineModel_agent.transform(dfTest.limit(10))
display(result_agent)#.show(5)

# COMMAND ----------

pipelineModel_customer.write().overwrite().save("/temporary/speech_modeling/customer_model/")

# COMMAND ----------

pipelineModel_agent.write().overwrite().save("/temporary/speech_modeling/agent_model/")

# COMMAND ----------


