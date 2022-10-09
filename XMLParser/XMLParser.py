# Databricks notebook source
import re
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from datetime import datetime
from datetime import timedelta
from pyspark.sql.functions import col
import pyspark.sql.types as T 
import time
from datetime import datetime as dt

# COMMAND ----------

class quinticPlanXmlData:
  def __init__(self):
    self.rowTag = 'ASDA:Route'
    self.filepath = 'dbfs:/FileStore/Plan.xml'
    self.outputpath = 'dbfs:/FileStore/Plan_out'

  def getXmlExtracts(self, rowTag, filepath):
    return spark.read.format("com.databricks.spark.xml").option("rowTag", rowTag).load(filepath)   

  def getColumnList(self, df, src_datatype = None):
    colsTypeList = []
    for colu, data in df.dtypes:
      if(src_datatype != None):
        if(re.search(src_datatype, data[:len(src_datatype)])):
          colsTypeList.append(colu)
      else:
        colsTypeList.append(colu)

    return colsTypeList 

  def flattenStructSchema(self, df):
    newColumns, submittedTables = {}, {}
    colsToDrop, tablesToParse = [], []

    #Loop through the columns and their datatypes to identify any columns that need to be further parsed
    for colu, data in df.dtypes:
      #If it is a struct, remember it so we can parse it
      if(re.search('struct', data[:len('struct')])):
        colsToDrop.append(colu)
        schema = T._parse_datatype_string(data)
        for i in schema:
          if(isinstance(i.dataType, T.StructType)):
            #Nested struct - Check what we can flatten
            for nest in i.dataType:
              if(isinstance(nest.dataType, T.StructType)):
                #nested node - Grab
                newColumns[f"{colu}.{i.name}.{nest.name}"] = f"{colu}_{i.name}_{nest.name}"
              else:
                #Value - grab
                newColumns[f"{colu}.{i.name}.{nest.name}"] = f"{colu}_{i.name}_{nest.name}"
          else:
            #another value
            newColumns[F"{colu}.{i.name}"] = F"{colu}_{i.name}"

    #Create the new table
    for column in newColumns:
        df = df.withColumn(newColumns[column], col(column))
    for drop in colsToDrop:
        df = df.drop(col(drop))  

    return df

  def flatten_process(self, df):
    flatten_done = True
    array_fields = []
    array_fields = self.getColumnList(df, "array")
    
    for colu in array_fields:
      df = df.withColumn(colu, explode_outer(col(colu)))
      
    df = self.flattenStructSchema(df)

    for tab, data in df.dtypes:
      #check if there is a struct type to further parse.
      if(re.search('struct', data[:len('struct')]) or re.search('array', data[:len('array')])):
        flatten_done = False

    return df, flatten_done

  def xmldataflatten(self, df):
    flatten_done = False
    while(not flatten_done):
        df, flatten_done = self.flatten_process(df)   
    return df
  
  def saveflattenData(self, df, outputpath):
    df.write.mode("overwrite").parquet(outputpath)
 
  def run(self):
    xmlDF = self.getXmlExtracts(self.rowTag, self.filepath) 
    xmldataflatten = self.xmldataflatten(xmlDF)
    self.saveflattenData(xmldataflatten, self.outputpath)
    display(xmldataflatten)

# COMMAND ----------

aa = quinticPlanXmlData()
aa.run()
