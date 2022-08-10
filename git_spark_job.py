#importing the required packages
import os
import sys
import re
import pyspark
from pyspark.sql.types import DecimalType,StringType
from pyspark.sql.functions import concat_ws,col,sha2
from pyspark.sql import functions as f
import json
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('app1').getOrCreate()
jar_path=sys.argv[1]
spark.sparkContext.addPyFile(jar_path)
from delta import *
class Configure:

    #To read the configuration file
    def read_config(self):
        app_config = spark.conf.get("spark.path")
        configData = spark.sparkContext.textFile(app_config).collect()
        data       = ''.join(configData)
        jsonData = json.loads(data)
        return jsonData

    def __init__(self):
        self.jsonData=self.read_config()
        # Assigning variables for Configuration File Parameters
        self.ingest_datasets = self.jsonData['datasets']
        self.ingest_actives_source = self.jsonData['ingest-Actives']['source']['data-location']
        self.ingest_actives_destination = self.jsonData['ingest-Actives']['destination']['data-location']
        self.ingest_viewership_source = self.jsonData['ingest-Viewership']['source']['data-location']
        self.ingest_viewership_destination = self.jsonData['ingest-Viewership']['destination']['data-location']
        self.transformation_cols_actives = self.jsonData['masked-Actives']['transformation-cols']
        self.transformation_cols_viewership = self.jsonData['masked-Viewership']['transformation-cols']
        self.ingest_raw_actives_destination = self.jsonData['masked-Actives']['destination']['data-location']
        self.ingest_raw_actives_source = self.jsonData['masked-Actives']['source']['data-location']
        self.ingest_raw_viewership_source = self.jsonData['masked-Viewership']['source']['data-location']
        self.masking_col_actives= self.jsonData['masked-Actives']['masking-cols']
        self.masking_col_viewership= self.jsonData['masked-Viewership']['masking-cols']
        self.lookup_location = self.jsonData['lookup-dataset']['data-location']
        self.pii_cols =  self.jsonData['lookup-dataset']['pii-cols']
        

class Transform(Configure):
    #Function to read the data from the source
    def to_read_data(self,path,format):
        df=spark.read.parquet(path)
        return df

    #Function to write the data to the destination
    def to_write_data(self,df,path,format,partition_columns=[]):
        if partition_columns:
            df.write.mode("overwrite").partitionBy(partition_columns[0],partition_columns[1]).parquet(path)
        else:
            df.write.mode("overwrite").parquet(path)

    #Fetching the columns for Actives transformation
    def columns_for_transformation_actives(self):
        #Fetching columns for transformation in actives dataset
        Transform.actives_cast_columns = self.jsonData['masked-Actives']['transformation-cols']
        Transform.actives_mask_columns = self.jsonData['masked-Actives']['masking-cols']
        #Fetching the partition columns
        self.active_partition_columns = self.jsonData['masked-Actives']['partition-cols']
    
    #Fetching the columns for viewership transformation
    def columns_for_transformation_viewership(self):
        #Fetching columns for transformation in viewership dataset
        Transform.viewership_cast_columns = self.jsonData['masked-Viewership']['transformation-cols']
        Transform.viewership_mask_columns = self.jsonData['masked-Viewership']['masking-cols']
        self.viewership_partition_columns = self.jsonData['masked-Viewership']['partition-cols']

    #Function to perform the casting operation
    def transformation_data(self,df,cast_dict):
        items_list = []
        for item in cast_dict.keys():
            items_list.append(item)

        for column in items_list:
            if cast_dict[column].split(",")[0] == "DecimalType":
                df = df.withColumn(column,df[column].cast(DecimalType(scale=int(cast_dict[column].split(",")[1]))))
            elif cast_dict[column] == "ArrayType-StringType":
                df = df.withColumn(column,f.concat_ws(",",'city','state','location_category'))
        return df

    #Funstion to perform the masking operation
    def mask_data(self,df,column_list):
        for column in column_list:
            df = df.withColumn("masked_"+column,f.sha2(f.col(column),256))
        return df

    #To write the data to staging zone
    def raw_to_staging_actives(self,masked_actives):
        #Fetching the destination path and file format of actives dataset required to put into staging zone
        active_destination = self.jsonData['masked-Actives']['destination']['data-location']
        active_format = self.jsonData['masked-Actives']['destination']['file-format']
        self.to_write_data(masked_actives,active_destination,active_format,self.active_partition_columns) # write active data to stagingzone
        
    def raw_to_staging_viewership(self,masked_viewership):
        #Fetching the destination path and file format of viewership dataset required to put into staging zone
        viewership_destination = self.jsonData['masked-Viewership']['destination']['data-location']
        viewership_format = self.jsonData['masked-Viewership']['destination']['file-format']
        self.to_write_data(masked_viewership,viewership_destination,viewership_format,self.viewership_partition_columns) # write viewership data to stagingzone
    def scd2_implementaion(self, df, lookup_location, pii_cols): 
        file_name = "Actives"
        df_source = df.withColumn("start_date",f.current_date())
        df_source = df_source.withColumn("end_date",f.lit("null"))
        
        # getting required (masked and unmasked pii columns) from a dataset for delta table
        source_columns = []
        for col in pii_cols:
            if col in df.columns:
                source_columns += [col,"masked_"+col]
        
        # adding start_date, end_date columns to delta table for lookup
        source_columns_used = source_columns + ['start_date','end_date']        
        df_source = df_source.select(*source_columns_used)
            
        try:
            targetTable = DeltaTable.forPath(spark, lookup_location + file_name)
            delta_df = targetTable.toDF()
        except pyspark.sql.utils.AnalysisException:
            print('Table does not exist')
            df_source = df_source.withColumn("flag_active",f.lit("true"))
            df_source.write.format("delta").mode("overwrite").save(lookup_location + file_name)
            print('Table Created Sucessfully!')
            targetTable = DeltaTable.forPath(spark, lookup_location + file_name)
            delta_df = targetTable.toDF()
            delta_df.show(10)
            
        delta_columns = [i for i in delta_df.columns if i not in ['start_date', 'end_date', 'flag_active']]        
        delta_df = delta_df.select(*(f.col(i).alias('target_' + i) for i in delta_df.columns))        
        join_condition = (df_source.advertising_id == delta_df.target_advertising_id) | (df_source.user_id == delta_df.target_user_id)
        join_df = df_source.join(delta_df, join_condition, "leftouter").select(df_source['*'], delta_df['*'])
        
        
        new_data = join_df.filter("target_user_id is null")
        filter_df = join_df.filter(join_df.user_id != join_df.target_user_id)
        filter_df = filter_df.union(new_data)
        
        if filter_df.count() != 0:
            mergeDf = filter_df.withColumn("MERGEKEY", f.concat(filter_df.advertising_id, filter_df.target_user_id))
            dummyDf = filter_df.filter("target_advertising_id is not null").withColumn("MERGEKEY", f.lit(None))

            scdDF = mergeDf.union(dummyDf)

            Insertable = {i: "source." + i for i in delta_columns if i}
            Insertable.update({"start_date": "current_date", "end_date": "null", "flag_active": "True"})

            targetTable.alias("target").merge(
                source=scdDF.alias("source"),
                condition="concat(target.advertising_id, target.user_id) = source.MERGEKEY and target.flag_active = 'true'"
            ).whenMatchedUpdate(set={
                "end_date": "current_date",
                "flag_active": "False",
            }).whenNotMatchedInsert(values=Insertable
            ).execute()
            
        for i in pii_cols:
            df = df.drop(i).withColumnRenamed("masked_"+i, i)

        return df
        
confObj=Configure()
transObj=Transform()

for dataset in confObj.ingest_datasets:
    if dataset == "Actives":

        #To read the data of actives and viewership files from landing zone
        actives_data= transObj.to_read_data(confObj.ingest_actives_source,"parquet")
        #To write the actives and viewership files data to raw zone
        transObj.to_write_data(actives_data,confObj.ingest_actives_destination,"parquet")
        #Fetching the columns required for transformation
        transObj.columns_for_transformation_actives()
        #To read the data of actives and viewership files from raw  zone
        actives_rawzone_data=transObj.to_read_data(confObj.ingest_raw_actives_source ,"parquet")
        actives_rawzone_data=actives_rawzone_data.withColumn('user_id',actives_rawzone_data['user_id'].cast(StringType()))
        # masking some fields from actives files
        actives_masked_data = transObj.mask_data(actives_rawzone_data, transObj.masking_col_actives)
        #Casting fields in actives and viewership files
        actives_stagingzone_data=transObj.transformation_data(actives_masked_data,confObj.transformation_cols_actives)
        #scd2 implementation
        lookup_data = transObj.scd2_implementaion( actives_stagingzone_data,confObj.lookup_location,confObj.pii_cols)
        # write actives Transformed data to Staging Zone
        transObj.raw_to_staging_actives(lookup_data)
        
        
    elif dataset == "Viewership":
        #To read the data of actives and viewership files from landing zone
        viewership_data=transObj.to_read_data(confObj.ingest_viewership_source,"parquet")
        #To write the actives and viewership files data to raw zone
        transObj.to_write_data(viewership_data,confObj.ingest_viewership_destination,"parquet")
        #Fetching the columns required for transformation
        transObj.columns_for_transformation_viewership()
        #To read the data of actives and viewership files from raw  zone
        viewership_rawzone_data=transObj.to_read_data(confObj.ingest_raw_viewership_source ,"parquet")
        #Casting fields in actives and viewership files
        viewership_stagingzone_data=transObj.transformation_data(viewership_rawzone_data,confObj.transformation_cols_viewership)
        #Masking on actives and viewership files
        masked_viewership=transObj.mask_data(viewership_stagingzone_data,confObj.masking_col_viewership)
        #Masking on actives and viewership files
        transObj.raw_to_staging_viewership(masked_viewership)
    else:
        pass

spark.stop()
