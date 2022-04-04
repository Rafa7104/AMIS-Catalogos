#!/usr/bin/env python
# coding: utf-8

import googleapiclient.discovery
import argparse
import apache_beam as beam
import pandas as pd
import apache_beam.runners.interactive.interactive_beam as ib
from apache_beam.options.pipeline_options import PipelineOptions
from beam_nuggets.io import relational_db
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
from google.cloud import storage

# Instantiates a client
storage_client = storage.Client()

# List all the buckets available
for bucket in storage_client.list_buckets():  
  if bucket.name.startswith('amis-ocra-bucket-etl'):
    bucnam=bucket.name

def replace_status(row):
  if row['ESTATUS_ID'] == 'ACTIVO':
    row['ESTATUS_ID'] = True
  else:
    row['ESTATUS_ID'] = False
  return row

def replace_keys_transport_type(row):
  new_key_assign = { 'TIPO_VEHICULO_ID' : 'id_tipo_transporte', 'TIPO_VEHICULO_DESC' : 'descripcion', 'ESTATUS_ID' : 'estatus_tipo_transporte' }
  new_row = dict([(new_key_assign.get(key), value) for key, value in row.items()])
  return new_row

def replace_keys_brand(row):
  new_key_assign = { 'MARCAS_ID' : 'id_marca', 'TIPO_VEHICULO_ID' : 'id_tipo_transporte', 'MARCAS_DESC' : 'descripcion', 'ESTATUS_ID' : 'estatus_marca' }
  new_row = dict([(new_key_assign.get(key), value) for key, value in row.items()])
  return new_row

def replace_keys_sub_brand(row):
  new_key_assign = { 'TIPOS_ID': 'id_submarca', 'MARCAS_ID' : 'id_marca', 'TIPOS_DESC' : 'descripcion', 'ESTATUS_ID' : 'estatus_submarca' }
  new_row = dict([(new_key_assign.get(key), value) for key, value in row.items()])
  return new_row
  

def run(argv=None):
  parser = argparse.ArgumentParser()

#  parser.add_argument(
#        '--bucket',
#        dest='bucket',
#        required=False,
#        help='Input file to read. This can be a local file or '
#        'a file in a Google Storage Bucket.',
#        # This example file contains a total of only 10 lines.
#        # Useful for developing on a small set of data.
#        default='gs://poc_amis')

  known_args, pipeline_args = parser.parse_known_args(argv)
  
  params=PipelineOptions(pipeline_args).get_all_options()
  proj=params['project']
  
  sqladmin = googleapiclient.discovery.build('sqladmin', 'v1beta4')
  response = sqladmin.connect().get(project=proj,instance='amis-ocra-sql').execute()
  for dir in response['ipAddresses']:  
    if dir['type'] == 'PRIMARY':
      dirip=dir['ipAddress']
  sqladmin.close()

  source = 'gs://'+bucnam+'/catalogos/cat_ttr_mar_sub.json'

  pipeline = beam.Pipeline(InteractiveRunner())

# Create a deferred Beam DataFrame with the contents of our json file.
  json_df = pipeline | 'Read JSON' >> beam.dataframe.io.read_json(source)

# We can use `ib.collect` to view the contents of a Beam DataFrame.
#ib.collect(json_df)
# Collect the Beam DataFrame into a Pandas DataFrame.
  df = ib.collect(json_df)


# We can now use any Pandas transforms with our data.
#pd.options.display.max_columns = None
#pd.options.display.max_rows = 10
  dfx = pd.DataFrame(df)
  
# Transport types dataframe 
  dft = dfx[['TIPO_VEHICULO_ID', 'TIPO_VEHICULO_DESC', 'ESTATUS_ID']].astype(str).drop_duplicates()
  
# Brands dataframe
  dfm = dfx[['MARCAS_ID', 'TIPO_VEHICULO_ID', 'MARCAS_DESC', 'ESTATUS_ID']].astype(str).drop_duplicates()  

# Sub brands dataframe
  dfs = dfx[['TIPOS_ID', 'MARCAS_ID', 'TIPOS_DESC', 'ESTATUS_ID']].astype(str).drop_duplicates()  
  
  reg = dfx[['ID']].count()

# Transport types rows
  rowst = dft.to_dict('records')
  
# Brands rows
  rowsm = dfm.to_dict('records')
  
# Sub brands rows
  rowss = dfs.to_dict('records')

  source_config = relational_db.SourceConfiguration(
    drivername='postgresql+pg8000',  #postgresql+pg8000
    host=dirip,
    port=5432,
    username='ocra_admin',
    password='ocra_admin',
    database='amis-ocra-db',
    create_if_missing=True  # create the database if not there 
  )

  table_config_transport_types = relational_db.TableConfiguration(
    name='cat_tipo_transporte',
    create_if_missing=True,
    primary_key_columns=['id_tipo_transporte']
  )

  table_config_brands = relational_db.TableConfiguration(
    name='cat_marca',
    create_if_missing=True,
    primary_key_columns=['id_marca']
  )

  table_config_sub_brands = relational_db.TableConfiguration(
    name='cat_submarca',
    create_if_missing=True,
    primary_key_columns=['id_submarca']
  )

  with beam.Pipeline(options=PipelineOptions(pipeline_args)) as pt:
    rpt = (pt | "Reading records" >> beam.Create(rowst)
            | 'Replace status value' >> beam.Map(replace_status)
            | 'New keys to PG' >> beam.Map(replace_keys_transport_type)
            | 'Writing to DB table' >> relational_db.Write(source_config=source_config,table_config=table_config_transport_types))

  with beam.Pipeline(options=PipelineOptions(pipeline_args)) as pm:
    rpm = (pm | "Reading records" >> beam.Create(rowsm)
            | 'Replace status value' >> beam.Map(replace_status)
            | 'New keys to PG' >> beam.Map(replace_keys_brand)
            | 'Writing to DB table' >> relational_db.Write(source_config=source_config,table_config=table_config_brands))

  with beam.Pipeline(options=PipelineOptions(pipeline_args)) as ps:
    rps = (ps | "Reading records" >> beam.Create(rowss)
            | 'Replace status value' >> beam.Map(replace_status)
            | 'New keys to PG' >> beam.Map(replace_keys_sub_brand)
            | 'Writing to DB table' >> relational_db.Write(source_config=source_config,table_config=table_config_sub_brands))
            
if __name__ == "__main__":
    run()
    print('Save in PostgresSql')
