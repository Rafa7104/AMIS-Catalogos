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

def replace_keys_state(row):
  new_key_assign = { 'ENTIDAD_ID' : 'id_estado', 'id_pais': 'id_pais', 'ENTIDAD_DESC' : 'descripcion', 'ESTATUS_ID' : 'estatus_estado' }
  new_row = dict([(new_key_assign.get(key), value) for key, value in row.items()])
  return new_row

def replace_keys_municipality(row):
  new_key_assign = { 'MUNICIPIO_ID' : 'id_municipio', 'ENTIDAD_ID' : 'id_estado', 'MUNICIPIO_DESC' : 'descripcion', 'ESTATUS_ID' : 'estatus_municipio' }
  new_row = dict([(new_key_assign.get(key), value) for key, value in row.items()])
  return new_row

def replace_keys_postal_code(row):
  new_key_assign = { 'CODIGO_POSTAL_ID': 'id_codigo_postal', 'MUNICIPIO_ID' : 'id_municipio', 'CODIGO_POSTAL_DESC' : 'numero_codigo_postal', 'ESTATUS_ID' : 'estatus_codigo_postal' }
  new_row = dict([(new_key_assign.get(key), value) for key, value in row.items()])
  return new_row
  
def replace_keys_suburb(row):
  new_key_assign = { 'COLONIA_ID': 'id_colonia', 'CODIGO_POSTAL_ID': 'id_codigo_postal', 'COLONIA_DESC' : 'descripcion', 'ESTATUS_ID' : 'estatus_colonia' }
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

  source = 'gs://'+bucnam+'/catalogos/cat_edo_mun_cp_col*.json'

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
  
# States dataframe 
  dfs = dfx[['ENTIDAD_ID', 'ENTIDAD_DESC', 'ESTATUS_ID']].astype(str).drop_duplicates()
  dfx['id_pais'] = '118'
  dfsp = dfx[['ENTIDAD_ID', 'id_pais', 'ENTIDAD_DESC', 'ESTATUS_ID']]
  
# Municipalities dataframe
  dfm = dfx[['MUNICIPIO_ID', 'ENTIDAD_ID', 'MUNICIPIO_DESC', 'ESTATUS_ID']].astype(str).drop_duplicates()  

# Postal codes dataframe
  dfp = dfx[['CODIGO_POSTAL_ID', 'MUNICIPIO_ID', 'CODIGO_POSTAL_DESC', 'ESTATUS_ID']].astype(str).drop_duplicates()  

# Suburbs dataframe
  dfsb = dfx[['COLONIA_ID', 'CODIGO_POSTAL_ID', 'COLONIA_DESC', 'ESTATUS_ID']].astype(str).drop_duplicates() 
  
  reg = dfx[['ID']].count()

# States rows
  rowss = dfsp.to_dict('records')
  
# Municipalities rows
  rowsm = dfm.to_dict('records')
  
# Postal codes rows
  rowsp = dfp.to_dict('records')
  
# Suburbs rows
  rowsb = dfsb.to_dict('records')

  source_config = relational_db.SourceConfiguration(
    drivername='postgresql+pg8000',  #postgresql+pg8000
    host=dirip,
    port=5432,
    username='ocra_admin',
    password='ocra_admin',
    database='amis-ocra-db',
    create_if_missing=True  # create the database if not there 
  )

  table_config_states = relational_db.TableConfiguration(
    name='cat_estado',
    create_if_missing=True,
    primary_key_columns=['id_estado']
  )

  table_config_municipalities = relational_db.TableConfiguration(
    name='cat_municipio',
    create_if_missing=True,
    primary_key_columns=['id_municipio']
  )

  table_config_postal_codes = relational_db.TableConfiguration(
    name='cat_codigo_postal',
    create_if_missing=True,
    primary_key_columns=['id_codigo_postal']
  )

  table_config_suburbs = relational_db.TableConfiguration(
    name='cat_colonia',
    create_if_missing=True,
    primary_key_columns=['id_colonia']
  )

  with beam.Pipeline(options=PipelineOptions(pipeline_args)) as ps:
    rps = (ps | "Reading records" >> beam.Create(rowss)
            | 'Replace status value' >> beam.Map(replace_status)
            | 'New keys to PG' >> beam.Map(replace_keys_state)
            | 'Writing to DB table' >> relational_db.Write(source_config=source_config,table_config=table_config_states))

  with beam.Pipeline(options=PipelineOptions(pipeline_args)) as pm:
    rpm = (pm | "Reading records" >> beam.Create(rowsm)
            | 'Replace status value' >> beam.Map(replace_status)
            | 'New keys to PG' >> beam.Map(replace_keys_municipality)
            | 'Writing to DB table' >> relational_db.Write(source_config=source_config,table_config=table_config_municipalities))

  with beam.Pipeline(options=PipelineOptions(pipeline_args)) as pp:
    rpp = (pp | "Reading records" >> beam.Create(rowsp)
            | 'Replace status value' >> beam.Map(replace_status)
            | 'New keys to PG' >> beam.Map(replace_keys_postal_code)
            | 'Writing to DB table' >> relational_db.Write(source_config=source_config,table_config=table_config_postal_codes))

  with beam.Pipeline(options=PipelineOptions(pipeline_args)) as pb:
    rpb = (pb | "Reading records" >> beam.Create(rowsb)
            | 'Replace status value' >> beam.Map(replace_status)
            | 'New keys to PG' >> beam.Map(replace_keys_suburb)
            | 'Writing to DB table' >> relational_db.Write(source_config=source_config,table_config=table_config_suburbs))
            
if __name__ == "__main__":
    run()
    print('Save in PostgresSql')
