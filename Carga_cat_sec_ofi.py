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

def replace_keys_sector(row):
  new_key_assign = { 'SECTOR_ID' : 'id_sector', 'SECTOR_DESC' : 'descripcion', 'ESTATUS_ID' : 'estatus_sector' }
  new_row = dict([(new_key_assign.get(key), value) for key, value in row.items()])
  return new_row

def replace_keys_office(row):
  new_key_assign = { 'OFICINAS_ID' : 'id_oficina_sector', 'SECTOR_ID' : 'id_sector', 'OFICINAS_DESC' : 'descripcion', 'ESTATUS_ID' : 'estatus_oficina_sector' }
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

  source = 'gs://'+bucnam+'/catalogos/cat_sector_oficina.json'

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
  
# Sectors dataframe 
  dfs = dfx[['SECTOR_ID', 'SECTOR_DESC', 'ESTATUS_ID']].astype(str).drop_duplicates()
  
# Offices dataframe
  dfo = dfx[['OFICINAS_ID', 'SECTOR_ID', 'OFICINAS_DESC', 'ESTATUS_ID']].astype(str).drop_duplicates()  
  
  reg = dfx[['ID']].count()

# Transport types rows
  rowss = dfs.to_dict('records')
  
# Brands rows
  rowso = dfo.to_dict('records')

  source_config = relational_db.SourceConfiguration(
    drivername='postgresql+pg8000',  #postgresql+pg8000
    host=dirip,
    port=5432,
    username='ocra_admin',
    password='ocra_admin',
    database='amis-ocra-db',
    create_if_missing=True  # create the database if not there 
  )

  table_config_sectors = relational_db.TableConfiguration(
    name='cat_sector',
    create_if_missing=True,
    primary_key_columns=['id_sector']
  )

  table_config_offices = relational_db.TableConfiguration(
    name='cat_oficina_sector',
    create_if_missing=True,
    primary_key_columns=['id_oficina_sector']
  )

  with beam.Pipeline(options=PipelineOptions(pipeline_args)) as ps:
    rps = (ps | "Reading records" >> beam.Create(rowss)
            | 'Replace status value' >> beam.Map(replace_status)
            | 'New keys to PG' >> beam.Map(replace_keys_sector)
            | 'Writing to DB table' >> relational_db.Write(source_config=source_config,table_config=table_config_sectors))

  with beam.Pipeline(options=PipelineOptions(pipeline_args)) as po:
    rpo = (po | "Reading records" >> beam.Create(rowso)
            | 'Replace status value' >> beam.Map(replace_status)
            | 'New keys to PG' >> beam.Map(replace_keys_office)
            | 'Writing to DB table' >> relational_db.Write(source_config=source_config,table_config=table_config_offices))
            
if __name__ == "__main__":
    run()
    print('Save in PostgresSql')
