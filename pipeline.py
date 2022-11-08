import argparse
import json
import logging

import apache_beam as beam
import pandas as pd
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from smart_open import open
import os

#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'project-demo-1508-9f80e4c2c1c6.json'

""" sample json that we are going to be parsing through our program, store this in a gcs bucket and then
run this script which converts 
{
    "product": {
        "id": "1234567890",
        "title": "Awesome Product",
        "vendor": "Vendor Test",
        "product_type": "Test",
        "created_at": "2022-10-11T16:07:45-4:00",
        "updated_at": "2022-10-15T14:32:09-4:00",

    }
}
"""

class ReadFile(beam.DoFn):

    def __init__(self, input_path):
        self.input_path = input_path

    def start_bundle(self):
        from google.cloud import storage
        self.client = storage.Client()

    def process(self, element):
        datalist = []
        with open(self.input_path) as f:
            for line in f:
                data = json.loads(line)
        
                user_id = data.get('user_id')
                business_id = data.get('business_id')
                text = data.get('text').replace(",", ".")
                date = data.get('date')
                compliment_count = data.get('compliment_count')
                    
                datalist.append([user_id, business_id, text, date, compliment_count])
            
            yield datalist


"""
class ReadFile(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path

    def start_bundle(self):
        from google.cloud import storage
        self.client = storage.Client()

    def process(self, something):
        clear_data = []
        with open(self.input_path) as fin:
            ss=fin.read()
            data = json.loads(ss)
            product = data.get('product')
        
        
        if product and product.get('id'): #verifies if there exists a key/value pair
            product_id = product.get('id')
            vendor = product.get('vendor')
            product_type = product.get('product_type')
            updated_at = product.get('updated_at')
            created_at = product.get('created_at') 


            
            product_options = product.get('product_options')

            option_ids =[]

            if product_options: 
                for option in product_options:
                    option_ids.append(option.get('id'))

            clear_data.append([product_id, vendor, product_type, updated_at, created_at])

        yield clear_data """

class WriteCSVFile(beam.DoFn):

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def start_bundle(self):
        from google.cloud import storage
        self.client = storage.Client()

    def process(self, mylist):
        df = pd.DataFrame(mylist, columns={'user_id': str, 'business_id': str, 'text': str, 'date': str, 'compliment_count': str})

        bucket = self.client.get_bucket(self.bucket_name)

        bucket.blob(f"csv_output_big.csv").upload_from_string(df.to_csv(index=False), 'text/csv')

class DataflowOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls,parser):
        parser.add_argument('--input_path', type=str, default='gs://project_yelp_574839/test.json') #change bucket name here in the 'gs://...' format
        parser.add_argument('--output_bucket', type=str, default='project_yelp_574839') #change bucket name here in similar format to <--

def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    dataflow_options = pipeline_options.view_as(DataflowOptions)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (pipeline
        | 'Start' >> beam.Create([None])
        | 'Read JSON' >> beam.ParDo(ReadFile(dataflow_options.input_path))
        | 'Write CSV' >> beam.ParDo(WriteCSVFile(dataflow_options.output_bucket))
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
