import argparse
import logging
import json
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class ReadJsonDoFn(beam.DoFn):
    """Parse each line of the input file. And load as dictory, extract values"""
    def process(self, input):
        datalist = []
        dic = json.loads(input)
        for val in dic.values():
            datalist.append(str(val))
        yield datalist

def run(argv=None, save_main_session=True):
# Set the save_main_session=True because one or more DoFn's in this
# workflow rely on global context (e.g., a module imported at module level).
    parser = argparse.ArgumentParser()

    parser.add_argument(
      '--input',
      required=True,
      help='Input json file to process.')

    parser.add_argument(
      '--output',
      required=True,
      help='Output csv file.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    def my_replace(my_datalist):
        """find all the comma in the text column and repalce with a dot"""
        my_datalist[2] = my_datalist[2].replace(",", ".")
        return my_datalist

    with beam.Pipeline(options=pipeline_options) as p:
        (p 
        | 'ReadFile' >> ReadFromText(known_args.input)
        | 'ReadJson' >> beam.ParDo(ReadJsonDoFn())
        | 'ReplaceComma' >> beam.Map(my_replace)
        | 'WriteCSV' >> WriteToText(known_args.output)
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()


