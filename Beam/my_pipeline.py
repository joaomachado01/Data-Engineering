import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions
import argparse
from datetime import datetime
import json

### functions
class ExtractFields(beam.DoFn):
    def process(self, element):
        data = json.loads(element)

        # Get the current timestamp
        current_timestamp = datetime.now()
        current_timestamp_bq_format = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')

        yield {
            "ingestion_time": current_timestamp_bq_format,
            "response_enabler": data.get("labels", {}).get("response_enabler", ""),
            "response_healthcheck": data.get("labels", {}).get("response_healthcheck", ""),
            "response_status": data.get("labels", {}).get("response_status", ""),
            "response_time": data.get("labels", {}).get("response_time", "")
        }

parser = argparse.ArgumentParser()

parser.add_argument('--input',
                      dest='input',
                      required=True,
                      help='Input file to process.')
parser.add_argument('--table',
                      dest='table',
                      required=True,
                      help='Output file to write results to.')


path_args, pipeline_args = parser.parse_known_args()

inputs_pattern = path_args.input
table_prefix = path_args.table


options = PipelineOptions(pipeline_args)
options.view_as(SetupOptions).save_main_session = True ## this is required for datetime module inside function
p = beam.Pipeline(options=options)

attendance_count =   (
                          p
                          | 'Read json' >> beam.io.ReadFromText(inputs_pattern)
                          | 'Process lines in json' >> beam.ParDo(ExtractFields())
                          | 'Write to Bigquery' >> beam.io.WriteToBigQuery(
                              table_prefix,
                              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                              write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
                      )

p.run()
