import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'lab3-341009' # Enter your project ID
google_cloud_options.job_name = 'q1lab3'
google_cloud_options.temp_location = "gs://me18b183_l1/lab3assignment/tmp"
google_cloud_options.region = "us-central1"
options.view_as(StandardOptions).runner = 'DataflowRunner'
p = beam.Pipeline(options=options)
lines = p | 'Read' >> beam.io.ReadFromText('gs://bdl2022/lines_big.txt') 
          | 'Line Count' >> beam.combiners.Count.Globally() 
          | 'Write' >> beam.io.WriteToText('gs://me18b183_l1/lab3assignment/lab3_q1.txt')
result = p.run()

