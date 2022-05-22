import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'lab3-341009' # Enter your project ID
google_cloud_options.job_name = 'q2lab3'
google_cloud_options.temp_location = "gs://me18b183_l1/tmp2"
google_cloud_options.region = "us-central1"
options.view_as(StandardOptions).runner = 'DataflowRunner'
output_file = 'gs://me18b183_l1/lab3assignment/lab3_q2.txt'
p = beam.Pipeline(options=options)

read = p | 'Read' >> beam.io.ReadFromText('gs://bdl2022/lines_big.txt')

class WordsInALine(beam.DoFn):
        def process(self, text):
                yield len(text.split())

#Assuming that the lines in the files_big.txt are well puntuated.
 
avg_word = read | 'Words in a line' >> beam.ParDo(WordsInALine()) 
                | 'Mean' >> beam.combiners.Mean.Globally() 
                | 'Write' >> beam.io.WriteToText(output_file)
result = p.run()

