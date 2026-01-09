import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

options = PipelineOptions(
    project="YOUR_GCP_PROJECT",
    region="us-central1",
    temp_location="gs://YOUR_BUCKET/temp"
)
options.view_as(StandardOptions).streaming = True

with beam.Pipeline(options=options) as p:
    (
        p
        | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
            subscription="projects/YOUR_PROJECT/subscriptions/sales-sub"
        )
        | "Parse" >> beam.Map(lambda x: eval(x.decode("utf-8")))
        | "WriteToBQ" >> beam.io.WriteToBigQuery(
            "enterprise_dw.fact_sales",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
