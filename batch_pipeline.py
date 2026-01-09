import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class ParseSales(beam.DoFn):
    def process(self, element):
        yield {
            "order_id": element["order_id"],
            "customer_id": element["customer_id"],
            "product": element["product"],
            "amount": float(element["amount"]),
            "order_timestamp": element["order_timestamp"]
        }

options = PipelineOptions(
    runner="DataflowRunner",
    project="YOUR_GCP_PROJECT",
    region="us-central1",
    temp_location="gs://YOUR_BUCKET/temp"
)

with beam.Pipeline(options=options) as p:
    (
        p
        | "ReadFromGCS" >> beam.io.ReadFromText(
            "gs://YOUR_BUCKET/raw/sales.json"
        )
        | "ParseJSON" >> beam.Map(lambda x: eval(x))
        | "Transform" >> beam.ParDo(ParseSales())
        | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            "enterprise_dw.fact_sales",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
