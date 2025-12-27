import apache_beam as beam
from apache_beam.metrics import Metrics
import logging
from apache_beam.options.pipeline_options import PipelineOptions
import argparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RetailSalesPipeline")

class ValidateAndTransform(beam.DoFn):
    def __init__(self):
        self.invalid_records = Metrics.counter(self.__class__, 'invalid_records')
        self.valid_records = Metrics.counter(self.__class__, 'valid_records')

    def process(self, element):
        try:
            fields = element.split(",")
            order_id = fields[0]
            product_id = fields[1]
            category = fields[2]
            quantity = int(fields[3])
            price = float(fields[4])

            if not order_id or not product_id or not category:
                raise ValueError("Missing required fields")
            if quantity <= 0 or price <= 0:
                raise ValueError("Invalid quantity or price")

            self.valid_records.inc()
            yield {
                'order_id': order_id,
                'product_id': product_id,
                'category': category,
                'quantity': quantity,
                'price': price,
                'total_sale': quantity * price
            }

        except Exception as e:
            logger.error(f"Invalid record {element}: {e}")
            self.invalid_records.inc()


class SumSalesPerCategory(beam.DoFn):
    def process(self, element):
        category, sales = element
        yield (category, sum(sales))


def run():
    GCS_BUCKET = "dataflow-bkt-26122025"
    BQ_PROJECT = "quantum-episode-345713"
    REGION = "us-east1"

    csv_source_path = f"gs://{GCS_BUCKET}/sales_data.csv"
    csv_destination_path = f"gs://{GCS_BUCKET}/sales_output"

    # pipeline_options = PipelineOptions(beam_args)
    pipeline_options = PipelineOptions(
        runner="DataflowRunner",
        project=BQ_PROJECT,
        region=REGION,
        job_name="retail_sales-{{ ts_nodash | lower }}",
        temp_location=f"gs://{GCS_BUCKET}/temp/",
        staging_location=f"gs://{GCS_BUCKET}/staging/"
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Read Sales Data" >> beam.io.ReadFromText(csv_source_path, skip_header_lines=1)
            | "Validate and Transform" >> beam.ParDo(ValidateAndTransform())
            | "Extract Category and Total Sale" >> beam.Map(
                lambda r: (r['category'], r['total_sale'])
            )
            | "Group Sales By Category" >> beam.GroupByKey()
            | "Sum Sales Per Category" >> beam.ParDo(SumSalesPerCategory())
            | "Write Output" >> beam.io.WriteToText(csv_destination_path, file_name_suffix=".json")
        )


if __name__ == "__main__":
    run()
