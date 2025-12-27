import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
from common_lib.constants import Constants

class ValidateAndTransform(beam.DoFn):
    def process(self, element):
        try:
            fields = element.split(",")

            transaction_id = fields[0]
            user_id = int(fields[1])
            product_id = fields[2]
            quantity = int(fields[3])
            price = float(fields[4])
            timestamp = fields[5]

            if not user_id or not product_id:
                raise ValueError("Missing user_id or product_id")
            if quantity <= 0 or price <= 0:
                raise ValueError("Invalid quantity or price")

            yield {
                "transaction_id": transaction_id,
                "user_id": user_id,
                "product_id": product_id,
                "quantity": quantity,
                "price": price,
                "timestamp": timestamp,
                "total_sales": quantity * price
            }

        except Exception as e:
            logging.error(f"Invalid record: {element} | {e}")
            yield beam.pvalue.TaggedOutput("invalid_records", element)

def run():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--gcs_bucket", required=True)
    parser.add_argument("--project_id", required=True)
    args, beam_args = parser.parse_known_args()
    GCS_BUCKET = args.gcs_bucket
    BQ_PROJECT = args.project_id

    csv_source_path = f"gs://{GCS_BUCKET}/transactions.csv"
    sink_invalid_path = f"gs://{GCS_BUCKET}/invalid/transactions_invalid"
    table_id = Constants.BQ.VALID_TRANSACTIONS_TABLE.format(bq_project=BQ_PROJECT)

    BQ_SCHEMA = (
        "transaction_id:STRING,"
        "user_id:INT64,"
        "product_id:STRING,"
        "quantity:INT64,"
        "price:FLOAT64,"
        "timestamp:TIMESTAMP,"
        "total_sales:FLOAT64"
    )

    pipeline_options = PipelineOptions(beam_args)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        pc1 = pipeline | "Read CSV" >> beam.io.ReadFromText(csv_source_path, skip_header_lines=1)

        pc_valid, pc_invalid = (
            pc1
            | "Validate & Transform"
            >> beam.ParDo(ValidateAndTransform()).with_outputs("invalid_records", main="valid_records")
        )

        pc_valid | "Write to BQ" >> beam.io.WriteToBigQuery(
            table=table_id,
            schema=BQ_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location=f"gs://{GCS_BUCKET}/stage/"
        )

        pc_invalid | "Write Invalid to GCS" >> beam.io.WriteToText(
            sink_invalid_path,
            file_name_suffix=".json"
        )

if __name__ == "__main__":
    run()
