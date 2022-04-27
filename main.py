import apache_beam as beam
import time
import re


CSV_PATH="data/ag_news_csv/test.csv"
OUTPUT_PATH="output/output.csv"



## MAIN -----

## Start counting time
start = time.time()


## Creating pipeline
print("[INFO] Creating pipeline ...")
with beam.Pipeline() as pipeline:
    outputs = (
        pipeline
        | 'ReadFromCsv' >> beam.io.ReadFromText(CSV_PATH)
        | 'ParseLineToWord' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
        | 'CountWords' >> beam.combiners.Count.PerElement()
        | 'WriteOutputToFile' >> beam.io.WriteToText(OUTPUT_PATH)
    )
#   outputs | beam.Map(print)


## End time counter
end = time.time()
total_time=end - start
print("[DONE] Total time : " + str(total_time) + " s")
