import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount,Repeatedly,AfterAny
from apache_beam import window
import argparse
from datetime import datetime

parser=argparse.ArgumentParser() 

parser.add_argument('--input',
                    dest='input',
                    required=False,
                    help='From PubSub Topic',
                    default='projects/beam-290211/subscriptions/subscription1'
                    )

parser.add_argument('--output',
                    dest='output',
                    required=False,
                    help='PubSub to BigQuery',
                    default='beam.sales'
                    )



path_args, pipeline_args = parser.parse_known_args()   

inputs_pattern = path_args.input   
outputs_prefix = path_args.output 

options = PipelineOptions(pipeline_args)
options.view_as(StandardOptions).streaming=True
p = beam.Pipeline(options=options)

def covert_to_dict(row):
    row=dict(zip(('Store_id', 'Store_location', 'Product_id', 'Product_category',
    'sold_unit', 'buy_rate', 'sell_price','profit','transaction_date'),row))
    return row

def calculateProfit(elements):
  buy_rate = elements[5]
  sell_price = elements[6]
  products_count = int(elements[4])
  profit = (int(sell_price) - int(buy_rate)) * products_count
  elements.append(profit)
  return elements

def custom_timestamp(row):
    unix_time=row[7]
    return beam.window.TimestampedValue(row, int(unix_time))

class BuildRecordFn(beam.DoFn):
    def process(self,data,window=beam.DoFn.WindowParam):
        window_start = window.start
        #mylist=list()
        ts=datetime.utcfromtimestamp(int(window_start)).strftime('%Y-%m-%d %H:%M:%S')
        key,val=data
        (Store_id,Store_location,Product_id,Product_category,sold_unit,buy_rate,sell_price)=key
        profit=val
        transaction_date=ts
        return [(Store_id,Store_location,Product_id,Product_category,sold_unit,buy_rate,sell_price,profit,transaction_date)]



#############Create Pipeline ###########
stream_data=(
            p
             |'Read from PubSub'>>beam.io.ReadFromPubSub(subscription= inputs_pattern)
             |'Remove space in the Data '>>beam.Map(lambda row: row.lstrip().rstrip())
             |'Split Data '>>beam.Map(lambda row:row.decode().split(','))
             |'Calculate Profit'>>beam.Map(calculateProfit)
             | 'Apply custom timestamp' >> beam.Map(custom_timestamp)  
             |'Make Key value'>>beam.Map(lambda row:(row[:-2],row[-1]))
             |'Set Fixed Window of 30 sec'>>beam.WindowInto(window.FixedWindows(30),trigger=Repeatedly(
                    AfterAny(
                      AfterCount(5),
                      AfterProcessingTime(10)
                      )
                      ),accumulation_mode=AccumulationMode.DISCARDING)
             |'Combine Result of 30 Sec'>>beam.CombinePerKey(sum)
             |'Format result and append time'>>beam.ParDo(BuildRecordFn())
             |'Prepare data for BigQuery'>>beam.Map(covert_to_dict)
             #|'Write to Text'>>beam.io.WriteToText(outputs_prefix)
             |'Write to BigQuery'>>beam.io.WriteToBigQuery(table ='sales',dataset='beam',project='beam-290211')
                
             )

p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()


