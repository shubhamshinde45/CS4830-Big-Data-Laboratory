#!/usr/bin/env python

import pyspark
import sys

if len(sys.argv) != 3:
    raise Exception("Exactly 2 arguments are required: <inputUri> <outputUri>")

inputUri=sys.argv[1]
outputUri=sys.argv[2]

def hour_slotting(datetime):
    time = datetime.split(" ")[1]
    hour = time.split(":")[0]
    if hour == 'ID':
        return 'Not a number!'
    if 0 <= int(hour) < 6:
        return '0-6'
    if 6 <= int(hour) < 12:
        return '6-12'
    if 12 <= int(hour) < 18:
        return '12-18'
    if 18 <= int(hour) < 24:
        return '18-24'

sc = pyspark.SparkContext()
userclicks = sc.textFile(sys.argv[1])
count = userclicks.map(hour_slotting)
clicks_keeper = count.map(lambda Time: (Time,1)).reduceByKey(lambda x1, x2: x1 + x2)
clicks_keeper.coalesce(1).saveAsTextFile(sys.argv[2])
