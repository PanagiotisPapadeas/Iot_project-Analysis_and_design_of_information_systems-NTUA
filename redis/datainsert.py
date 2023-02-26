import sys
import csv
import time
import redis
from kafka import KafkaConsumer
from datetime import datetime


r = redis.Redis(
  host='redis-10829.c55.eu-central-1-1.ec2.cloud.redislabs.com',
  port=10829,
  password='ZZRzLsLoJODxbmXZREbi1bFiGd3AfsLK')

#r.execute_command("ts.create AggDayRestEtot")

consumer = KafkaConsumer('flinkAggr','dailyAggr', group_id='my-group', auto_offset_reset='earliest')
for msg in consumer:
    msgstr = str(msg.value)
    x1 = [w for w in msgstr.split()]
    v1 = x1[0][2::]
    v2 = x1[1]+" "+x1[2]
    v3 = x1[3][:-1]
    # print(v1)
    # print(v2)
    # print(v3)
    datetime_object = datetime.strptime(v2, '%Y-%m-%d %H:%M:%S')
    final_timestamp = str(int(datetime_object.timestamp()*1e3))
    print(v1, final_timestamp, v3)
# print (msg.value)
# r = redis.Redis(host='redis-10829.c55.eu-central-1-1.ec2.cloud.redislabs.com', port=10829, db=0)
# r.execute_command("ts.create AggDayRestEtot")
    r.execute_command("ts.add " + v1 + " " + final_timestamp + " " + v3 + " ON_DUPLICATE LAST")

# if(len(sys.argv) > 1):
#    ticker = str(sys.argv[1])
# else:
#    ticker = 'test'

# file = ticker + '.csv'

# r = redis.Redis(host='redis-10829.c55.eu-central-1-1.ec2.cloud.redislabs.com', port=10829, db=0)

# r.execute_command("ts.create AggDayRestEtot")


# with open(file) as csv_file:
#    csv_reader = csv.reader(csv_file, delimiter=",")
#    r.execute_command("ts.create stock:"+ticker);
#    count = 0
#    temp = 0
#    for row in csv_reader:
#       if temp==0:
#           temp = temp + 1
#           continue
#       print(row) 
#       time_tuple = time.strptime(row[0], '%m/%d/%y')
#       time_epoch = time.mktime(time_tuple)*1000
#       r.execute_command("ts.add stock:"+ticker+" "+str(int(time_epoch))+" "+row[1])
#       count = count + 1

#    print(f'Imported {count} lines')
