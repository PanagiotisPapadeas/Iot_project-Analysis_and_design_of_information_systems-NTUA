import argparse
import logging
import sys
from datetime import datetime, timedelta
from typing import Tuple

from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common import WatermarkStrategy, Encoder, Types, SimpleStringSchema, Time, Duration
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, AggregateFunction
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.connectors.kafka import DeliveryGuarantee, KafkaRecordSerializationSchema, KafkaSink, KafkaSource, KafkaOffsetsInitializer, KafkaTopicPartition
from pyflink.datastream.window import TumblingEventTimeWindows, TumblingProcessingTimeWindows, SlidingEventTimeWindows


class KafkaRowTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value, record_timestamp):
        temporary = str(value[1])
        #print(temporary)
        datetime_object = datetime.strptime(temporary, '%Y-%m-%d %H:%M:%S')
        #print(datetime_object)
        final = int(datetime_object.timestamp()*1e3)
        #print(final)
        return final

def my_map_func(line):
    x = []
    for i in line.split():      
        x.append(i)
    temp = x[1] + " " + x[2]    
    y = (x[0], temp, float(x[3]))  
    return y  
     
def live_streaming_layer(input_path, output_path):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    # write all the data to one file
    env.set_parallelism(1)


    #TH1, TH2    
    partition_set = {
        KafkaTopicPartition("dailyAggr", 0),
    }
    
    #HVaC, MiaC
    partition_set1 = {
        KafkaTopicPartition("dailyAggr", 1),
    }
    
    #Etot
    partition_set2 = {
        KafkaTopicPartition("dailyAggr", 2),
    }
    
    #W1
    partition_set3 = {
        KafkaTopicPartition("dailyAggr", 3),
    }
    
    #Wtot
    partition_set4 = {
        KafkaTopicPartition("dailyAggr", 4),
    }

    env.add_jars("file:///home/panoplos/apache/flink-sql-connector-kafka-1.16.0.jar")

    # define the source
    source = KafkaSource.builder() \
        .set_bootstrap_servers('localhost:9092') \
        .set_partitions(partition_set) \
        .set_group_id("my-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    source1 = KafkaSource.builder() \
        .set_bootstrap_servers('localhost:9092') \
        .set_partitions(partition_set1) \
        .set_group_id("my-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()    

    source2 = KafkaSource.builder() \
        .set_bootstrap_servers('localhost:9092') \
        .set_partitions(partition_set2) \
        .set_group_id("my-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    source3 = KafkaSource.builder() \
        .set_bootstrap_servers('localhost:9092') \
        .set_partitions(partition_set3) \
        .set_group_id("my-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build() 

    source4 = KafkaSource.builder() \
        .set_bootstrap_servers('localhost:9092') \
        .set_partitions(partition_set4) \
        .set_group_id("my-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()      
         
    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
    ds = ds.map(my_map_func, output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]))

    ds1 = env.from_source(source1, WatermarkStrategy.no_watermarks(), "Kafka Source1")
    ds1 = ds1.map(my_map_func, output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]))

    ds2 = env.from_source(source2, WatermarkStrategy.no_watermarks(), "Kafka Source2")
    ds2 = ds2.map(my_map_func, output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]))

    ds3 = env.from_source(source3, WatermarkStrategy.no_watermarks(), "Kafka Source3")
    ds3 = ds3.map(my_map_func, output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]))

    ds4 = env.from_source(source4, WatermarkStrategy.no_watermarks(), "Kafka Source4")
    ds4 = ds4.map(my_map_func, output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]))

    with_timestamp_and_watermarks = ds.filter(lambda e: e).assign_timestamps_and_watermarks(WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(1)).with_timestamp_assigner(KafkaRowTimestampAssigner()))

    with_timestamp_and_watermarks1 = ds1.filter(lambda e: e).assign_timestamps_and_watermarks(WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(1)).with_timestamp_assigner(KafkaRowTimestampAssigner()))
    
    with_timestamp_and_watermarks2 = ds2.filter(lambda e: e).assign_timestamps_and_watermarks(WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(1)).with_timestamp_assigner(KafkaRowTimestampAssigner()))
    
    with_timestamp_and_watermarks3 = ds3.filter(lambda e: e).assign_timestamps_and_watermarks(WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(1)).with_timestamp_assigner(KafkaRowTimestampAssigner()))

    with_timestamp_and_watermarks4 = ds4.filter(lambda e: e).assign_timestamps_and_watermarks(WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(1)).with_timestamp_assigner(KafkaRowTimestampAssigner()))
  
    #TH1, TH2
    #86400 seconds in a day
    result = with_timestamp_and_watermarks.key_by(lambda i: i[0]) \
               .window(TumblingEventTimeWindows.of(Time.seconds(86400))) \
               .reduce(lambda v1, v2: (v1[0], v1[1], (v1[2] + v2[2])), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()])) \
               .map(lambda v: ('AggDay'+v[0], str(datetime.strptime(v[1], '%Y-%m-%d %H:%M:%S') - timedelta(minutes=120)), v[2]/96), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]))
    
    #Etot
    result2 = with_timestamp_and_watermarks2.key_by(lambda i: i[0]) \
               .window(SlidingEventTimeWindows.of(Time.seconds(2*86400-48), Time.seconds(86400))) \
               .reduce(lambda v1, v2: (v1[0], v2[1], v2[2] - v1[2]), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()])) \
               .map(lambda v: ('AggDayDiff'+v[0], str(datetime.strptime(v[1], '%Y-%m-%d %H:%M:%S') - timedelta(hours=23) - timedelta(minutes=45)), v[2]), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]))
    
    #HVaC, MiaC
    result1 = with_timestamp_and_watermarks1.key_by(lambda i: i[0]) \
               .window(TumblingEventTimeWindows.of(Time.seconds(86400))) \
               .reduce(lambda v1, v2: (v1[0], v1[1], v1[2] + v2[2]), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()])) \
               .map(lambda v: ('AggDay'+v[0], str(datetime.strptime(v[1], '%Y-%m-%d %H:%M:%S') - timedelta(minutes=120)), v[2]), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]))
  
    #Mtot
    result4 = with_timestamp_and_watermarks4.key_by(lambda i: i[0]) \
               .window(SlidingEventTimeWindows.of(Time.seconds(2*86400-48), Time.seconds(86400))) \
               .reduce(lambda v1, v2: (v1[0], v2[1], v2[2] - v1[2]), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()])) \
               .map(lambda v: ('AggDayDiff'+v[0], str(datetime.strptime(v[1], '%Y-%m-%d %H:%M:%S') - timedelta(hours=23) - timedelta(minutes=45)), v[2]), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]))

#str(datetime.strptime(v1[1], '%Y-%m-%d %H:%M:%S') - timedelta(minutes=45))
    
    #W1
    result3 = with_timestamp_and_watermarks3.key_by(lambda i: i[0]) \
               .window(TumblingEventTimeWindows.of(Time.seconds(86400))) \
               .allowed_lateness((2*86400+10)*1e3) \
               .reduce(lambda v1, v2: (v1[0], v1[1], v1[2] + v2[2]), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()])) \
               .map(lambda v: ('AggDay'+v[0], str(datetime.strptime(v[1], '%Y-%m-%d %H:%M:%S') - timedelta(minutes=120)), v[2]), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]))

    resultunion = result2.union(result1)
    resultunion1 = result4.union(result3)  

    with_timestamp_and_watermarks_union = resultunion.filter(lambda e: e).assign_timestamps_and_watermarks(WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(1)).with_timestamp_assigner(KafkaRowTimestampAssigner()))
    with_timestamp_and_watermarks_union1 = resultunion1.filter(lambda e: e).assign_timestamps_and_watermarks(WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(1)).with_timestamp_assigner(KafkaRowTimestampAssigner()))
    
    #AggDayRestEtot
    resultfinal = with_timestamp_and_watermarks_union \
                   .map(lambda v: (v[0], v[1], v[2]) if v[0] == 'AggDayDiffEtot' else (v[0], v[1], -1*v[2]), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()])) \
                   .window_all(TumblingEventTimeWindows.of(Time.seconds(86400))) \
                   .reduce (lambda v1, v2: ('AggDayRestEtot', v2[1], v1[2]+v2[2]), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()])) 
    
    #AggDayRestWtot
    resultfinal1 = with_timestamp_and_watermarks_union1 \
                   .map(lambda v: (v[0], v[1], v[2]) if v[0] == 'AggDayDiffWtot' else (v[0], v[1], -1*v[2]), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()])) \
                   .window_all(TumblingEventTimeWindows.of(Time.seconds(86400))) \
                   .reduce(lambda v1, v2: ('AggDayRestWtot', v2[1], v1[2] + v2[2]) , output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]))
    
    #AggDayAll
    resultunionall = result.union(result1, result2, result3, result4, result, resultfinal, resultfinal1)
    resultunionall.print()


    sink = KafkaSink.builder() \
        .set_bootstrap_servers('localhost:9092') \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("flinkAggr")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.NONE) \
        .build()

    resultunionall.map(lambda x: str(x[0])+" "+str(x[1])+" "+str(x[2]), output_type=Types.STRING()).sink_to(sink)

    #resultfinal.print()
    #resultfinal1.print()

    #print("Printing result to stdout. Use --output to specify output path.")
    #ds.print()
    #result.print()
    #result1.print()
    #result2.print()
    #result3.print()
    #result4.print()

    # submit for execution
    env.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        help='Output file to write results to.')

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    live_streaming_layer(known_args.input, known_args.output)
