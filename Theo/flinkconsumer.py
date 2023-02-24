import argparse
import logging
import sys

from datetime import datetime
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common import WatermarkStrategy, Duration, Encoder, Types, SimpleStringSchema, Time
from pyflink.datastream import TimeCharacteristic, StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaTopicPartition
from pyflink.datastream.window import TumblingEventTimeWindows, TumblingProcessingTimeWindows

class KafkaRowTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value, record_timestamp):
        temporary = str(value[1])
        #print(temporary)
        datetime_object = datetime.strptime(temporary, '%Y-%m-%d %H:%M:%S')
        #print(datetime_object)
        final = int(datetime_object.timestamp()*1e3)
        #print(final)
        return final

def my_map_func(event):
    x = []
    for i in event.split():      
        x.append(i)
    temp = x[1] + " " + x[2]    
    y = (x[0], temp, float(x[3])) # (device, timestamps, value)

    return y  

def live_streaming_layer(input_path, output_path):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    # write all the data to one file
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    # kafka connector jar 
    env.add_jars("file:///home/theo/flink-sql-connector-kafka-1.16.1.jar")

    # define the source
    #if input_path is not None:
        # ds = env.from_source(
        #     source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
        #                                                input_path)
        #                      .process_static_file_set().build(),
        #     watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        #     source_name="file_source"
        # )

    # -------------------------- Thermal Sensors --------------------------
    partition_set = {
        KafkaTopicPartition("dailyAggr", 0),
        #KafkaTopicPartition("dailyAgg", 5)
    }

    thermal_sensors = KafkaSource.builder() \
        .set_bootstrap_servers('localhost:9092') \
        .set_partitions(partition_set) \
        .set_group_id("my-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(thermal_sensors, WatermarkStrategy.no_watermarks(), "Kafka Source") # .for_bounded_out_of_orderness(Duration.ofSeconds(48*3600)).with_timestamp_assigner(KafkaRowTimestampAssigner())

    ds = ds.map(my_map_func, output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]))

    with_timestamp_and_watermarks = ds.filter(lambda e: e) \
                                    .assign_timestamps_and_watermarks(WatermarkStrategy \
                                    .for_bounded_out_of_orderness(Duration.of_seconds(21)) \
                                    .with_timestamp_assigner(KafkaRowTimestampAssigner()))

    result = with_timestamp_and_watermarks.key_by(lambda i: i[0]) \
               .window(TumblingEventTimeWindows.of(Time.seconds(3600))) \
               .reduce(lambda v1, v2: (v1[0], v1[1], v1[2] + v2[2]), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()])) \


# -------------------------- Other Sensors --------------------------
    partition_set1 = {
        KafkaTopicPartition("dailyAggr", 1),
        #KafkaTopicPartition("dailyAgg", 5)
    }

    other_sensors = KafkaSource.builder() \
        .set_bootstrap_servers('localhost:9092') \
        .set_partitions(partition_set1) \
        .set_group_id("my-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds1 = env.from_source(other_sensors, WatermarkStrategy.no_watermarks(), "Kafka Source") # .for_bounded_out_of_orderness(Duration.ofSeconds(48*3600)).with_timestamp_assigner(KafkaRowTimestampAssigner())

    ds1 = ds1.map(my_map_func, output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]))

    with_timestamp_and_watermarks1 = ds1.filter(lambda e: e) \
                                    .assign_timestamps_and_watermarks(WatermarkStrategy \
                                    .for_bounded_out_of_orderness(Duration.of_seconds(21)) \
                                    .with_timestamp_assigner(KafkaRowTimestampAssigner()))

    result1 = with_timestamp_and_watermarks1.key_by(lambda i: i[0]) \
               .window(TumblingEventTimeWindows.of(Time.seconds(3600))) \
               .reduce(lambda v1, v2: (v1[0], v1[1], v1[2] + v2[2]), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()])) \


    # define the sink
    # if output_path is not None:
    #     ds.sink_to(
    #         sink=FileSink.for_row_format(
    #             base_path=output_path,
    #             encoder=Encoder.simple_string_encoder())
    #         .with_output_file_config(
    #             OutputFileConfig.builder()
    #             .with_part_prefix("prefix")
    #             .with_part_suffix(".ext")
    #             .build())
    #         .with_rolling_policy(RollingPolicy.default_rolling_policy())
    #         .build()
    #     )
    # else:
    print("Printing result to stdout. Use --output to specify output path.")

    result.print()
    result1.print()

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
