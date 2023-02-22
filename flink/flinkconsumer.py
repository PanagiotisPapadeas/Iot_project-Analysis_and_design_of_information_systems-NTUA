import argparse
import logging
import sys
from datetime import datetime

from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common import WatermarkStrategy, Encoder, Types, SimpleStringSchema, Time, Duration
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
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

def word_count(input_path, output_path):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    # write all the data to one file
    env.set_parallelism(1)

    # define the source
    #if input_path is not None:
        # ds = env.from_source(
        #     source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
        #                                                input_path)
        #                      .process_static_file_set().build(),
        #     watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        #     source_name="file_source"
        # )
    env.add_jars("file:///home/panoplos/apache/flink-sql-connector-kafka-1.16.0.jar")
    source = KafkaSource.builder().set_bootstrap_servers('localhost:9092').set_topics("TutorialTopic").set_group_id("my-group").set_starting_offsets(KafkaOffsetsInitializer.earliest()).set_value_only_deserializer(SimpleStringSchema()).build()
    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    # def split(line):
    #     yield from line.split()

    def my_map_func(line):
        x = []
        for i in line.split():      
            x.append(i)
        temp = x[1] + " " + x[2]    
        y = (x[0], temp, float(x[3]))  
        return y  
     

    # compute word count
    #ds = ds.flat_map(split)
    ds = ds.map(my_map_func, output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]))

    with_timestamp_and_watermarks = ds.filter(lambda e: e).assign_timestamps_and_watermarks(WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(1)).with_timestamp_assigner(KafkaRowTimestampAssigner()))


    result = with_timestamp_and_watermarks.key_by(lambda i: i[0]) \
               .window(TumblingEventTimeWindows.of(Time.seconds(3600))) \
               .reduce(lambda v1, v2: (v1[0], v1[1], v1[2] + v2[2]), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()])) \

        # .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()]))
        # .key_by(lambda i: i[0]) \
        # .reduce(lambda i, j: (i[0], i[1] + j[1]))

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
    #ds.print()
    result.print()

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

    word_count(known_args.input, known_args.output)
