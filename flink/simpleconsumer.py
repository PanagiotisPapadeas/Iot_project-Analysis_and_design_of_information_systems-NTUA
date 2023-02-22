import argparse
import logging
import sys
from datetime import datetime

from pyflink.common import WatermarkStrategy, Encoder, Types, SimpleStringSchema, Time
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingEventTimeWindows, TumblingProcessingTimeWindows

word_count_data = ["To be, or not to be,--that is the question:--",
                   "Whether 'tis nobler in the mind to suffer",
                   "The slings and arrows of outrageous fortune",
                   "Or to take arms against a sea of troubles,",
                   "And by opposing end them?--To die,--to sleep,--",
                   "No more; and by a sleep to say we end",
                   "The heartache, and the thousand natural shocks",
                   "That flesh is heir to,--'tis a consummation",
                   "Devoutly to be wish'd. To die,--to sleep;--",
                   "To sleep! perchance to dream:--ay, there's the rub;",
                   "For in that sleep of death what dreams may come,",
                   "When we have shuffled off this mortal coil,",
                   "Must give us pause: there's the respect",
                   "That makes calamity of so long life;",
                   "For who would bear the whips and scorns of time,",
                   "The oppressor's wrong, the proud man's contumely,",
                   "The pangs of despis'd love, the law's delay,",
                   "The insolence of office, and the spurns",
                   "That patient merit of the unworthy takes,",
                   "When he himself might his quietus make",
                   "With a bare bodkin? who would these fardels bear,",
                   "To grunt and sweat under a weary life,",
                   "But that the dread of something after death,--",
                   "The undiscover'd country, from whose bourn",
                   "No traveller returns,--puzzles the will,",
                   "And makes us rather bear those ills we have",
                   "Than fly to others that we know not of?",
                   "Thus conscience does make cowards of us all;",
                   "And thus the native hue of resolution",
                   "Is sicklied o'er with the pale cast of thought;",
                   "And enterprises of great pith and moment,",
                   "With this regard, their currents turn awry,",
                   "And lose the name of action.--Soft you now!",
                   "The fair Ophelia!--Nymph, in thy orisons",
                   "Be all my sins remember'd."]


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

    # else:
    #     print("Executing word_count example with default input data set.")
    #     print("Use --input to specify file input.")
    #     ds = env.from_collection(word_count_data)

    # def split(line):
    #     yield from line.split()

    # def my_map_func(line):
    #     x = []
    #     for i in line.split():      
    #         x.append(i)
    #     temp = x[1] + " " + x[2]    
    #     y = (x[0], temp, float(x[3]))
    #     # print(x)
    #     #y = tuple(new_x)    
    #     return y  
     

    # # compute word count
    # #ds = ds.flat_map(split)
    # ds = ds.map(my_map_func, output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]))

    # result = ds.key_by(lambda i: i[0]) \
    #            .window(TumblingProcessingTimeWindows.of(Time.seconds(5))) \
    #            .reduce(lambda v1, v2: (v1[0], v1[1], v1[2] + v2[2]), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()])) \

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
    ds.print()
    # result.print()

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
