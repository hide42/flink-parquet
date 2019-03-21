import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import java.util.Properties;

public class Main {

    private static String outputPath = "/tmp/test";

    public static void main(String[] args){
        Properties properties = new Properties();
        if(args.length>2){
            properties.put("bootstrap.servers", args[0]);
            properties.put("zookeeper.connect", args[1]);
            outputPath=args[2];
        }else {
            properties.put("bootstrap.servers", "kafka-1:9092");
            properties.put("zookeeper.connect", "kafka-1:2181");
        }


        properties.put("group.id", "test");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(
                RestartStrategies.fixedDelayRestart(4, 1000));
        //env.getConfig().disableSysoutLogging();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        DataStream<Tuple2<Void, Person>> sourceStream = env.addSource(
                new FlinkKafkaConsumer010(
                        "test",
                        new Tuple2Deserializer(),
                        properties)).name("kafka_consumer");
        //for debug
        sourceStream.rebalance().print();
        BucketingSink<Tuple2<Void, Person>> hdfsSink = new BucketingSink<Tuple2<Void, Person>>(outputPath);
        // format folders for partitions
        hdfsSink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HH-mm"));
        hdfsSink.setWriter(new SinkParquetWritter<Tuple2<Void, Person>>("person.avsc"));
        //When a bucket part file becomes larger than this size a new bucket part file is started and
        // the old one is closed (status pending - finished)
        hdfsSink.setBatchSize(10 * 1024 * 1);
        sourceStream.addSink(hdfsSink).name("write_parquet");
        try {
            env.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

