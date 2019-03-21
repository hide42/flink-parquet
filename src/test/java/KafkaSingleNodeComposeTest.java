import kafka.EmbeddedKafkaServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaSingleNodeComposeTest {

    static EmbeddedKafkaServer kafkaServer = new EmbeddedKafkaServer();

    private static final int countMessages = 1000;

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        kafkaServer.start();
        Thread.sleep(2000);
        kafkaServer.createTopic("test");

        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                KafkaProducer<String,String> producer = kafkaServer.getProducer();
                for (int i = 0; i < countMessages; i++) {
                    producer.send(new ProducerRecord<String, String>("test", "test_message_generator"));
                }
            }
        };
        thread.start();

        //your main class with args
        Main.main(new String[]{host + ":" + kafkaServer.kbPort,host+":"+kafkaServer.zkPort, "hdfs://localhost:9000/tmp/test_parquet"});

    }
}
