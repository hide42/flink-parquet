import kafka.EmbeddedKafkaServer;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.util.Properties;

public class KafkaSingleNodeComposeTest {

    static EmbeddedKafkaServer kafkaServer = new EmbeddedKafkaServer();

    private static final int countMessages = 100;

    public static void main(String[] args) throws InterruptedException {
        String host = "localhost";
        Integer port = kafkaServer.kbPort;
        kafkaServer.start();
        Thread.sleep(2000);
        Thread thread = new Thread(){
            @Override
            public void run(){
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Properties kafkaProps = new Properties();
                kafkaProps.put("bootstrap.servers", host+":"+port);
                kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                KafkaProducer<String,String> producer = new KafkaProducer<String, String>(kafkaProps);

                for (int i = 0; i < countMessages; i++) {
                    producer.send(new ProducerRecord<String, String>("test", RandomStringUtils.random(10)));
                }
            }
        };
        thread.start();
        Thread thread2 = new Thread(){
            @Override
            public void run() {
                Main.main(new String[]{host+":"+port, "test", "test_parquet"});
            }
        };
        thread2.start();
        Thread.sleep(5000);
        thread2.interrupt();
        thread2.stop();
    }
}
