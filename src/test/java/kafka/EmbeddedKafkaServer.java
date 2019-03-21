package kafka;

import dirictories.TemporaryDirectories;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zookeeper.EmbeddedZookeeper;

import java.util.HashMap;
import java.util.Map;

public class EmbeddedKafkaServer extends Thread{
    TemporaryDirectories tempDirs = new TemporaryDirectories();
    public int zkPort = 2185;
    public int kbPort = 9095;
    Logger logger = LoggerFactory.getLogger(EmbeddedKafkaServer.class);
    EmbeddedZookeeper embeddedZookeeper;
    KafkaServerStartable kafkaServer ;

    @Override
    public void run() {
        System.out.println("Kek");
        System.out.println(String.format("starting on %d and %d",zkPort,kbPort));
        embeddedZookeeper=new EmbeddedZookeeper(zkPort, tempDirs);
        embeddedZookeeper.start();
        Map<String,String> kafkaProps = new HashMap<>();
        kafkaProps.put("port",Integer.toString(kbPort));
        kafkaProps.put("broker.id","1");
        kafkaProps.put("host.name","localhost");
        kafkaProps.put("log.dir",tempDirs.kafkaLogDirPath);
        kafkaProps.put("zookeeper.connect","localhost:"+zkPort);
        kafkaServer = new KafkaServerStartable(new KafkaConfig(kafkaProps));
        try{
            kafkaServer.startup();
        }catch (Exception e){
            stopServer();
        }
    }
    public void stopServer(){

        logger.info("shutting down broker on {}",kbPort);
        kafkaServer.shutdown();
        kafkaServer.awaitShutdown();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("shutting down zk on {}",zkPort);
        embeddedZookeeper.stopZk();
    }
}
