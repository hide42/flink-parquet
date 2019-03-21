package zookeeper;

import dirictories.TemporaryDirectories;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;


public class EmbeddedZookeeper extends Thread {
    int port;
    TemporaryDirectories temporaryDirectories;
    Logger logger = LoggerFactory.getLogger(EmbeddedZookeeper.class);
    private ServerCnxnFactory factory;

    public EmbeddedZookeeper(int port, TemporaryDirectories temporaryDirectories) {
        this.port = port;
        this.temporaryDirectories = temporaryDirectories;
    }

    @Override
    public void run() {
        System.out.println(String.format("Starting zk on %d", port));
        int tickTime = 2000;
        int maxConns = 30;
        try {
            ZooKeeperServer zkServer = new ZooKeeperServer(temporaryDirectories.zkSnapshotDir,
                    temporaryDirectories.zkLogDir, tickTime);
            factory = NIOServerCnxnFactory.createFactory(new InetSocketAddress("localhost", port), maxConns);
            factory.startup(zkServer);
            System.out.println("zk started");
        } catch (IOException | InterruptedException e) {
            logger.error("error");
            stopZk();
            e.printStackTrace();
        }
    }

    public void stopZk() {
        logger.info("shutting down zk on {}", port);
        Thread.currentThread().interrupt();
        factory.shutdown();
    }
}
