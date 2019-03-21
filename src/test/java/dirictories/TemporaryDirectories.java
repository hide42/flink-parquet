package dirictories;

import java.io.File;
import java.util.Arrays;

public class TemporaryDirectories {
    String tempPath = java.io.File.separator + "tmp" + java.io.File.separator + "test_zookeeper";
    String checkpointPath = checkpointPath = tempPath + java.io.File.separator + "checkpoints";
    public File dir = new File(tempPath);
    String zkSnapshotPath = tempPath + File.separator + "zookeeper-snapshot";
    public File zkSnapshotDir = new File(zkSnapshotPath);
    String zkLogDirPath = tempPath + File.separator + "zookeeper-logs";
    public File zkLogDir = new File(zkLogDirPath);
    public String kafkaLogDirPath = tempPath + File.separator + "kafka-logs";
    public File kafkaLogDir = new File(kafkaLogDirPath);

    public TemporaryDirectories() {
        deleteRecurs(dir);
        dir.mkdir();
        zkSnapshotDir.mkdir();
        zkLogDir.mkdir();
        kafkaLogDir.mkdir();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try{
                deleteRecurs(dir);
            }catch (Exception e)
            {
                e.printStackTrace();
            }
        }));
    }

    private void deleteRecurs(File file){
        if (file.isDirectory())
            Arrays.asList(file.listFiles()).forEach(this::deleteRecurs);
        if (file.exists() && !file.delete())
            try {
                throw new Exception(String.format("Unable to delete %s",file.getAbsolutePath()));
            } catch (Exception e) {
                e.printStackTrace();
            }
    }
}
