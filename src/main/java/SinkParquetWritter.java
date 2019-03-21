import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.fs.Writer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class SinkParquetWritter<T> implements Writer<T> {

    transient ParquetWriter writer = null;
    String schema = null;
    transient Schema schemaInstance = null;
    final ObjectMapper MAPPER = new ObjectMapper();

    public SinkParquetWritter(String schema) {
        this.writer = writer;
        this.schema = schema;
        try {
            this.schemaInstance = new Schema.Parser().parse(getClass().getClassLoader()
                    .getResourceAsStream(schema));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void open(FileSystem fileSystem, Path path) throws IOException {
        writer = AvroParquetWriter.builder(path)
                .withSchema(this.schemaInstance)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();
    }

    public long flush() throws IOException {
        return writer.getDataSize();
    }

    public long getPos() throws IOException {
        return writer.getDataSize();
    }

    public void close() throws IOException {
        writer.close();
    }


    public void write(T t) throws IOException {
        final Person person = ((Tuple2<Void,Person>) t).f1;
        GenericRecord record = new GenericData.Record(schemaInstance);
        record.put("text", String.valueOf(person.getText()));
        writer.write(record);
    }

    public Writer<T> duplicate() {
        return new SinkParquetWritter<T>(schema);
    }
}