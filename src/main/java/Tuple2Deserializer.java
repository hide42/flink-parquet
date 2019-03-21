import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import java.io.IOException;

public class Tuple2Deserializer  implements DeserializationSchema {

    public Object deserialize(byte[] bytes) throws IOException {

        String message = new String(bytes);
        if (message.trim().length()>0) {
            //init fields of model class :
            Person person = new Person(message);

            Tuple2<Void, Person> tuple = new Tuple2<Void, Person>();
            //init tuple by (void, ModelClass)
            tuple.setField(person,1);
            return tuple;
        } else {
            return null;
        }
    }

    public void getVoid(){}

    public boolean isEndOfStream(Object o) {
        return false;
    }

    public TypeInformation<Tuple2<Void, Person>> getProducedType() {
        return new TupleTypeInfo<Tuple2<Void, Person>>(TypeExtractor.createTypeInfo(Void.class), TypeExtractor.createTypeInfo(Person.class));
    }
}
