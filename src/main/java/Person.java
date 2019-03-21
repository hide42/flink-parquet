import java.io.Serializable;

// Model class основанный на avro schema
public class Person implements Serializable {
    public String text;

    public Person() {
    }

    public Person(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }
}
