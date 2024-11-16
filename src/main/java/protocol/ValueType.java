package protocol;

public enum ValueType {
    NONE("none"),
    STRING("string"),
    STREAM("stream");

    private final String value;

    ValueType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
