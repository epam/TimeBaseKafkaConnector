package deltix.kafka.connect;

import org.apache.kafka.connect.data.*;

/**
 * Avro Logical Types
 */
public enum LogicalType {
    DATE("date", Date.SCHEMA),
    TIME_MS("time-millis", Time.SCHEMA),
    TIME_MC("time-micros", SchemaBuilder.int64().name("time-micros").build()),
    TIMESTAMP_MS("timestamp-millis", Timestamp.SCHEMA),
    TIMESTAMP_MC("timestamp-micros", SchemaBuilder.int64().name("timestamp-micros").build()),
    LOCAL_TIMESTAMP_MS("local-timestamp-millis", SchemaBuilder.int64().name("local-timestamp-millis").build()),
    LOCAL_TIMESTAMP_MC("local-timestamp-micros", SchemaBuilder.int64().name("local-timestamp-micros").build()),
    DURATION("duration", SchemaBuilder.bytes().name("duration").build()),
    DECIMAL("decimal", Decimal.schema(0)),
    UUID("uuid", SchemaBuilder.struct().name("uuid").build());

    private String avroName;
    private Schema schema;

    private LogicalType(String avroName, Schema schema) {
        this.avroName = avroName;
        this.schema = schema;
    }

    public Schema getSchema() {
        return schema;
    }

    public String getLogicalName() {
        return schema.name();
    }

    public static LogicalType getTypeByName(String logicalName) {
        if (logicalName != null) {
            for (LogicalType type : values()) {
                if (type.getLogicalName().equals(logicalName) || type.avroName.equals(logicalName))
                    return type;
            }
        }
        return null;
    }
}
