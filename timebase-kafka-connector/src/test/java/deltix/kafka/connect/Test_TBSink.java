package deltix.kafka.connect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static deltix.kafka.connect.TBConnectorConfig.*;

public class Test_TBSink {

    public static void main(String[] args) {
        String tbURL = args.length > 0 ? args[0] : "dxtick://localhost:8011";
        String topic = "app-activity-test";

        Map<String, String> config = new HashMap<>();
        config.put(TB_URL_PROP, tbURL);
        config.put(TB_STREAM_PROP, "connector_test");
        config.put(TB_MESSAGE_ID_PROP, "kafkaOffset");
        config.put(TB_MESSAGE_KEY_PROP, "kafkaKey");
        config.put(TBSinkTask.TOPICS_CONFIG, topic);
        //config.put(EXCLUDE_PROP, "quantity,struct.array");
        //config.put(FLATTEN_FIELDS_PROP, "struct");
        config.put(TIMESTAMP_FIELDS_PROP, "currentTime");
        config.put(TIME_FIELD_PROP, "currentTime");
        config.put(SYMBOL_FIELD_PROP, "currency");
        //config.put(RENAME_FIELDS_PROP, "amount:amount_renamed,array:array_renamed,struct.amount:s_amount,struct.id:s_id");

        DataGenerator generator = new DataGenerator(topic,1);
        TBSinkTask task = new TBSinkTask();
        try {
            task.start(config);
            for (int i = 0; i < 1000; i++) {
                task.put(generator.generateRecords(1000));
                System.out.print("\r\tProcessed: " + i);
            }
        }
        finally {
            task.stop();
        }
    }

    private static class DataGenerator {
        private Random random = new Random();
        private String topic;
        private int offset;
        private Schema keySchema;
        private Schema valueSchema;
        private int partition = 1;

        public DataGenerator(String topic, int offset) {
            this.topic = topic;
            this.offset = offset;
            this.valueSchema = buildSchema();
            this.keySchema = Schema.STRING_SCHEMA;
        }

        private ArrayList<SinkRecord> generateRecords(int count) {
            ArrayList<SinkRecord> records = new ArrayList<>(count);
            for (int n = 0; n < count; offset++, n++) {
                records.add(new SinkRecord(topic, partition, keySchema, generateKey(), valueSchema, generateValue(), offset));
            }
            return records;
        }

        private Schema buildSchema() {
            SchemaBuilder schemaBuilder = SchemaBuilder.struct();
            schemaBuilder.name("test");
            schemaBuilder.version(1);
            schemaBuilder.field("currentTime", Schema.INT64_SCHEMA);
            schemaBuilder.field("timeOfDay", LogicalType.TIME_MS.getSchema());
            schemaBuilder.field("currency", Schema.STRING_SCHEMA);
            schemaBuilder.field("name", Schema.STRING_SCHEMA);
            schemaBuilder.field("amount", Schema.FLOAT64_SCHEMA);
            schemaBuilder.field("quantity", Schema.FLOAT32_SCHEMA);
            schemaBuilder.field("count", Schema.OPTIONAL_INT32_SCHEMA);
            schemaBuilder.field("short", Schema.OPTIONAL_INT16_SCHEMA);
            schemaBuilder.field("byte", Schema.OPTIONAL_INT8_SCHEMA);
            schemaBuilder.field("array", SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional());

            SchemaBuilder structField = SchemaBuilder.struct().name("metadata");
            structField.field("id", LogicalType.TIMESTAMP_MS.getSchema());
            structField.field("amount", Schema.FLOAT32_SCHEMA);
            structField.field("array", SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional());

            schemaBuilder.field("struct", structField);
            return schemaBuilder.build();
        }

        private String generateKey() {
            return "key" + offset;
        }

        private Struct generateValue() {
            long currentTime = System.currentTimeMillis();
            Struct struct = new Struct(valueSchema);
            struct.put("currentTime", currentTime);
            struct.put("timeOfDay", new Date(random.nextInt((int) TimeUnit.DAYS.toMillis(1))));
            struct.put("currency", "BTCUSD");
            struct.put("name", generateString(random.nextInt(20)));
            struct.put("amount", random.nextDouble() * currentTime);
            struct.put("quantity", random.nextFloat() * currentTime);
            struct.put("count", random.nextInt());
            struct.put("short", (short) random.nextInt(Short.MAX_VALUE));
            struct.put("byte", (byte) random.nextInt(Byte.MAX_VALUE));
            struct.put("array", generateArray(random.nextInt(20)));

            Struct structField = new Struct(valueSchema.field("struct").schema());
            structField.put("id", new Date());
            structField.put("amount", random.nextFloat() * currentTime);
            structField.put("array", generateArray(random.nextInt(10)));
            struct.put("struct", structField);

            return struct;
        }

        private String generateString(int size) {
            String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvxyz";
            StringBuilder sb = new StringBuilder(size);
            for (int i = 0; i < size; i++) {
                sb.append(chars.charAt(random.nextInt(chars.length())));
            }
            return sb.toString();
        }

        private List<Integer> generateArray(int size) {
            ArrayList<Integer> array = null;
            if (size > 0) {
                array = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    Integer value = random.nextInt();
                    if (value % 4 == 0) value = null;
                    array.add(value);
                }
            }
            return array;
        }
    }
}
