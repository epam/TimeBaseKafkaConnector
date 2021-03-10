package deltix.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;

import java.util.*;

public class TBConnectorConfig extends AbstractConfig {
    public static final String VERSION = "0.1";
    public final static char FIELD_PATH_SEPARATOR = '.';
    public final static char KEY_FIELDS_SEPARATOR = '_';
    public final static String NO_SYMBOL = "_";

    // system properties
    public static final String TB_USERNAME = "TIMEBASE_USERNAME";
    public static final String TB_PASSWORD = "TIMEBASE_PASSWORD";

    // timebase properties
    public static final String TB_URL_PROP = "timebase.url";
    public static final String TB_USER_PROP = "timebase.user";
    public static final String TB_PASS_PROP = "timebase.pwd";
    public static final String TB_STREAM_PROP = "timebase.stream";
    public static final String TB_MESSAGE_TYPE_PROP = "timebase.message.type";
    public static final String TB_MESSAGE_ID_PROP = "timebase.message.id";
    public static final String TB_MESSAGE_KEY_PROP = "timebase.message.key";
    public static final String TB_MESSAGE_TOMBSTONE_PROP = "timebase.message.tombstone";

    // kafka properties
    public static final String TOPIC_PROP = "topic";
    public static final String INSTRUMENT_FIELD_PROP = "instrument.field";
    public static final String SYMBOL_FIELD_PROP = "symbol.field";
    public static final String TIME_FIELD_PROP = "time.field";
    public static final String INCLUDE_PROP = "include.fields";
    public static final String EXCLUDE_PROP = "exclude.fields";
    public static final String TIMESTAMP_FIELDS_PROP = "timestamp.fields";

    public static final String KEY_FIELDS_PROP = "key.fields";
    public static final String FLATTEN_FIELDS_PROP = "flatten.fields";
    public static final String RENAME_FIELDS_PROP = "rename.fields";

    public static final ConfigDef SOURCE_CONFIG_DEF = createSourceConfigDef();
    public static final ConfigDef SINK_CONFIG_DEF = createSinkConfigDef();

    public TBConnectorConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }

    private static ConfigDef createSourceConfigDef() {
        ConfigDef def = new ConfigDef();
        addTBConnectionOptions(def);
        def.define(TB_MESSAGE_ID_PROP, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "TimeBase field containing Message ID");
        def.define(TB_MESSAGE_TYPE_PROP, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "Type of TimeBase messages to include");

        def.define(TOPIC_PROP, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Kafka Topic for storing TimeBase messages");
        def.define(INSTRUMENT_FIELD_PROP, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "Topic field to store message Instrument Type");
        def.define(SYMBOL_FIELD_PROP, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "Topic field to store message Symbol");
        def.define(TIME_FIELD_PROP, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "Topic field to store message Time");
        def.define(INCLUDE_PROP, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "TimeBase fields that should be included");
        def.define(EXCLUDE_PROP, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "TimeBase fields that should be excluded");
        def.define(RENAME_FIELDS_PROP, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "Rename fields according to specified aliases");
        def.define(KEY_FIELDS_PROP, ConfigDef.Type.LIST, null, ConfigDef.Importance.LOW, "TimeBase fields to use as record key");
        return def;
    }

    private static ConfigDef createSinkConfigDef() {
        ConfigDef def = new ConfigDef();
        addTBConnectionOptions(def);
        def.define(TB_MESSAGE_ID_PROP, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "TimeBase field for storing Kafka records Offsets");
        def.define(TB_MESSAGE_KEY_PROP, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "TimeBase field for storing Kafka records Keys");
        def.define(TB_MESSAGE_TOMBSTONE_PROP, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "TimeBase tombstone message marker field");
        def.define(INSTRUMENT_FIELD_PROP, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "Topic field to store as TimeBase message Instrument Type");
        def.define(SYMBOL_FIELD_PROP, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "Topic field to store as TimeBase message Symbol");
        def.define(TIME_FIELD_PROP, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "Topic field to store as TimeBase message Time");
        def.define(INCLUDE_PROP, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "Topic fields that should be included in TimeBase messages");
        def.define(EXCLUDE_PROP, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "Topic fields that should be excluded from TimeBase messages");
        def.define(TIMESTAMP_FIELDS_PROP, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "Topic fields that should be interpreted as timestamps in ms");
        def.define(FLATTEN_FIELDS_PROP, ConfigDef.Type.LIST, null, ConfigDef.Importance.LOW, "Flatten top level fields with sub-structures");
        def.define(RENAME_FIELDS_PROP, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "Rename fields according to specified aliases");
        return def;
    }

    private static void addTBConnectionOptions(ConfigDef def) {
        def.define(TB_URL_PROP, ConfigDef.Type.STRING, "dxtick://localhost:8011", ConfigDef.Importance.HIGH, "Timebase URL");
        def.define(TB_USER_PROP, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "Timebase User");
        def.define(TB_PASS_PROP, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.HIGH, "Timebase Password");
        def.define(TB_STREAM_PROP, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Timebase Stream");
    }

    static final String normalize(String value) {
        return (value == null || value.isEmpty()) ? null : value;
    }

    public String getTBUrl() {
        return getString(TB_URL_PROP);
    }

    public String getTBUser() {
        String user = getString(TB_USER_PROP);
        if (user == null) {
            user = System.getenv().get(TB_USERNAME);
        }
        return user;
    }

    public String getTBPassword() {
        Password pwd = getPassword(TB_PASS_PROP);
        return pwd == null ? System.getenv().get(TB_PASSWORD) : pwd.value();
    }

    public String getTBStream() {
        return getString(TB_STREAM_PROP);
    }

    public String getTBMessageType() {
        return normalize(getString(TB_MESSAGE_TYPE_PROP));
    }

    public String getTBMessageIDField() {
        return normalize(getString(TB_MESSAGE_ID_PROP));
    }

    public String getTBMessageKeyField() {
        return normalize(getString(TB_MESSAGE_KEY_PROP));
    }

    public String getTBMessageTombstoneField() {
        return normalize(getString(TB_MESSAGE_TOMBSTONE_PROP));
    }

    public FieldSelection getFieldSelection() {
        return new FieldSelection(getString(INCLUDE_PROP), getString(EXCLUDE_PROP));
    }

    public String getInstrumentField() {
        return normalize(getString(INSTRUMENT_FIELD_PROP));
    }

    public String getSymbolField() {
        return normalize(getString(SYMBOL_FIELD_PROP));
    }

    public String getTimeField() {
        return normalize(getString(TIME_FIELD_PROP));
    }

    public Set<String> getTimestampFields() {
        String timestampFields = normalize(getString(TIMESTAMP_FIELDS_PROP));
        return timestampFields == null ? null : new HashSet<String>(Arrays.asList(timestampFields.split(",")));
    }

    public List<String> getFlattenFields() {
        return getList(FLATTEN_FIELDS_PROP);
    }

    public FieldMap getNameAliases() {
        return new FieldMap(getString(RENAME_FIELDS_PROP), FIELD_PATH_SEPARATOR);
    }

    public List<String> getKeyFields() { return getList(KEY_FIELDS_PROP); }

    public String getTopic() {
        return getString(TOPIC_PROP);
    }
}
