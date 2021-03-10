package deltix.kafka.connect;

import deltix.qsrv.hf.pub.InstrumentIdentity;
import deltix.qsrv.hf.pub.RawMessage;
import deltix.qsrv.hf.pub.md.RecordClassDescriptor;
import deltix.qsrv.hf.tickdb.pub.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static deltix.kafka.connect.TBConnectorConfig.*;

public class TBSinkTask extends SinkTask {
    private final static Logger LOG = LoggerFactory.getLogger(TBSinkTask.class);

    private TBConnectorConfig config;
    private String offsetField;

    private DXTickDB timebase;
    private DXTickStream msgStream;
    private TickLoader tbLoader;
    private RawMessageSerializer serializer;
    private long lastMessageTimestamp = 0;

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        config = new TBConnectorConfig(SINK_CONFIG_DEF, props);

        offsetField = config.getTBMessageIDField();

        timebase = TickDBFactory.createFromUrl(config.getTBUrl(), config.getTBUser(), config.getTBPassword());
        timebase.open(false);
        msgStream = timebase.getStream(config.getTBStream());

        String topic = props.get(TOPICS_CONFIG);
        if (topic == null || topic.split(",").length != 1) {
            throw new IllegalArgumentException("Expecting a single topic instead of " + topic);
        }

        if (msgStream != null)
            lastMessageTimestamp = getLastMessageTimestamp();

//        if (msgStream != null) {
//            //Struct record = getLastRecord();
//            if (offsetField != null && record != null) {
//                //Long offset = record.getInt64(offsetField);
//                //TopicPartition partition = new TopicPartition(topic, 0);
//                //LOG.info("Starting at partition " + partition + " offset: " + offset);
//                //context.offset(partition, offset);
//            }
//        }
    }

    private RawMessageDeserializer getDeserializer() {
        HashMap<String,String> srcConfig = new HashMap<>();
        srcConfig.put(TB_STREAM_PROP, "stream");
        srcConfig.put(TOPIC_PROP, "topic");
        TBConnectorConfig config = new TBConnectorConfig(SOURCE_CONFIG_DEF, srcConfig);
        return new RawMessageDeserializer(msgStream.getFixedType(), config);
    }

    private long getLastMessageTimestamp() {
        RawMessageDeserializer deserializer = getDeserializer();
        try (TickCursor cursor = msgStream.select(Long.MAX_VALUE, new SelectionOptions(true, false, true))) {
            if (cursor.next()) {
                return cursor.getMessage().getTimeStampMs();
            }
        }
        return 0;
    }

    // can use this to count messages with the same timestamp
    private Struct getLastRecord() {
        Struct record = null;
        long[] range = msgStream.getTimeRange(new InstrumentIdentity[0]);
        if (range != null) {
            RawMessageDeserializer deserializer = getDeserializer();
            try (TickCursor cursor = msgStream.select(range[1], new SelectionOptions(true, false, false))) {
                while (cursor.next()) {
                    record = deserializer.deserialize((RawMessage) cursor.getMessage());
                }
            }
        }
        return record;
    }

    private Struct getLastRecord2() {
        Struct record = null;
        RawMessageDeserializer deserializer = getDeserializer();
        try (TickCursor cursor = msgStream.select(Long.MAX_VALUE, new SelectionOptions(true, false, true))) {
            if (cursor.next()) {
                record = deserializer.deserialize((RawMessage) cursor.getMessage());
            }
        }
        return record;
    }

    private void initSerializer(SinkRecord record) {
        assert serializer == null;

        RecordClassDescriptor descriptor = null;
        if (msgStream != null) {
            descriptor = msgStream.getFixedType();
            if (descriptor == null)
                throw new IllegalArgumentException("TimeBase message stream \"" + config.getTBStream() + "\" does not have fixed type");
        }
        serializer = new RawMessageSerializer(config, descriptor, record.valueSchema());
    }

    private void send(RawMessage message) {
        if (tbLoader == null) {
            if (msgStream == null) {
                String tbStream = config.getTBStream();
                StreamOptions options = new StreamOptions(StreamScope.DURABLE, tbStream, "Created by TBSinkConnector", StreamOptions.MAX_DISTRIBUTION);
                options.setFixedType(message.type);
                msgStream = timebase.createStream(tbStream, options);
            }
            LoadingOptions options = new LoadingOptions(true);
            options.writeMode = LoadingOptions.WriteMode.APPEND;
            tbLoader = msgStream.createLoader(options);
        }
        tbLoader.send(message);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        boolean debug = LOG.isDebugEnabled();

        for (SinkRecord record : records) {
            if (serializer == null) {
                // ignore tombstone messages if the stream was not created yet
                if (msgStream == null && record.valueSchema() == null) {
                    LOG.warn("Ignoring tombstone record with the key " + record.key());
                    continue;
                }
                initSerializer(record);
            }

            if (debug) LOG.debug("Record: " + record);
            RawMessage message = serializer.serialize(record, lastMessageTimestamp);
            if (debug) LOG.debug("RawMessage: " + message);

            if (message != null) {
                lastMessageTimestamp = message.getTimeStampMs();
                send(message);
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        try {
            if (LOG.isDebugEnabled()) {
                StringBuilder offsetInfo = new StringBuilder("Flushing at ");
                currentOffsets.forEach((key, value) -> offsetInfo.append(key).append(":").append(value));
                LOG.debug(offsetInfo.toString());
            }
           if (tbLoader != null)
               tbLoader.flush();
        }
        catch (IOException ex) {
            LOG.error("Failed to write to TimeBase", ex);
            throw new RuntimeException("Failed to write to TimeBase", ex);
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        return super.preCommit(currentOffsets);
    }

    @Override
    public void stop() {
        LOG.info("Stopping TBSinkTask");

        if (tbLoader != null) {
            tbLoader.close();
        }
        if (timebase != null) {
            timebase.close();
        }
    }
}
