package deltix.kafka.connect;

import deltix.qsrv.hf.pub.RawMessage;
import deltix.qsrv.hf.pub.md.RecordClassDescriptor;
import deltix.qsrv.hf.tickdb.pub.*;
import deltix.util.concurrent.UncheckedInterruptedException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static deltix.kafka.connect.TBConnectorConfig.*;

public class TBSourceTask extends SourceTask {
    private final static Logger LOG = LoggerFactory.getLogger(TBSourceTask.class);

    public static final String PARTITION_ATTR = "partition";
    public static final String TIMESTAMP_ATTR = "timestamp";
    public static final String OFFSET_ATTR = "offset";

    // Safe period away from live messages when reading from TB
    private static final long OFFSET_INTERVAL = TimeUnit.SECONDS.toMillis(10);
    // Time to wait for more messages after stepping inside OFFSET_INTERVAL
    private static final long ACCUMULATION_INTERVAL  = TimeUnit.MINUTES.toMillis(1);

    private String tbMessageId;
    private String tbMessageType;

    private DXTickDB timebase;
    private TickCursor cursor;
    private boolean live = true;
    private RawMessageDeserializer deserializer;

    private Map<String,String> partition;

    private long lastMessageTimestamp = 0;
    private long lastMessageCounter = 0;

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        TBConnectorConfig config = new TBConnectorConfig(SOURCE_CONFIG_DEF, props);

        String tbStream = config.getTBStream();
        tbMessageId = config.getTBMessageIDField();
        tbMessageType = config.getTBMessageType();

        partition = Collections.singletonMap(PARTITION_ATTR, tbStream);

        timebase = TickDBFactory.createFromUrl(config.getTBUrl(), config.getTBUser(), config.getTBPassword());
        timebase.open(true);

        DXTickStream msgStream = timebase.getStream(tbStream);
        if (msgStream == null) {
            throw new IllegalArgumentException("TimeBase stream \"" + tbStream + "\"does not exist");
        }
        RecordClassDescriptor msgType = null;
        RecordClassDescriptor[] msgTypes = msgStream.getTypes();
        if (msgTypes.length > 1) {
            if (tbMessageType == null) {
                throw new IllegalArgumentException("Specify type of messages to read from polymorphic TimeBase stream " + config.getTBStream());
            }
            msgType = getDescriptor(msgTypes, tbMessageType);
            if (msgType == null) {
                throw new IllegalArgumentException("TimeBase stream does not have \"" + tbMessageType + "\" message type");
            }
        } else if (msgTypes.length == 1) {
            msgType = msgTypes[0];
            if (tbMessageType != null && !tbMessageType.equals(msgType.getName())) {
                throw new IllegalArgumentException("TimeBase stream does not have \"" + tbMessageType + "\" message type");
            }
        } else {
            throw new IllegalArgumentException("TimeBase stream has no record descriptors");
        }

        deserializer = new RawMessageDeserializer(msgType, config);

        Long lastTimestamp = 0L;
        Long lastMessageOffset = null;
        Map<String, Object> offset = context.offsetStorageReader().offset(partition);

        if (offset != null) {
            lastTimestamp = (Long) offset.get(TIMESTAMP_ATTR);
            lastMessageOffset = (Long) offset.get(OFFSET_ATTR);
        }
        LOG.info("Starting TBSourceTask at timestamp: " + lastTimestamp + ", offset: " + lastMessageOffset);

        SelectionOptions options = new SelectionOptions(true, true);
        cursor = msgStream.select(lastTimestamp, options);

        lastMessageTimestamp = lastTimestamp;
        lastMessageCounter = 0;

        if (lastMessageOffset != null) {
            resetToOffset(lastMessageOffset);
        }
    }

    private static RecordClassDescriptor getDescriptor(RecordClassDescriptor[] descriptors, String name) {
        for (RecordClassDescriptor descriptor : descriptors) {
            if (name.equals(descriptor.getName()))
                return descriptor;
        }
        return null;
    }

    // TODO this message can block
    private void resetToOffset(long offset) {
        while (cursor.next()) {
            RawMessage message = (RawMessage) cursor.getMessage();
            if (message.getTimeStampMs() != lastMessageTimestamp) {
                break;
            }

            lastMessageCounter++;

            if (tbMessageId == null) {
                if (offset == lastMessageCounter) {
                    LOG.info("Found last recorded message: " + offset);
                    return;
                }
            }
            else {
                Map<String,Object> values = deserializer.getValues(message);
                Long messageId = (Long) values.get(tbMessageId);
                if (messageId != null && messageId == offset) {
                    LOG.info("Found last recorded message with ID: " + offset);
                    return;
                }
            }
        }

        LOG.warn("Last message with offset " + offset + " not found. Starting at timestamp: " + lastMessageTimestamp);
        cursor.reset(lastMessageTimestamp);
        lastMessageCounter = 0;
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        boolean debug = LOG.isDebugEnabled();
        try {
            RawMessage message;
            while ((message = nextMessage(cursor)) != null) {
                if (debug) LOG.debug("RawMessage: " + message);

                if (tbMessageType == null || tbMessageType.equals(message.type.getName())) {
                    SourceRecord record = deserializer.deserialize(message, partition, lastMessageTimestamp, lastMessageCounter);
                    if (debug) LOG.debug("Record: " + record.value() + " (" + record.sourceOffset() + ")");

                    return Arrays.asList(record);
                }
            }
        }
        catch (Exception ex) {
            LOG.error("Failed to read next message", ex);
            throw new InterruptedException("Exiting");
        }
        return null;
    }

    private void sleep(long timeToWait) {
        try {
            Thread.sleep(timeToWait);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt(); // Restore interruption flag
            throw new UncheckedInterruptedException(ex);
        }
    }

    @Override
    public synchronized void stop() {
        LOG.info("Stopping TBSourceTask");
        if (cursor != null) {
            cursor.close();
        }
        if (timebase != null) {
            timebase.close();
        }
    }

    private RawMessage nextMessage(TickCursor cursor) {
        RawMessage message = null;
        if (!cursor.isClosed() && cursor.next()) {
            message = (RawMessage) cursor.getMessage();
            if (message.getTimeStampMs() != lastMessageTimestamp) {
                // we need to wait if cursor is too close to the live edge
                while (message == null || message.getTimeStampMs() > System.currentTimeMillis() - OFFSET_INTERVAL) {
                    if (! live)
                        return null; // stop here to avoid reading messages out of order

                    LOG.info("Waiting for more messages " + ACCUMULATION_INTERVAL + " ms");
                    sleep(ACCUMULATION_INTERVAL);

                    if (cursor.isClosed())
                        return null;

                    cursor.reset(lastMessageTimestamp + 1);
                    message = cursor.next() ? (RawMessage) cursor.getMessage() : null;
                }

                lastMessageTimestamp = message.getTimeStampMs();
                lastMessageCounter = 1;
            } else {
                lastMessageCounter++;
            }
        }
        return message;
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put(TB_URL_PROP, "dxtick://localhost:8011");
        config.put(TB_STREAM_PROP, "sink-test");
        //config.put(TB_MESSAGE_TYPE_PROP, "deltix.timebase.api.messages.securities.Currency");
        //config.put(TB_MESSAGE_ID_PROP, "currentTime");
        config.put(TOPIC_PROP, "tb-orders");

        TBSourceTask task = new TBSourceTask();
        try {
            task.initialize(new SourceTaskContext() {
                @Override
                public Map<String, String> configs() { return null; }
                @Override
                public OffsetStorageReader offsetStorageReader() {
                    return new OffsetStorageReader() {
                        @Override
                        public <T> Map<String, Object> offset(Map<String, T> map) { return null;}
                        @Override
                        public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> collection) { return null;}
                    };
                }
            });
            task.start(config);
            for (List<SourceRecord> records = task.poll(); records != null; records = task.poll()) {
                System.out.println(records.get(0));
            }
        }
        finally {
            task.stop();
        }
    }
}
