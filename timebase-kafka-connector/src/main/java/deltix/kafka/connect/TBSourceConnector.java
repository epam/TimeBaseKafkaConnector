package deltix.kafka.connect;

import deltix.qsrv.hf.pub.md.RecordClassDescriptor;
import deltix.qsrv.hf.tickdb.pub.DXTickDB;
import deltix.qsrv.hf.tickdb.pub.DXTickStream;
import deltix.qsrv.hf.tickdb.pub.TickDBFactory;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static deltix.kafka.connect.TBConnectorConfig.SOURCE_CONFIG_DEF;
import static deltix.kafka.connect.TBConnectorConfig.VERSION;

/**
 * TimeBase Source Connector
 */
public class TBSourceConnector extends SourceConnector {

    private final static Logger lOG = LoggerFactory.getLogger(TBSourceConnector.class);

    private TBConnectorConfig config;

    @Override
    public Class<? extends Task> taskClass() {
        return TBSourceTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new TBConnectorConfig(config(), props);

        try (DXTickDB timebase = TickDBFactory.createFromUrl(config.getTBUrl(), config.getTBUser(), config.getTBPassword())) {
            timebase.open(true);

            DXTickStream msgStream = timebase.getStream(config.getTBStream());
            if (msgStream == null) {
                throw new IllegalArgumentException(config.getTBStream() + " timebase stream does not exist");
            }

            //DataField field = msgStream.getFixedType().getField(tbMessageId);
            //if (field == null) {
            //    throw new IllegalArgumentException(tbMessageId + " field does not exist in timebase stream " + tbStream);
            //}

            //if allow multiple streams and using a filter, monitor TB stream spaces and reassign tasks when new space is added
        }
    }

    @Override
    public void stop() {
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        configs.add(config.originalsStrings());
        return configs;
    }

    @Override
    public ConfigDef config() {
        return SOURCE_CONFIG_DEF;
    }

    @Override
    public String version() {
        return VERSION;
    }
}