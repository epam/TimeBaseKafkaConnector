package deltix.kafka.connect;

import deltix.qsrv.hf.tickdb.pub.DXTickDB;
import deltix.qsrv.hf.tickdb.pub.DXTickStream;
import deltix.qsrv.hf.tickdb.pub.TickDBFactory;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static deltix.kafka.connect.TBConnectorConfig.SINK_CONFIG_DEF;
import static deltix.kafka.connect.TBConnectorConfig.VERSION;

/**
 * TimeBase Sink Connector
 */
public class TBSinkConnector extends SinkConnector {

    private final static Logger lOG = LoggerFactory.getLogger(TBSinkConnector.class);

    private TBConnectorConfig config;

    @Override
    public void start(Map<String, String> props) {
        this.config = new TBConnectorConfig(config(), props);

        try (DXTickDB timebase = TickDBFactory.createFromUrl(config.getTBUrl(), config.getTBUser(), config.getTBPassword())) {
            timebase.open(false);

            DXTickStream msgStream = timebase.getStream(config.getTBStream());
            if (msgStream != null && msgStream.isPolymorphic()) {
                throw new IllegalArgumentException(msgStream.getName() + " is a polymorphic timebase stream");
            }
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TBSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        configs.add(config.originalsStrings());
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return SINK_CONFIG_DEF;
    }

    @Override
    public String version() {
        return VERSION;
    }
}
