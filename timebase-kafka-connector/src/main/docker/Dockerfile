FROM confluentinc/cp-kafka-connect-base:6.0.0

COPY /tb-kafka /tmp/tb-kafka

RUN confluent-hub install --no-prompt /tmp/tb-kafka/epam-timebase-connector-@pluginVersion@.zip
