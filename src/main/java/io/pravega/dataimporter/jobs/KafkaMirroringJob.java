/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.dataimporter.jobs;

import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.connectors.flink.serialization.PravegaSerializationSchema;
import io.pravega.dataimporter.AppConfiguration;
import io.pravega.dataimporter.utils.ConsumerRecordByteArrayKafkaDeserializationSchema;
import io.pravega.dataimporter.utils.PravegaRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;

/**
 * Continuously copy a Kafka stream to a Pravega stream.
 */
public class KafkaMirroringJob extends AbstractJob {
    final private static Logger log = LoggerFactory.getLogger(KafkaMirroringJob.class);

    private final String jobName = getConfig().getJobName(KafkaMirroringJob.class.getName());

    private final StreamExecutionEnvironment env;

    /**
     * Creates a new instance of the KafkaMirroringJob class.
     *
     * @param appConfiguration The application parameters needed for configuration of a KafkaMirroringJob.
     */
    public KafkaMirroringJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
        env = initializeFlinkStreaming(appConfiguration, true);
    }

    /**
     * Create and configure the KafkaSource to replicate streams from.
     * @param bootstrapServers servers string for KafkaSource.builder().setBootstrapServers()
     * @param kafkaTopic topic to mirror to Pravega
     * @return constructed KafkaSource
     */
    public static KafkaSource<PravegaRecord> createKafkaSourceForPravegaRecord(
            String bootstrapServers,
            String kafkaTopic) {
        return KafkaSource.<PravegaRecord>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(Collections.singletonList(kafkaTopic))
                .setDeserializer(new ConsumerRecordByteArrayKafkaDeserializationSchema())
                .build();
    }

    /**
     * Create and configure the FlinkPravegaWriter to replicate streams to.
     * @param outputStreamConfig stream config
     * @param isStreamOrdered are the events in the source Kafka Topic ordered?
     * @param pravegaWriterMode at-least-once or exactly-once semantics
     * @return constructed FlinkPravegaWriter
     */
    public static FlinkPravegaWriter<PravegaRecord> createFlinkPravegaWriterForPravegaRecord(
            AppConfiguration.StreamConfig outputStreamConfig,
            boolean isStreamOrdered,
            PravegaWriterMode pravegaWriterMode) {
        FlinkPravegaWriter.Builder<PravegaRecord> flinkPravegaWriterBuilder = FlinkPravegaWriter.<PravegaRecord>builder()
                .withPravegaConfig(outputStreamConfig.getPravegaConfig())
                .forStream(outputStreamConfig.getStream())
                .withSerializationSchema(new PravegaSerializationSchema<>(new JavaSerializer<>()));
        if (isStreamOrdered) {
            // ordered write, multi-partition.
            // routing key taken from ConsumerRecord key if exists, else ConsumerRecord partition
            flinkPravegaWriterBuilder.withEventRouter(event ->
                    event.getKey() != null ? Arrays.toString(event.getKey()) : String.valueOf(event.getPartition()));
        }
        flinkPravegaWriterBuilder.withWriterMode(pravegaWriterMode);

        return flinkPravegaWriterBuilder.build();
    }

    /**
     * Creates Kafka source and Pravega sink and submits to Flink cluster.
     */
    public JobClient submitJob() {
        try {
            final AppConfiguration.StreamConfig outputStreamConfig = getConfig().getStreamConfig("output");
            log.info("output stream: {}", outputStreamConfig);

            final boolean isStreamOrdered = getConfig().getParams().getBoolean("isStreamOrdered", true);
            log.info("isStreamOrdered: {}", isStreamOrdered);

            final String bootstrapServers = getConfig().getParams().get(
                    "bootstrap.servers", "localhost:9092");
            final String kafkaTopic = getConfig().getParams().get("input-topic");
            final KafkaSource<PravegaRecord> kafkaSource =
                    createKafkaSourceForPravegaRecord(bootstrapServers, kafkaTopic);

            final DataStream<PravegaRecord> toOutput =
                    env.fromSource(
                            kafkaSource,
                            WatermarkStrategy.noWatermarks(),
                            "Kafka consumer from " + getConfig().getParams().get("input-topic"));

            final FlinkPravegaWriter<PravegaRecord> sink = createFlinkPravegaWriterForPravegaRecord(
                    outputStreamConfig, isStreamOrdered, PravegaWriterMode.EXACTLY_ONCE);

            toOutput
                    .addSink(sink)
                    .uid("pravega-writer")
                    .name("Pravega writer to " + outputStreamConfig.getStream().getScopedName());

            log.info("Executing {} job", jobName);
            return env.executeAsync(jobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
