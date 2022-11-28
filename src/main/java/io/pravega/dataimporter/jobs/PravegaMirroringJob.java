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

import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.dataimporter.AppConfiguration;
import io.pravega.dataimporter.utils.ByteArrayDeserializationFormat;
import io.pravega.dataimporter.utils.ByteArraySerializationFormat;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Continuously copy a Pravega stream to another Pravega stream.
 * This supports events with any serialization.
 * When writing events, a fixed routing key is used.
 */
public class PravegaMirroringJob extends AbstractJob {

    final private static Logger log = LoggerFactory.getLogger(PravegaMirroringJob.class);

    final StreamExecutionEnvironment env;

    final String jobName = getConfig().getJobName(PravegaMirroringJob.class.getName());

    /**
     * Creates a new instance of the PravegaMirroringJob class.
     *
     * @param appConfiguration The application parameters needed for configuration of a PravegaMirroringJob.
     */
    public PravegaMirroringJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
        env = initializeFlinkStreaming(appConfiguration, true);
    }

    /**
     * Static factory for creating a FlinkPravegaReader. This is used in the Flink job as well as testing.
     *
     * @param inputStreamConfig Input Stream configuration needed for {@link FlinkPravegaReader}
     * @param startStreamCut Starting stream cut
     * @param endStreamCut Ending stream cut
     */
    public static FlinkPravegaReader<byte[]> createFlinkPravegaReader(AppConfiguration.StreamConfig inputStreamConfig,
                                                                      StreamCut startStreamCut,
                                                                      StreamCut endStreamCut) {
        return FlinkPravegaReader.<byte[]>builder()
                .withPravegaConfig(inputStreamConfig.getPravegaConfig())
                .forStream(inputStreamConfig.getStream(), startStreamCut, endStreamCut)
                .withDeserializationSchema(new ByteArrayDeserializationFormat())
                .build();
    }

    /**
     * Static factory for creating a FlinkPravegaWriter. This is used in the Flink job as well as testing.
     *
     * @param outputStreamConfig Output Stream configuration needed for {@link FlinkPravegaWriter}
     * @param isStreamOrdered boolean used to determine if writes in input stream are ordered. If true, routing key is
     *                        taken from current thread name. Else, it is an unordered write.
     * @param pravegaWriterMode The writer mode of BEST_EFFORT, ATLEAST_ONCE, or EXACTLY_ONCE.
     */
    public static FlinkPravegaWriter<byte[]> createFlinkPravegaWriter(AppConfiguration.StreamConfig outputStreamConfig,
                                                               boolean isStreamOrdered,
                                                               PravegaWriterMode pravegaWriterMode) {
        FlinkPravegaWriter.Builder<byte[]> flinkPravegaWriterBuilder = FlinkPravegaWriter.<byte[]>builder()
                .withPravegaConfig(outputStreamConfig.getPravegaConfig())
                .forStream(outputStreamConfig.getStream())
                .withSerializationSchema(new ByteArraySerializationFormat());
        if (isStreamOrdered) {
            //ordered write, multi-partition. routing key taken from current thread name
            flinkPravegaWriterBuilder.withEventRouter(event -> Thread.currentThread().getName());
        }
        flinkPravegaWriterBuilder.withWriterMode(pravegaWriterMode);

        return flinkPravegaWriterBuilder.build();
    }

    /**
     * Creates Pravega source and Pravega sink and submits to Flink cluster.
     */
    public JobClient submitJob() {
        try {
            final AppConfiguration.StreamConfig inputStreamConfig = getConfig().getStreamConfig("input");

            final StreamCut startStreamCut = resolveStartStreamCut(inputStreamConfig);
            final StreamCut endStreamCut = resolveEndStreamCut(inputStreamConfig);
            final AppConfiguration.StreamConfig outputStreamConfig = getConfig().getStreamConfig("output");

            final boolean isStreamOrdered = getConfig().getParams().getBoolean("isStreamOrdered", true);
            log.info("isStreamOrdered: {}", isStreamOrdered);

            final FlinkPravegaReader<byte[]> flinkPravegaReader = createFlinkPravegaReader(inputStreamConfig,
                    startStreamCut,
                    endStreamCut);
            final DataStream<byte[]> events = env
                    .addSource(flinkPravegaReader)
                    .uid("pravega-reader")
                    .name("Pravega reader from " + inputStreamConfig.getStream().getScopedName());

            final FlinkPravegaWriter<byte[]> sink = createFlinkPravegaWriter(outputStreamConfig,
                    isStreamOrdered,
                    PravegaWriterMode.EXACTLY_ONCE);

            events.addSink(sink)
                    .uid("pravega-writer")
                    .name("Pravega writer to " + outputStreamConfig.getStream().getScopedName());

            log.info("Executing {} job", jobName);
            return env.executeAsync(jobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}