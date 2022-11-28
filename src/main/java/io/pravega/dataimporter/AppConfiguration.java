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
package io.pravega.dataimporter;

import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.dataimporter.utils.PravegaKeycloakCredentialsFromString;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;
import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_UNKNOWN;

/**
 * A generic configuration class for Flink Pravega applications.
 * This class can be extended for job-specific configuration parameters.
 */
public class AppConfiguration {
    public static final String ACTION_PARAMETER = "action-type";

    final private static Logger log = LoggerFactory.getLogger(AppConfiguration.class);

    private final Map<String, String> params;
    private final int parallelism;
    private final int readerParallelism;
    private final long checkpointIntervalMs;
    private final long checkpointTimeoutMs;
    private final boolean enableCheckpoint;
    private final boolean enableOperatorChaining;
    private final boolean enableRebalance;
    private final long maxOutOfOrdernessMs;
    private final String jobName;
    private final String avroSchema;

    /**
     * Creates a new instance of the AppConfiguration class.
     * 
     * @param args command-line arguments
     * @throws IOException failed to read avroSchemaFile
     */
    private AppConfiguration(Map<String, String> args) throws IOException {
        params = args;
        log.info("Parameter Tool: {}", getParams().toMap());
        parallelism = getParams().getInt("parallelism", PARALLELISM_UNKNOWN);
        readerParallelism = getParams().getInt("readerParallelism", PARALLELISM_DEFAULT);
        checkpointIntervalMs = getParams().getLong("checkpointIntervalMs", 10000);
        checkpointTimeoutMs = getParams().getLong("checkpointTimeoutMs", 20000);
        enableCheckpoint = getParams().getBoolean("enableCheckpoint", true);
        enableOperatorChaining = getParams().getBoolean("enableOperatorChaining", true);
        enableRebalance = getParams().getBoolean("rebalance", true);
        maxOutOfOrdernessMs = getParams().getLong("maxOutOfOrdernessMs", 1000);
        jobName = getParams().get("jobName");

        // Get Avro schema from base-64 encoded string parameter or from a file.
        final String avroSchemaBase64 = getParams().get("avroSchema", "");
        if (avroSchemaBase64.isEmpty()) {
            final String avroSchemaFileName = getParams().get("avroSchemaFile", "");
            if (avroSchemaFileName.isEmpty()) {
                avroSchema = "";
            } else {
                avroSchema = Files.readString(Paths.get(avroSchemaFileName));
            }
        } else {
            avroSchema = new String(Base64.getDecoder().decode(avroSchemaBase64), StandardCharsets.UTF_8);
        }
    }

    /**
     * Static factory method that returns a new {@link AppConfiguration}.
     * @param args command-line arguments
     * @return new AppConfiguration of parsed arguments
     * @throws IOException failed to read avroSchemaFile
     */
    public static AppConfiguration createAppConfiguration(Map<String, String> args) throws IOException {
        return new AppConfiguration(args);
    }

    /**
     * Returns String representation of AppConfiguration.
     */
    @Override
    public String toString() {
        return "AppConfiguration{" +
                "parallelism=" + parallelism +
                ", readerParallelism=" + readerParallelism +
                ", checkpointIntervalMs=" + checkpointIntervalMs +
                ", checkpointTimeoutMs=" + checkpointTimeoutMs +
                ", enableCheckpoint=" + enableCheckpoint +
                ", enableOperatorChaining=" + enableOperatorChaining +
                ", enableRebalance=" + enableRebalance +
                ", maxOutOfOrdernessMs=" + maxOutOfOrdernessMs +
                ", jobName=" + jobName +
                ", avroSchema=" + avroSchema +
                '}';
    }

    /**
     * Returns application parameters.
     */
    public ParameterTool getParams() {
        return ParameterTool.fromMap(params);
    }

    /**
     * Returns stream configuration.
     *
     * @param argName Info about the stream such as "input" or "output"
     */
    public StreamConfig getStreamConfig(final String argName) {
        return new StreamConfig(argName,  getParams());
    }

    /**
     * Returns parallelism for the app configuration.
     */
    public int getParallelism() {
        return parallelism;
    }

    /**
     * Returns reader parallelism for the app configuration.
     */
    public int getReaderParallelism() {
        return readerParallelism;
    }

    /**
     * Returns checkpoint timeout in milliseconds for the app configuration.
     */
    public long getCheckpointTimeoutMs() {
        return checkpointTimeoutMs;
    }

    /**
     * Returns checkpoint interval in milliseconds for the app configuration.
     */
    public long getCheckpointIntervalMs() {
        return checkpointIntervalMs;
    }

    /**
     * Returns if checkpointing is enabled for the app configuration.
     */
    public boolean isEnableCheckpoint() {
        return enableCheckpoint;
    }

    /**
     * Returns if operator chaining is enabled for the app configuration.
     */
    public boolean isEnableOperatorChaining() {
        return enableOperatorChaining;
    }

    /**
     * Returns rebalancing is enabled for the app configuration.
     */
    public boolean isEnableRebalance() {
        return enableRebalance;
    }

    /**
     * Returns maximum out of orderness in milliseconds for the app configuration.
     */
    public long getMaxOutOfOrdernessMs() {
        return maxOutOfOrdernessMs;
    }

    /**
     * Returns job name for the app configuration.
     * @param defaultJobName the default job name to return if jobName is not set
     */
    public String getJobName(String defaultJobName) {
        return (jobName == null) ? defaultJobName : jobName;
    }

    /**
     * Returns Avro schema for the app configuration.
     */
    public String getAvroSchema() {
        return avroSchema;
    }

    /**
     * Inner class that represents the configuration for a Pravega Stream.
     */
    public static class StreamConfig {
        private final Stream stream;
        private final PravegaConfig pravegaConfig;
        private final int targetRate;
        private final int scaleFactor;
        private final int minNumSegments;
        private final StreamCut startStreamCut;
        private final StreamCut endStreamCut;
        private final boolean startAtTail;
        private final boolean endAtTail;

        /**
         * Creates a new instance of the StreamConfig class.
         * @param argName prefix string used to pull matching stream parameters
         * @param globalParams all parameters provided to the application
         */
        public StreamConfig(final String argName, final ParameterTool globalParams) {
            final String argPrefix = argName.isEmpty() ? argName : argName + "-";

            // Build ParameterTool parameters with stream-specific parameters copied to global parameters.
            Map<String, String> streamParamsMap = new HashMap<>(globalParams.toMap());
            globalParams.toMap().forEach((k, v) -> {
                if (k.startsWith(argPrefix)) {
                    streamParamsMap.put(k.substring(argPrefix.length()), v);
                }
            });
            ParameterTool params = ParameterTool.fromMap(streamParamsMap);
            final String streamSpec = globalParams.getRequired(argPrefix + "stream");
            log.info("Parameters for {} stream {}: {}", argName, streamSpec, params.toMap());

            // Build Pravega config for this stream.
            PravegaConfig tempPravegaConfig = PravegaConfig.fromParams(params);
            stream = tempPravegaConfig.resolve(streamSpec);
            // Copy stream's scope to default scope.
            tempPravegaConfig = tempPravegaConfig.withDefaultScope(stream.getScope());

            final String keycloakConfigBase64 = params.get("keycloak", "");
            if (!keycloakConfigBase64.isEmpty()) {
                // Add Keycloak credentials. This is decoded as base64 to avoid complications with JSON in arguments.
                log.info("Loading base64-encoded Keycloak credentials from parameter {}keycloak.", argPrefix);
                final String keycloakConfig = new String(Base64.getDecoder().decode(keycloakConfigBase64), StandardCharsets.UTF_8);
                tempPravegaConfig = tempPravegaConfig.withCredentials(new PravegaKeycloakCredentialsFromString(keycloakConfig));
            } else {
                // Add username/password credentials.
                final String username = params.get("username", "");
                final String password = params.get("password", "");
                if (!username.isEmpty() || !password.isEmpty()) {
                    tempPravegaConfig = tempPravegaConfig.withCredentials(new DefaultCredentials(password, username));
                }
            }

            pravegaConfig = tempPravegaConfig;
            targetRate = params.getInt("targetRate", 10 * 1024 * 1024);  // data rate in KiB/sec
            scaleFactor = params.getInt("scaleFactor", 2);
            minNumSegments = params.getInt("minNumSegments", 1);
            startStreamCut = StreamCut.from(params.get("startStreamCut", StreamCut.UNBOUNDED.asText()));
            endStreamCut = StreamCut.from(params.get("endStreamCut", StreamCut.UNBOUNDED.asText()));
            startAtTail = params.getBoolean( "startAtTail", false);
            endAtTail = params.getBoolean("endAtTail", false);
        }

        /**
         * Returns StreamConfig in String format.
         */
        @Override
        public String toString() {
            return "StreamConfig{" +
                    "stream=" + stream +
                    ", pravegaConfig=" + pravegaConfig.getClientConfig() +
                    ", targetRate=" + targetRate +
                    ", scaleFactor=" + scaleFactor +
                    ", minNumSegments=" + minNumSegments +
                    ", startStreamCut=" + startStreamCut +
                    ", endStreamCut=" + endStreamCut +
                    ", startAtTail=" + startAtTail +
                    ", endAtTail=" + endAtTail +
                    '}';
        }

        /**
         * Returns Pravega Stream for the StreamConfig.
         */
        public Stream getStream() {
            return stream;
        }

        /**
         * Returns PravegaConfig for the StreamConfig.
         */
        public PravegaConfig getPravegaConfig() {
            return pravegaConfig;
        }

        /**
         * Returns Scaling Policy for the StreamConfig.
         */
        public ScalingPolicy getScalingPolicy() {
            return ScalingPolicy.byDataRate(targetRate, scaleFactor, minNumSegments);
        }

        /**
         * Returns Starting StreamCut for the given StreamConfig.
         */
        public StreamCut getStartStreamCut() {
            return startStreamCut;
        }

        /**
         * Returns ending StreamCut for the given StreamConfig.
         */
        public StreamCut getEndStreamCut() {
            return endStreamCut;
        }

        /**
         * Returns if the start is at the tail of the Pravega Stream for the given StreamConfig.
         */
        public boolean isStartAtTail() {
            return startAtTail;
        }

        /**
         * Returns if the end is at the tail of the Pravega Stream for the given StreamConfig.
         */
        public boolean isEndAtTail() {
            return endAtTail;
        }
    }
}
