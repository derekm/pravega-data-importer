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
package io.pravega.dataimporter.actions;

import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.dataimporter.AppConfiguration;
import io.pravega.dataimporter.jobs.AbstractJob;
import org.apache.flink.core.execution.JobClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface that all Actions should implement (mirroring, importing, etc.).
 */
public abstract class AbstractAction {

    final private static Logger log = LoggerFactory.getLogger(AbstractAction.class);

    protected AbstractJob job;

    /**
     * Most actions may require to perform some metadata changes before actually start doing the actual import job.
     * Changes may involve creating Streams, add tags or any other metadata change that is required to correctly run
     * the data import job afterwards.
     */
    public abstract void commitMetadataChanges();

    /**
     * Returns the name of the job.
     */
    public abstract String getJobName();

    /**
     * Submits Flink job to Flink cluster, and logs the {@link org.apache.flink.api.common.JobID} of the submitted job.
     */
    public void submitDataImportJob() {
        JobClient jobClient = job.submitJob();
        log.info("\n\n\nJob ID: " + jobClient.getJobID().toString() + "\n\n");
    }

    /**
     * If the Pravega stream does not exist, creates a new stream with the specified stream configuration.
     * If the stream exists, it is unchanged.
     * @param streamConfig stream configuration
     * @param streamTag tag to place on the stream
     */
    public static void createStream(AppConfiguration.StreamConfig streamConfig, String streamTag) {
        try (StreamManager streamManager = StreamManager.create(streamConfig.getPravegaConfig().getClientConfig())) {
            StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                    .scalingPolicy(streamConfig.getScalingPolicy())
                    .tag(streamTag)
                    .build();
            streamManager.createStream(
                    streamConfig.getStream().getScope(),
                    streamConfig.getStream().getStreamName(),
                    streamConfiguration);
        }
    }

}
