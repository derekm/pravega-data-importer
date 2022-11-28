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

import io.pravega.dataimporter.AppConfiguration;
import io.pravega.dataimporter.jobs.KafkaMirroringJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that represents the workflow for a Kafka Mirroring Action.
 * The action will create or connect to an existing stream on the output cluster.
 */
public class KafkaMirroringAction extends AbstractAction {
    public final static String NAME = "kafka-stream-mirroring";

    final private static Logger log = LoggerFactory.getLogger(KafkaMirroringAction.class);

    private final AppConfiguration config;

    /**
     * Creates a new instance of the KafkaMirroringAction class.
     * @param config app configs
     */
    public KafkaMirroringAction(AppConfiguration config) {
        this.config = config;
        super.job = new KafkaMirroringJob(this.config);
    }

    /**
     * Returns the application configuration passed in during action creation.
     */
    public AppConfiguration getConfig() {
        return config;
    }

    /**
     * Creates the output Pravega stream for the data importer to write to.
     */
    @Override
    public void commitMetadataChanges() {
        final AppConfiguration.StreamConfig outputStreamConfig = getConfig().getStreamConfig("output");
        log.info("output stream: {}", outputStreamConfig);
        AbstractAction.createStream(outputStreamConfig, NAME);
    }

    /**
     * Retrieves name of associated Flink job.
     */
    @Override
    public String getJobName() {
        return job.getClass().getName();
    }
    
}
