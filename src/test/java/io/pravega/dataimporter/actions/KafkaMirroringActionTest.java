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
import io.pravega.dataimporter.AppConfiguration;
import io.pravega.dataimporter.PravegaEmulatorResource;
import lombok.Cleanup;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test class for KafkaMirroringAction
 */
public class KafkaMirroringActionTest {

    @ClassRule
    public static final PravegaEmulatorResource EMULATOR = PravegaEmulatorResource.builder().build();

    final String streamScope = "testScope";
    final String streamName = "testStream";

    /**
     * Tests KafkaMirroringAction metadata changes.
     * Checks if action creates output stream with correct stream tag.
     */
    @Test
    public void testKafkaMirroringAction() {

        String controllerURI = EMULATOR.getControllerURI();

        final String scopedStreamName = streamScope + "/" + streamName;

        @Cleanup
        StreamManager streamManager = StreamManager.create(URI.create(controllerURI));
        final boolean beforeCheck = streamManager.checkStreamExists(streamScope, streamName);

        HashMap<String, String> argsMap = new HashMap<>();
        argsMap.put(AppConfiguration.ACTION_PARAMETER, KafkaMirroringAction.NAME);
        argsMap.put("output-stream", scopedStreamName);
        AppConfiguration configuration;
        try {
            configuration = AppConfiguration.createAppConfiguration(argsMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String actionType = configuration.getParams().get(AppConfiguration.ACTION_PARAMETER);
        ActionFactory.instantiateAction(actionType, configuration);

        final boolean afterCheck = streamManager.checkStreamExists(streamScope, streamName);

        assertTrue(!beforeCheck && afterCheck);
        assertTrue(streamManager.getStreamTags(streamScope, streamName).contains(KafkaMirroringAction.NAME));

    }
}
