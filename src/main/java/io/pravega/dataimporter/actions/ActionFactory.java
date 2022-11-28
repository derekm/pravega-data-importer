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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.dataimporter.AppConfiguration;

import java.io.IOException;
import java.util.Map;

/**
 * Factory class that is used to instantiate concrete implementations of AbstractAction.
 */
public class ActionFactory {

    /**
     * Method that instantiates concrete implementations of AbstractAction from an input String and AppConfiguration.
     */
    @VisibleForTesting
    static AbstractAction instantiateAction(String actionType, AppConfiguration configuration) {
        switch (actionType) {

            case PravegaMirroringAction.NAME:
                return new PravegaMirroringAction(configuration);
            case KafkaMirroringAction.NAME:
                return new KafkaMirroringAction(configuration);
            default:
                throw new IllegalArgumentException("Unknown action type.");
        }
    }

    public static int createActionSubmitJob(Map<String, String> argsMap) {
        AppConfiguration configuration;
        try {
            configuration = AppConfiguration.createAppConfiguration(argsMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // STEP 1: Instantiate the Action based on input parameter
        String actionType = configuration.getParams().get(AppConfiguration.ACTION_PARAMETER);
        AbstractAction dataImportAction = ActionFactory.instantiateAction(actionType, configuration);

        // STEP 2: Run the metadata workflow for the action.
        dataImportAction.commitMetadataChanges();

        // STEP 3: Submit the associated job to Flink.
        dataImportAction.submitDataImportJob();
        return 0;
    }
}
