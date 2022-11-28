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
package io.pravega.dataimporter.cli;

import io.pravega.dataimporter.actions.ActionFactory;
import picocli.CommandLine;

import java.util.HashMap;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "stream-mirroring",
        description = "Initiates Pravega Stream Mirroring Command",
        mixinStandardHelpOptions = true)
public class PravegaMirroringCommand implements Callable<Integer> {

    @CommandLine.Option(
            names = "input-stream",
            description = "Scoped Pravega stream name that data-importer reads from.",
            required = true)
    String inputStream;
    @CommandLine.Option(
            names = "input-controller",
            description = "Pravega Controller URL of input stream.",
            defaultValue = "tcp://localhost:9090")
    String inputController = "tcp://localhost:9090";
    @CommandLine.Option(
            names = "input-startAtTail",
            description = "Choose whether the input stream starts at the tail of the stream or not.")
    boolean inputStartAtTail = false;

    @CommandLine.Option(
            names = "isStreamOrdered",
            description = "Choose whether input stream is ordered or not.")
    boolean isStreamOrdered = true;

    @CommandLine.Option(
            names = "output-stream",
            description = "Scoped Pravega stream name that data-importer writes to.",
            required = true)
    String outputStream;
    @CommandLine.Option(
            names = "output-controller",
            description = "Pravega Controller URL of output stream.",
            defaultValue = "tcp://localhost:9090")
    String outputController = "tcp://localhost:9090";

    @CommandLine.Option(
            names = "flink-host",
            description = "Flink Host Name (e.g. localhost)",
            defaultValue = "localhost")
    String flinkHost = "localhost";
    @CommandLine.Option(
            names = "flink-port",
            description = "Flink Port Number (e.g. 8081)",
            defaultValue = "8081")
    int flinkPort = 8081;

    @Override
    public Integer call() {
        // When this method is executed, we should expect the following:
        // 1. Parameters specifying the job to be executed (e.g., mirroring, import)
        // 2. Parameters specifying the required metadata information for a given job (e.g., origin/target cluster endpoints)
        // 3. Parameters related to the Flink job itself to be executed (e.g., parallelism)
        HashMap<String, String> argsMap = new HashMap<>();
        argsMap.put("action-type", "stream-mirroring");
        argsMap.put("input-stream", inputStream);
        argsMap.put("input-controller", inputController);
        argsMap.put("input-startAtTail", String.valueOf(inputStartAtTail));
        argsMap.put("isStreamOrdered", String.valueOf(isStreamOrdered));
        argsMap.put("output-stream", outputStream);
        argsMap.put("output-controller", outputController);
        argsMap.put("flinkHost", flinkHost);
        argsMap.put("flinkPort", String.valueOf(flinkPort));

        return ActionFactory.createActionSubmitJob(argsMap);
    }
}
