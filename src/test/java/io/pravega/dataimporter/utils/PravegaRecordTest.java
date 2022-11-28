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
package io.pravega.dataimporter.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link PravegaRecord} and {@link ConsumerRecordByteArrayKafkaDeserializationSchema}
 */
@Slf4j
public class PravegaRecordTest {

    /**
     * Tests deserialization of {@link org.apache.kafka.clients.consumer.ConsumerRecord} and
     * conversion into {@link PravegaRecord} by {@link ConsumerRecordByteArrayKafkaDeserializationSchema}
     */
    @Test
    public void testPravegaRecord() {
        final String topic = "topic";
        final int partition = 0;
        final long offset = 0L;
        final byte[] key = "key1".getBytes();
        final byte[] value = "value1".getBytes();
        final HashMap<String, byte[]> headers = new HashMap<>();
        ConsumerRecord<byte[], byte[]> record1 = new ConsumerRecord<>(topic, partition, offset, key, value);
        PravegaRecord expected = new PravegaRecord(key, value, headers, partition, topic, record1.timestamp());

        ConsumerRecordByteArrayKafkaDeserializationSchema deserializationSchema = new ConsumerRecordByteArrayKafkaDeserializationSchema();

        ArrayList<PravegaRecord> outList = new ArrayList<>();

        ListCollector<PravegaRecord> collector = new ListCollector<>(outList);

        deserializationSchema.deserialize(record1, collector);

        PravegaRecord actual = outList.get(0);

        assertEquals(expected, actual);
    }
}
