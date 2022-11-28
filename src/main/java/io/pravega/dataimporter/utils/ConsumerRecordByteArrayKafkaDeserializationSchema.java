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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.util.HashMap;

/**
 * An implementation of {@link KafkaRecordDeserializationSchema} that is used to deserialize Kafka's
 * {@link ConsumerRecord} and convert to Data Importer's {@link PravegaRecord}.
 */
public class ConsumerRecordByteArrayKafkaDeserializationSchema implements KafkaRecordDeserializationSchema<PravegaRecord> {

    @Override
    public TypeInformation<PravegaRecord> getProducedType() {
        return TypeInformation.of(PravegaRecord.class);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<PravegaRecord> out) {
        HashMap<String, byte[]> headers = new HashMap<>();
        for (Header header: record.headers()) {
            headers.put(header.key(), header.value());
        }
        out.collect(new PravegaRecord(record.key(), record.value(), headers, record.partition(), record.topic(), record.timestamp()));
    }
}
