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

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;

/**
 * A class containing data extracted from Apache Kafka's {@link org.apache.kafka.clients.consumer.ConsumerRecord}
 * that can be useful when forwarding to a Pravega stream.
 */
public class PravegaRecord implements Serializable {

    private final byte[] key;
    private final byte[] value;
    private final HashMap<String, byte[]> headers;
    private final int partition;
    private final String topic;
    private final long timestamp;

    /**
     * Creates a new instance of the PravegaRecord class.
     * @param key the key of the record
     * @param value the value of the record
     * @param headers the headers in the record
     * @param partition the partition in Kafka that the record came from
     * @param topic the topic in Kafka that the record came from
     * @param timestamp the timestamp of the record in Kafka
     */
    public PravegaRecord(byte[] key, byte[] value, HashMap<String, byte[]> headers, int partition, String topic,
                         long timestamp) {
        this.key = key;
        this.value = value;
        this.partition = partition;
        this.headers = new HashMap<>(headers);
        this.topic = topic;
        this.timestamp = timestamp;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public HashMap<String, byte[]> getHeaders() {
        return new HashMap<>(headers);
    }

    public int getPartition() {
        return partition;
    }

    public String getTopic() {
        return topic;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "PravegaRecord{" +
                "key=" + Arrays.toString(key) +
                ", value=" + Arrays.toString(value) +
                ", headers=" + headers +
                ", partition=" + partition +
                ", topic='" + topic + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PravegaRecord that = (PravegaRecord) o;
        return partition == that.partition
                && timestamp == that.timestamp
                && Arrays.equals(key, that.key)
                && Arrays.equals(value, that.value)
                && Objects.equals(headers, that.headers)
                && Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(headers, partition, topic, timestamp);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }
}
