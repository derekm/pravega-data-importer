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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Implementation of Flink's DeserializationSchema that is used to deserialize byte arrays.
 */
public class ByteArrayDeserializationFormat implements DeserializationSchema<byte[]> {
    @Override
    public byte[] deserialize(byte[] message) throws IOException {
        return message;
    }

    @Override
    public boolean isEndOfStream(byte[] nextElement) {
        return nextElement == null;
    }

    @Override
    public TypeInformation<byte[]> getProducedType() {
        return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
    }
}