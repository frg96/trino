/*
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
package io.trino.plugin.kafka;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.airlift.slice.Slice;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.RowDecoder;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.decoder.FieldValueProviders.booleanValueProvider;
import static io.trino.decoder.FieldValueProviders.bytesValueProvider;
import static io.trino.decoder.FieldValueProviders.longValueProvider;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.lang.Math.max;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class KafkaRecordSet
        implements RecordSet
{
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final int CONSUMER_POLL_TIMEOUT = 100;

    private final KafkaSplit split;

    private final KafkaConsumerFactory consumerFactory;
    private final ConnectorSession connectorSession;
    private final RowDecoder keyDecoder;
    private final RowDecoder messageDecoder;
    private final KafkaInternalFieldManager kafkaInternalFieldManager;

    private final List<KafkaColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    KafkaRecordSet(
            KafkaSplit split,
            KafkaConsumerFactory consumerFactory,
            ConnectorSession connectorSession,
            List<KafkaColumnHandle> columnHandles,
            RowDecoder keyDecoder,
            RowDecoder messageDecoder,
            KafkaInternalFieldManager kafkaInternalFieldManager)
    {
        this.split = requireNonNull(split, "split is null");
        this.consumerFactory = requireNonNull(consumerFactory, "consumerFactory is null");
        this.connectorSession = requireNonNull(connectorSession, "connectorSession is null");

        this.keyDecoder = requireNonNull(keyDecoder, "keyDecoder is null");
        this.messageDecoder = requireNonNull(messageDecoder, "messageDecoder is null");

        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.kafkaInternalFieldManager = requireNonNull(kafkaInternalFieldManager, "kafkaInternalFieldManager is null");

        this.columnTypes = columnHandles.stream()
                .map(KafkaColumnHandle::getType)
                .collect(toImmutableList());
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new KafkaRecordCursor();
    }

    private class KafkaRecordCursor
            implements RecordCursor
    {
        private final TopicPartition topicPartition;
        private final KafkaConsumer<byte[], byte[]> kafkaConsumer;
        private Iterator<ConsumerRecord<byte[], byte[]>> records = emptyIterator();
        private long completedBytes;

        private final FieldValueProvider[] currentRowValues = new FieldValueProvider[columnHandles.size()];

        private KafkaRecordCursor()
        {
            topicPartition = new TopicPartition(split.getTopicName(), split.getPartitionId());
            kafkaConsumer = consumerFactory.create(connectorSession);
            kafkaConsumer.assign(ImmutableList.of(topicPartition));
            kafkaConsumer.seek(topicPartition, split.getMessagesRange().getBegin());
        }

        @Override
        public long getCompletedBytes()
        {
            return completedBytes;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            return columnHandles.get(field).getType();
        }

        @Override
        public boolean advanceNextPosition()
        {
            if (records.hasNext()) {
                return nextRow(records.next());
            }
            if (kafkaConsumer.position(topicPartition) >= split.getMessagesRange().getEnd()) {
                return false;
            }
            records = kafkaConsumer.poll(Duration.ofMillis(CONSUMER_POLL_TIMEOUT)).iterator();
            return advanceNextPosition();
        }

        private boolean nextRow(ConsumerRecord<byte[], byte[]> message)
        {
            requireNonNull(message, "message is null");

            if (message.offset() >= split.getMessagesRange().getEnd()) {
                return false;
            }

            completedBytes += max(message.serializedKeySize(), 0) + max(message.serializedValueSize(), 0);

            byte[] keyData = message.key() == null ? EMPTY_BYTE_ARRAY : message.key();
            byte[] messageData = message.value() == null ? EMPTY_BYTE_ARRAY : message.value();
            long timeStamp = message.timestamp() * MICROSECONDS_PER_MILLISECOND;

            Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedKey = keyDecoder.decodeRow(keyData);
            // tombstone message has null value body
            Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedValue = message.value() == null ? Optional.empty() : messageDecoder.decodeRow(messageData);

            Map<ColumnHandle, FieldValueProvider> currentRowValuesMap = columnHandles.stream()
                    .filter(KafkaColumnHandle::isInternal)
                    .collect(toMap(identity(), columnHandle -> switch (getInternalFieldId(columnHandle)) {
                        case PARTITION_OFFSET_FIELD -> longValueProvider(message.offset());
                        case MESSAGE_FIELD -> bytesValueProvider(messageData);
                        case MESSAGE_LENGTH_FIELD -> longValueProvider(messageData.length);
                        case KEY_FIELD -> bytesValueProvider(keyData);
                        case KEY_LENGTH_FIELD -> longValueProvider(keyData.length);
                        case OFFSET_TIMESTAMP_FIELD -> longValueProvider(timeStamp);
                        case KEY_CORRUPT_FIELD -> booleanValueProvider(decodedKey.isEmpty());
                        case HEADERS_FIELD -> headerMapValueProvider((MapType) columnHandle.getType(), message.headers());
                        case MESSAGE_CORRUPT_FIELD -> booleanValueProvider(decodedValue.isEmpty());
                        case PARTITION_ID_FIELD -> longValueProvider(message.partition());
                    }));

            decodedKey.ifPresent(currentRowValuesMap::putAll);
            decodedValue.ifPresent(currentRowValuesMap::putAll);

            for (int i = 0; i < columnHandles.size(); i++) {
                ColumnHandle columnHandle = columnHandles.get(i);
                currentRowValues[i] = currentRowValuesMap.get(columnHandle);
            }

            return true; // Advanced successfully.
        }

        private KafkaInternalFieldManager.InternalFieldId getInternalFieldId(KafkaColumnHandle columnHandle)
        {
            return kafkaInternalFieldManager.getFieldByName(columnHandle.getName()).getInternalFieldId();
        }

        @Override
        public boolean getBoolean(int field)
        {
            return getFieldValueProvider(field, boolean.class).getBoolean();
        }

        @Override
        public long getLong(int field)
        {
            return getFieldValueProvider(field, long.class).getLong();
        }

        @Override
        public double getDouble(int field)
        {
            return getFieldValueProvider(field, double.class).getDouble();
        }

        @Override
        public Slice getSlice(int field)
        {
            return getFieldValueProvider(field, Slice.class).getSlice();
        }

        @Override
        public Object getObject(int field)
        {
            return getFieldValueProvider(field, Block.class).getBlock();
        }

        @Override
        public boolean isNull(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            return currentRowValues[field] == null || currentRowValues[field].isNull();
        }

        private FieldValueProvider getFieldValueProvider(int field, Class<?> expectedType)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            checkFieldType(field, expectedType);
            return currentRowValues[field];
        }

        private void checkFieldType(int field, Class<?> expected)
        {
            Class<?> actual = getType(field).getJavaType();
            checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
        }

        @Override
        public void close()
        {
            kafkaConsumer.close();
        }
    }

    public static FieldValueProvider headerMapValueProvider(MapType varcharMapType, Headers headers)
    {
        Type keyType = varcharMapType.getTypeParameters().get(0);
        Type valueArrayType = varcharMapType.getTypeParameters().get(1);
        Type valueType = valueArrayType.getTypeParameters().get(0);

        // Group by keys and collect values as array.
        Multimap<String, byte[]> headerMap = ArrayListMultimap.create();
        for (Header header : headers) {
            headerMap.put(header.key(), header.value());
        }

        Block map = buildMapValue(
                varcharMapType,
                headerMap.size(),
                (keyBuilder, valueBuilder) -> {
                    for (String headerKey : headerMap.keySet()) {
                        writeNativeValue(keyType, keyBuilder, headerKey);
                        ((ArrayBlockBuilder) valueBuilder).buildEntry(elementBuilder -> {
                            for (byte[] value : headerMap.get(headerKey)) {
                                writeNativeValue(valueType, elementBuilder, value);
                            }
                        });
                    }
                });

        return new FieldValueProvider()
        {
            @Override
            public boolean isNull()
            {
                return false;
            }

            @Override
            public Block getBlock()
            {
                return map;
            }
        };
    }
}
