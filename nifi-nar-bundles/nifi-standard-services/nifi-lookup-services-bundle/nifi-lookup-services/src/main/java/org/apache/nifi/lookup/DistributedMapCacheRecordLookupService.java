/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.lookup;

import java.io.IOException;
import java.io.OutputStream;
import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.HashMap;
import java.io.InputStream;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.util.Tuple;
import org.apache.nifi.lookup.RecordLookupService;

@Tags({ "lookup", "enrich", "key", "value", "map", "cache", "distributed" })
@CapabilityDescription("Allows to choose a distributed map cache client to retrieve the value associated to a key as Record. "
        + "The coordinates that are passed to the lookup must contain the key 'key'.")
public class DistributedMapCacheRecordLookupService extends AbstractControllerService implements RecordLookupService {

    private static final List<Charset> STANDARD_CHARSETS = Arrays.asList(StandardCharsets.UTF_8, StandardCharsets.US_ASCII, StandardCharsets.ISO_8859_1, StandardCharsets.UTF_16,
            StandardCharsets.UTF_16LE, StandardCharsets.UTF_16BE);

    private static final String KEY = "key";
    private static final Set<String> REQUIRED_KEYS = Stream.of(KEY).collect(Collectors.toSet());
    private volatile RecordReaderFactory readerFactory;

    private volatile DistributedMapCacheClient cache;
    private volatile static Charset charset;
    private final Serializer<String> keySerializer = new StringSerializer();
    private final Deserializer<byte[]> valueDeserializer = new CacheValueDeserializer();

    public static final PropertyDescriptor PROP_DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder().name("distributed-map-cache-service").displayName("Distributed Cache Service")
            .description("The Controller Service that is used to get the cached values.").required(true).identifiesControllerService(DistributedMapCacheClient.class).build();

    public static final PropertyDescriptor CHARACTER_ENCODING = new PropertyDescriptor.Builder().name("character-encoding").displayName("Character Encoding")
            .description("Specifies a character encoding to use.").required(true).allowableValues(getStandardCharsetNames()).defaultValue(StandardCharsets.UTF_8.displayName()).build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder().name("record-reader").displayName("Record Reader")
            .description("The Record Reader to use for parsing fetched document from cache Server.").identifiesControllerService(RecordReaderFactory.class).required(true).build();

    private static Set<String> getStandardCharsetNames() {
        return STANDARD_CHARSETS.stream().map(c -> c.displayName()).collect(Collectors.toSet());
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder().name(propertyDescriptorName).required(false).dynamic(true).addValidator(Validator.VALID)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        cache = context.getProperty(PROP_DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);
        charset = Charset.forName(context.getProperty(CHARACTER_ENCODING).getValue());
        readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PROP_DISTRIBUTED_CACHE_SERVICE);
        descriptors.add(CHARACTER_ENCODING);
        descriptors.add(RECORD_READER);
        return descriptors;
    }

    @Override
    public Set<String> getRequiredKeys() {
        return REQUIRED_KEYS;
    }

    public static class StringSerializer implements Serializer<String> {
        @Override
        public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
            out.write(value.getBytes(charset));
        }
    }

    public static class CacheValueDeserializer implements Deserializer<byte[]> {

        @Override
        public byte[] deserialize(final byte[] input) throws DeserializationException, IOException {
            if (input == null || input.length == 0) {
                return null;
            }
            return input;
        }
    }

    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        final Optional<String> docId = Optional.ofNullable(coordinates.get(KEY)).map(Object::toString);
        if (!docId.isPresent()) {
            return Optional.empty();
        }
        final Optional<InputStream> inputStream;
        try {
            Optional<byte[]> byteArrayOutput = Optional.ofNullable(cache.get(docId.get(), keySerializer, valueDeserializer));
            if (byteArrayOutput.isPresent()) {
                inputStream = Optional.ofNullable(new ByteArrayInputStream(cache.get(docId.get(), keySerializer, valueDeserializer)));
            } else {
                return Optional.empty();
            }
        } catch (IOException ie) {
            throw new LookupFailureException("Failed to lookup from cachelookup IOException using this coordinates: " + coordinates);
        } catch (Exception e) {
            throw new LookupFailureException("Failed to lookup from cachelookup using this coordinates: " + coordinates);
        }

        final Optional<Tuple<Exception, RecordReader>> errOrReader = inputStream.map(in -> {
            try {
                // Pass coordinates to initiate RecordReader, so that the reader can resolve schema dynamically.
                // This allow using the same RecordReader service with different schemas if RecordReader is configured to
                // access schema based on Expression Language.
                final Map<String, String> recordReaderVariables = new HashMap<>(coordinates.size());
                coordinates.keySet().forEach(k -> {
                    final Object value = coordinates.get(k);
                    if (value != null) {
                        recordReaderVariables.put(k, value.toString());
                    }
                });
                return new Tuple<>(null, readerFactory.createRecordReader(recordReaderVariables, in, -1, getLogger()));
            } catch (Exception e) {
                return new Tuple<>(e, null);
            }
        });
        if (!errOrReader.isPresent()) {
            return Optional.empty();
        }

        final Exception exception = errOrReader.get().getKey();
        if (exception != null) {
            throw new LookupFailureException(String.format("Failed to lookup with %s", coordinates), exception);
        }

        try {
            return Optional.ofNullable(errOrReader.get().getValue().nextRecord());
        } catch (Exception e) {
            throw new LookupFailureException(String.format("Failed to read Record when looking up with %s", coordinates), e);
        }
    }

}
