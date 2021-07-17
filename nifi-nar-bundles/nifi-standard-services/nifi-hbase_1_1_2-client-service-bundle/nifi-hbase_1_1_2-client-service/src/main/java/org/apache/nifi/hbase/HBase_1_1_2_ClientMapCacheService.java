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
package org.apache.nifi.hbase;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.hbase.scan.Column;
import org.apache.nifi.hbase.scan.ResultCell;
import org.apache.nifi.hbase.scan.ResultHandler;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.hbase.VisibilityLabelUtils.AUTHORIZATIONS;

@Tags({"distributed", "cache", "state", "map", "cluster","hbase"})
@SeeAlso(classNames = {"org.apache.nifi.hbase.HBase_1_1_2_ClientService"})
@CapabilityDescription("Provides the ability to use an HBase table as a cache, in place of a DistributedMapCache."
    + " Uses a HBase_1_1_2_ClientService controller to communicate with HBase.")

public class HBase_1_1_2_ClientMapCacheService extends AbstractControllerService implements AtomicDistributedMapCacheClient<byte[]> {

    static final PropertyDescriptor HBASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
        .name("HBase Client Service")
        .description("Specifies the HBase Client Controller Service to use for accessing HBase.")
        .required(true)
        .identifiesControllerService(HBaseClientService.class)
        .build();

    public static final PropertyDescriptor HBASE_CACHE_TABLE_NAME = new PropertyDescriptor.Builder()
        .name("HBase Cache Table Name")
        .description("Name of the table on HBase to use for the cache.")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor HBASE_COLUMN_FAMILY = new PropertyDescriptor.Builder()
        .name("HBase Column Family")
        .description("Name of the column family on HBase to use for the cache.")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .defaultValue("f")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor HBASE_COLUMN_QUALIFIER = new PropertyDescriptor.Builder()
        .name("HBase Column Qualifier")
        .description("Name of the column qualifier on HBase to use for the cache")
        .defaultValue("q")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    public static final PropertyDescriptor VISIBILITY_EXPRESSION = new PropertyDescriptor.Builder()
        .name("hbase-cache-visibility-expression")
        .displayName("Visibility Expression")
        .description("The default visibility expression to apply to cells when visibility expression support is enabled.")
        .defaultValue("")
        .addValidator(Validator.VALID)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .required(false)
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(HBASE_CACHE_TABLE_NAME);
        descriptors.add(AUTHORIZATIONS);
        descriptors.add(VISIBILITY_EXPRESSION);
        descriptors.add(HBASE_CLIENT_SERVICE);
        descriptors.add(HBASE_COLUMN_FAMILY);
        descriptors.add(HBASE_COLUMN_QUALIFIER);
        return descriptors;
    }

    // Other threads may call @OnEnabled so these are marked volatile to ensure other class methods read the updated value
    private volatile String hBaseCacheTableName;
    private volatile HBaseClientService hBaseClientService;

    private volatile String hBaseColumnFamily;
    private volatile byte[] hBaseColumnFamilyBytes;

    private volatile String hBaseColumnQualifier;
    private volatile byte[] hBaseColumnQualifierBytes;

    private List<String> authorizations;
    private String defaultVisibilityExpression;

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException{
        hBaseClientService   = context.getProperty(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class);

        hBaseCacheTableName  = context.getProperty(HBASE_CACHE_TABLE_NAME).evaluateAttributeExpressions().getValue();
        hBaseColumnFamily    = context.getProperty(HBASE_COLUMN_FAMILY).evaluateAttributeExpressions().getValue();
        hBaseColumnQualifier = context.getProperty(HBASE_COLUMN_QUALIFIER).evaluateAttributeExpressions().getValue();

        hBaseColumnFamilyBytes    = hBaseColumnFamily.getBytes(StandardCharsets.UTF_8);
        hBaseColumnQualifierBytes = hBaseColumnQualifier.getBytes(StandardCharsets.UTF_8);

        authorizations = VisibilityLabelUtils.getAuthorizations(context);
        if (context.getProperty(VISIBILITY_EXPRESSION).isSet()) {
            defaultVisibilityExpression = context.getProperty(VISIBILITY_EXPRESSION).evaluateAttributeExpressions().getValue();
        } else {
            defaultVisibilityExpression = null;
        }
    }

    private <T> byte[] serialize(final T value, final Serializer<T> serializer) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(value, baos);
        return baos.toByteArray();
    }
    private <T> T deserialize(final byte[] value, final Deserializer<T> deserializer) throws IOException {
        return deserializer.deserialize(value);
    }


    @Override
    public <K, V> boolean putIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {

      final byte[] rowIdBytes = serialize(key, keySerializer);
      final byte[] valueBytes = serialize(value, valueSerializer);
      final PutColumn putColumn = new PutColumn(hBaseColumnFamilyBytes, hBaseColumnQualifierBytes, valueBytes, defaultVisibilityExpression);

      return hBaseClientService.checkAndPut(hBaseCacheTableName, rowIdBytes, hBaseColumnFamilyBytes, hBaseColumnQualifierBytes, null, putColumn);
    }

    @Override
    public <K, V> void put(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {

        List<PutColumn> putColumns = new ArrayList<PutColumn>(1);
        final byte[] rowIdBytes = serialize(key, keySerializer);
        final byte[] valueBytes = serialize(value, valueSerializer);

        final PutColumn putColumn = new PutColumn(hBaseColumnFamilyBytes, hBaseColumnQualifierBytes, valueBytes, defaultVisibilityExpression);
        putColumns.add(putColumn);

        hBaseClientService.put(hBaseCacheTableName, rowIdBytes, putColumns);
    }

    @Override
    public <K, V> void putAll(Map<K, V> keysAndValues, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        List<PutFlowFile> puts = new ArrayList<>();
        for (Map.Entry<K, V> entry : keysAndValues.entrySet()) {
            List<PutColumn> putColumns = new ArrayList<PutColumn>(1);
            final byte[] rowIdBytes = serialize(entry.getKey(), keySerializer);
            final byte[] valueBytes = serialize(entry.getValue(), valueSerializer);

            final PutColumn putColumn = new PutColumn(hBaseColumnFamilyBytes, hBaseColumnQualifierBytes, valueBytes, defaultVisibilityExpression);
            putColumns.add(putColumn);
            puts.add(new PutFlowFile(hBaseCacheTableName, rowIdBytes, putColumns, null));
        }
        hBaseClientService.put(hBaseCacheTableName, puts);
    }

    @Override
    public <K> boolean containsKey(final K key, final Serializer<K> keySerializer) throws IOException {
      final byte[] rowIdBytes = serialize(key, keySerializer);
      final HBaseRowHandler handler = new HBaseRowHandler();

      final List<Column> columnsList = new ArrayList<Column>(0);
      columnsList.add(new Column(hBaseColumnFamilyBytes, hBaseColumnQualifierBytes));

      hBaseClientService.scan(hBaseCacheTableName, rowIdBytes, rowIdBytes, columnsList, authorizations, handler);
      return (handler.numRows() > 0);
    }

    /**
     *  Note that the implementation of getAndPutIfAbsent is not atomic.
     *  The putIfAbsent is atomic, but a getAndPutIfAbsent does a get and then a putIfAbsent.
     *  If there is an existing value and it is updated in betweern the two steps, then the existing (unmodified) value will be returned.
     *  If the existing value was deleted between the two steps, getAndPutIfAbsent will correctly return null.
     *  This should not generally be an issue with cache processors such as DetectDuplicate.
     *
     */
    @Override
    public <K, V> V getAndPutIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer, final Deserializer<V> valueDeserializer) throws IOException {
      // Between the get and the putIfAbsent, the value could be deleted or updated.
      // Logic below takes care of the deleted case but not the updated case.
      // This is probably fine since DistributedMapCache and DetectDuplicate expect to receive the original cache value
      // Could possibly be fixed by implementing AtomicDistributedMapCache (Map Cache protocol version 2)
      final V got = get(key, keySerializer, valueDeserializer);
      final boolean wasAbsent = putIfAbsent(key, value, keySerializer, valueSerializer);

      if (! wasAbsent) return got;
      else return null;
   }

    @Override
    public <K, V> V get(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
      final byte[] rowIdBytes = serialize(key, keySerializer);
      final HBaseRowHandler handler = new HBaseRowHandler();

      final List<Column> columnsList = new ArrayList<Column>(0);
      columnsList.add(new Column(hBaseColumnFamilyBytes, hBaseColumnQualifierBytes));

      hBaseClientService.scan(hBaseCacheTableName, rowIdBytes, rowIdBytes, columnsList, authorizations, handler);
      if (handler.numRows() > 1) {
          throw new IOException("Found multiple rows in HBase for key");
      } else if(handler.numRows() == 1) {
          return deserialize( handler.getLastResultBytes(), valueDeserializer);
      } else {
          return null;
      }
    }

    @Override
    public <K> boolean remove(final K key, final Serializer<K> keySerializer) throws IOException {
        final boolean contains = containsKey(key, keySerializer);
        if (contains) {
            final byte[] rowIdBytes = serialize(key, keySerializer);
            final DeleteRequest deleteRequest = new DeleteRequest(rowIdBytes, hBaseColumnFamilyBytes, hBaseColumnQualifierBytes, null);
            hBaseClientService.deleteCells(hBaseCacheTableName, Collections.singletonList(deleteRequest));
        }
        return contains;
    }

    @Override
    public long removeByPattern(String regex) throws IOException {
        throw new IOException("HBase removeByPattern is not implemented");
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    protected void finalize() throws Throwable {
    }

    @Override
    public <K, V> AtomicCacheEntry<K, V, byte[]> fetch(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        final byte[] rowIdBytes = serialize(key, keySerializer);
        final HBaseRowHandler handler = new HBaseRowHandler();

        final List<Column> columnsList = new ArrayList<>(1);
        columnsList.add(new Column(hBaseColumnFamilyBytes, hBaseColumnQualifierBytes));

        hBaseClientService.scan(hBaseCacheTableName, rowIdBytes, rowIdBytes, columnsList, authorizations, handler);

        if (handler.numRows() > 1) {
            throw new IOException("Found multiple rows in HBase for key");
        } else if (handler.numRows() == 1) {
            return new AtomicCacheEntry<>(key, deserialize(handler.getLastResultBytes(), valueDeserializer), handler.getLastResultBytes());
        } else {
            return null;
        }
    }

    @Override
    public <K, V> boolean replace(AtomicCacheEntry<K, V, byte[]> entry, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        final byte[] rowIdBytes = serialize(entry.getKey(), keySerializer);
        final byte[] valueBytes = serialize(entry.getValue(), valueSerializer);
        final byte[] revision = entry.getRevision().orElse(null);
        final PutColumn putColumn = new PutColumn(hBaseColumnFamilyBytes, hBaseColumnQualifierBytes, valueBytes, defaultVisibilityExpression);

        // If the current revision is unset then only insert the row if it doesn't already exist.
        return hBaseClientService.checkAndPut(hBaseCacheTableName, rowIdBytes, hBaseColumnFamilyBytes, hBaseColumnQualifierBytes, revision, putColumn);
    }

    private class HBaseRowHandler implements ResultHandler {
        private int numRows = 0;
        private byte[] lastResultBytes;

        @Override
        public void handle(byte[] row, ResultCell[] resultCells) {
            numRows += 1;
            for( final ResultCell resultCell : resultCells ){
                lastResultBytes = Arrays.copyOfRange(resultCell.getValueArray(), resultCell.getValueOffset(), resultCell.getValueLength() + resultCell.getValueOffset());
            }
        }
        public int numRows() {
            return numRows;
        }
        public byte[] getLastResultBytes() {
           return lastResultBytes;
        }
    }

}
