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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;

import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.Deserializer;
import java.io.ByteArrayOutputStream;
import org.apache.nifi.reporting.InitializationException;

import java.nio.charset.StandardCharsets;
import org.apache.nifi.hbase.scan.ResultCell;
import org.apache.nifi.hbase.scan.ResultHandler;
import org.apache.nifi.hbase.scan.Column;
import org.apache.nifi.hbase.put.PutColumn;


import org.apache.nifi.processor.util.StandardValidators;

@Tags({"distributed", "cache", "state", "map", "cluster","hbase"})
@SeeAlso(classNames = {"org.apache.nifi.distributed.cache.server.map.DistributedMapCacheClient", "org.apache.nifi.hbase.HBase_1_1_2_ClientService"})
@CapabilityDescription("Provides the ability to use an HBase table as a cache, in place of a DistributedMapCache."
    + " Uses a HBase_1_1_2_ClientService controller to communicate with HBase.")

public class HBase_1_1_2_ClientMapCacheService extends AbstractControllerService implements DistributedMapCacheClient {

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
        .expressionLanguageSupported(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor HBASE_COLUMN_FAMILY = new PropertyDescriptor.Builder()
        .name("HBase Column Family")
        .description("Name of the column family on HBase to use for the cache.")
        .required(true)
        .expressionLanguageSupported(true)
        .defaultValue("f")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor HBASE_COLUMN_QUALIFIER = new PropertyDescriptor.Builder()
        .name("HBase Column Qualifier")
        .description("Name of the column qualifier on HBase to use for the cache")
        .defaultValue("q")
        .required(true)
        .expressionLanguageSupported(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(HBASE_CACHE_TABLE_NAME);
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

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException{
        hBaseClientService   = context.getProperty(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class);

        hBaseCacheTableName  = context.getProperty(HBASE_CACHE_TABLE_NAME).evaluateAttributeExpressions().getValue();
        hBaseColumnFamily    = context.getProperty(HBASE_COLUMN_FAMILY).evaluateAttributeExpressions().getValue();
        hBaseColumnQualifier = context.getProperty(HBASE_COLUMN_QUALIFIER).evaluateAttributeExpressions().getValue();

        hBaseColumnFamilyBytes    = hBaseColumnFamily.getBytes(StandardCharsets.UTF_8);
        hBaseColumnQualifierBytes = hBaseColumnQualifier.getBytes(StandardCharsets.UTF_8);
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
      final PutColumn putColumn = new PutColumn(hBaseColumnFamilyBytes, hBaseColumnQualifierBytes, valueBytes);

      return hBaseClientService.checkAndPut(hBaseCacheTableName, rowIdBytes, hBaseColumnFamilyBytes, hBaseColumnQualifierBytes, null, putColumn);
    }

    @Override
    public <K, V> void put(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {

        List<PutColumn> putColumns = new ArrayList<PutColumn>(1);
        final byte[] rowIdBytes = serialize(key, keySerializer);
        final byte[] valueBytes = serialize(value, valueSerializer);

        final PutColumn putColumn = new PutColumn(hBaseColumnFamilyBytes, hBaseColumnQualifierBytes, valueBytes);
        putColumns.add(putColumn);

        hBaseClientService.put(hBaseCacheTableName, rowIdBytes, putColumns);
    }

    @Override
    public <K> boolean containsKey(final K key, final Serializer<K> keySerializer) throws IOException {
      final byte[] rowIdBytes = serialize(key, keySerializer);
      final HBaseRowHandler handler = new HBaseRowHandler();

      final List<Column> columnsList = new ArrayList<Column>(0);

      hBaseClientService.scan(hBaseCacheTableName, rowIdBytes, rowIdBytes, columnsList, handler);
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

      hBaseClientService.scan(hBaseCacheTableName, rowIdBytes, rowIdBytes, columnsList, handler);
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
            hBaseClientService.delete(hBaseCacheTableName, rowIdBytes);
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
