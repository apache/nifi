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
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.hbase.scan.Column;
import org.apache.nifi.hbase.scan.ResultHandler;
import org.apache.nifi.hbase.validate.ConfigFilesValidator;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.Collection;

@Tags({"hbase", "client"})
@CapabilityDescription("A controller service for accessing an HBase client.")
public interface HBaseClientService extends ControllerService {

    PropertyDescriptor HADOOP_CONF_FILES = new PropertyDescriptor.Builder()
            .name("Hadoop Configuration Files")
            .description("Comma-separated list of Hadoop Configuration files," +
              " such as hbase-site.xml and core-site.xml for kerberos, " +
              "including full paths to the files.")
            .addValidator(new ConfigFilesValidator())
            .build();

    PropertyDescriptor ZOOKEEPER_QUORUM = new PropertyDescriptor.Builder()
            .name("ZooKeeper Quorum")
            .description("Comma-separated list of ZooKeeper hosts for HBase. Required if Hadoop Configuration Files are not provided.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor ZOOKEEPER_CLIENT_PORT = new PropertyDescriptor.Builder()
            .name("ZooKeeper Client Port")
            .description("The port on which ZooKeeper is accepting client connections. Required if Hadoop Configuration Files are not provided.")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    PropertyDescriptor ZOOKEEPER_ZNODE_PARENT = new PropertyDescriptor.Builder()
            .name("ZooKeeper ZNode Parent")
            .description("The ZooKeeper ZNode Parent value for HBase (example: /hbase). Required if Hadoop Configuration Files are not provided.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor HBASE_CLIENT_RETRIES = new PropertyDescriptor.Builder()
            .name("HBase Client Retries")
            .description("The number of times the HBase client will retry connecting. Required if Hadoop Configuration Files are not provided.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();

    /**
     * Puts a batch of mutations to the given table.
     *
     * @param tableName the name of an HBase table
     * @param puts a list of put mutations for the given table
     * @throws IOException thrown when there are communication errors with HBase
     */
    void put(String tableName, Collection<PutFlowFile> puts) throws IOException;

    /**
     * Puts the given row to HBase with the provided columns.
     *
     * @param tableName the name of an HBase table
     * @param rowId the id of the row to put
     * @param columns the columns of the row to put
     * @throws IOException thrown when there are communication errors with HBase
     */
    void put(String tableName, byte[] rowId, Collection<PutColumn> columns) throws IOException;

    /**
     * Scans the given table using the optional filter criteria and passing each result to the provided handler.
     *
     * @param tableName the name of an HBase table to scan
     * @param columns optional columns to return, if not specified all columns are returned
     * @param filterExpression optional filter expression, if not specified no filtering is performed
     * @param minTime the minimum timestamp of cells to return, passed to the HBase scanner timeRange
     * @param handler a handler to process rows of the result set
     * @throws IOException thrown when there are communication errors with HBase
     */
    void scan(String tableName, Collection<Column> columns, String filterExpression, long minTime, ResultHandler handler) throws IOException;

    /**
     * Converts the given boolean to it's byte representation.
     *
     * @param b a boolean
     * @return the boolean represented as bytes
     */
    byte[] toBytes(boolean b);

    /**
     * Converts the given long to it's byte representation.
     *
     * @param l a long
     * @return the long represented as bytes
     */
    byte[] toBytes(long l);

    /**
     * Converts the given double to it's byte representation.
     *
     * @param d a double
     * @return the double represented as bytes
     */
    byte[] toBytes(double d);

    /**
     * Converts the given string to it's byte representation.
     *
     * @param s a string
     * @return the string represented as bytes
     */
    byte[] toBytes(String s);

    /**
     * Converts the given binary formatted string to a byte representation
     * @param s a binary encoded string
     * @return the string represented as bytes
     */
    byte[] toBytesBinary(String s);

}
