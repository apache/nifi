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
import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.hbase.scan.Column;
import org.apache.nifi.hbase.scan.ResultHandler;
import org.apache.nifi.hbase.validate.ConfigFilesValidator;

import java.io.IOException;
import java.util.Collection;

@Tags({"hbase", "client"})
@CapabilityDescription("A controller service for accessing an HBase client.")
public interface HBaseClientService extends ControllerService {

    PropertyDescriptor HADOOP_CONF_FILES = new PropertyDescriptor.Builder()
            .name("Hadoop Configuration Files")
            .description("Comma-separated list of Hadoop Configuration files, such as hbase-site.xml")
            .required(true)
            .defaultValue("./conf/hbase-site.xml")
            .addValidator(new ConfigFilesValidator())
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

}
