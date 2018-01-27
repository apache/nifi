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
package org.apache.nifi.processors.influxdb;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

/**
 * Abstract base class for InfluxDB processors
 */
abstract class AbstractInfluxDBProcessor extends AbstractProcessor {

    protected static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("influxdb-charset")
            .displayName("Character Set")
            .description("Specifies the character set of the document data.")
            .required(true)
            .defaultValue("UTF-8")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    public static final PropertyDescriptor INFLUX_DB_URL = new PropertyDescriptor.Builder()
            .name("influxdb-url")
            .displayName("InfluxDB connection url")
            .description("InfluxDB url to connect to")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor DB_NAME = new PropertyDescriptor.Builder()
            .name("influxdb-dbname")
            .displayName("DB Name")
            .description("InfluxDB database to connect to")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("influxdb-username")
            .displayName("Username")
            .required(false)
            .description("Username for accessing InfluxDB")
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("influxdb-password")
            .displayName("Password")
            .required(false)
            .description("Password for user")
            .addValidator(Validator.VALID)
            .sensitive(true)
            .build();

    protected static final PropertyDescriptor MAX_RECORDS_SIZE = new PropertyDescriptor.Builder()
            .name("influxdb-max-records-size")
            .displayName("Max size of records")
            .description("Maximum size of records allowed to be posted in one batch")
            .defaultValue("1 MB")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Sucessful FlowFiles are routed to this relationship").build();

    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed FlowFiles are routed to this relationship").build();

    public static final String INFLUX_DB_ERROR_MESSAGE = "influxdb.error.message";

    protected InfluxDB influxDB;
    protected long maxRecordsSize;

    /**
     * Helper method to help testability
     * @return InfluxDB instance
     */
    protected InfluxDB getInfluxDB() {
        return influxDB;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        String username = context.getProperty(USERNAME).getValue();
        String password = context.getProperty(PASSWORD).getValue();
        String influxDbUrl = context.getProperty(INFLUX_DB_URL).getValue();

        maxRecordsSize = context.getProperty(MAX_RECORDS_SIZE).asDataSize(DataUnit.B).longValue();

        try {
            influxDB = makeConnection(username, password, influxDbUrl);
        } catch(Exception e) {
            getLogger().error("Error while getting connection {}", new Object[] { e.getLocalizedMessage() },e);
            throw new RuntimeException("Error while getting connection" + e.getLocalizedMessage(),e);
        }
        getLogger().info("InfluxDB connection created for host {}",
                new Object[] {influxDbUrl});
    }

    protected InfluxDB makeConnection(String username, String password, String influxDbUrl) {
        if ( StringUtils.isBlank(username) || StringUtils.isBlank(password) ) {
            return InfluxDBFactory.connect(influxDbUrl);
        } else {
            return InfluxDBFactory.connect(influxDbUrl, username, password);
        }
    }

    @OnStopped
    public void close() {
        if (getLogger().isDebugEnabled()) {
            getLogger().info("Closing connection");
        }
        if ( influxDB != null ) {
            influxDB.close();
        }
    }
}