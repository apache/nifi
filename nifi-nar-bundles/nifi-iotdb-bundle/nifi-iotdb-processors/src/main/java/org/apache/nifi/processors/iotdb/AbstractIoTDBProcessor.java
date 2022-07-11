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
package org.apache.nifi.processors.iotdb;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;


/**
 * Abstract base class for IoTDB processors
 */
public abstract class AbstractIoTDBProcessor extends AbstractProcessor {

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("IoTDB-charset")
            .displayName("Character Set")
            .description("Specifies the character set of the document data.")
            .required(true)
            .defaultValue("UTF-8")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    public static final PropertyDescriptor IOTDB_DB_URL = new PropertyDescriptor.Builder()
            .name("IoTDB-url")
            .displayName("IoTDB connection URL")
            .description("IoTDB URL to connect to. Eg: 127.0.0.1")
            .defaultValue("127.0.0.1")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor IOTDB_JDBC_URL = new PropertyDescriptor.Builder()
        .name("IoTDB-url")
        .displayName("IoTDB connection URL")
        .description("IoTDB URL to connect to. Eg: jdbc:iotdb://127.0.0.1:6667/")
        .defaultValue("jdbc:iotdb://127.0.0.1:6667/")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();


    public static final PropertyDescriptor IOTDB_PORT = new PropertyDescriptor.Builder()
        .name("IoTDB RPC Port")
        .displayName("IoTDB RPC Port")
        .description("IoTDB RPC Port")
        .defaultValue("6667")
        .required(true)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .build();


    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("IoTDB-username")
            .displayName("Username")
            .required(false)
            .description("Username for accessing IoTDB")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("IoTDB-password")
            .displayName("Password")
            .required(false)
            .description("Password for user")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor MAX_RECORDS_SIZE = new PropertyDescriptor.Builder()
            .name("iotdb-max-records-size")
            .displayName("Max size of records")
            .description("Maximum size of records allowed to be posted in one batch")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("1 MB")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final String IOTDB_ERROR_MESSAGE = "iotdb.error.message";
    protected AtomicReference<Session> iotdbSession = new AtomicReference<>();
    protected AtomicReference<Connection> iotdbConnection = new AtomicReference<>();
    protected long maxRecordsSize;

    /**
     * Helper method to create IoTDB instance
     * @return IoTDB instance
     */

    protected synchronized Session getIoTDBSession(ProcessContext context) {
        if ( iotdbSession.get() == null ) {
            String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
            String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
            int rpcPort= context.getProperty(IOTDB_PORT).asInteger();
            String ioTDBUrl = context.getProperty(IOTDB_DB_URL).evaluateAttributeExpressions().getValue();
            try {
                iotdbSession.set(new Session(ioTDBUrl,rpcPort,username,password));
            } catch(Exception e) {
                getLogger().error("Error while getting connection {}", new Object[] { e.getLocalizedMessage() },e);
                throw new RuntimeException("Error while getting connection " + e.getLocalizedMessage(),e);
            }
            getLogger().info("IoTDB connection created for host {}",
                ioTDBUrl);
        }
        return iotdbSession.get();
    }

    protected synchronized Connection getIoTDBconnection(ProcessContext context) {
        if ( iotdbConnection.get() == null ) {
            String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
            String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
            String jdbcUrl = context.getProperty(IOTDB_JDBC_URL).evaluateAttributeExpressions().getValue();
            try {
                Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
                iotdbConnection.set(DriverManager.getConnection(jdbcUrl,username,password));
            } catch(Exception e) {
                getLogger().error("Error while getting connection {}", new Object[] { e.getLocalizedMessage() },e);
                throw new RuntimeException("Error while getting connection " + e.getLocalizedMessage(),e);
            }
            getLogger().info("IoTDB connection created for host {}",
                IOTDB_JDBC_URL);
        }
        return iotdbConnection.get();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    }


    @OnStopped
    public void close()  {
        if (getLogger().isDebugEnabled()) {
            getLogger().info("Closing connection");
        }
        try {
            if (iotdbSession.get()!= null ) {
                iotdbSession.get().close();
                iotdbSession.set(null);
            }
            if(iotdbConnection.get()!=null){
                iotdbConnection.get().close();
                iotdbConnection.set(null);
            }
        } catch (IoTDBConnectionException | SQLException e) {
            e.printStackTrace();
        }

    }
}