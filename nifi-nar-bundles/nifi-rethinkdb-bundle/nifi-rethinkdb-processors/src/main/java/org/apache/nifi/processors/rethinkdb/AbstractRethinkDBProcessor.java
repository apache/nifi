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
package org.apache.nifi.processors.rethinkdb;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.net.Connection;

/**
 * Abstract base class for RethinkDB processors
 */
abstract class AbstractRethinkDBProcessor extends AbstractProcessor {

    protected static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("rethinkdb-charset")
            .displayName("Character Set")
            .description("Specifies the character set of the document data.")
            .required(true)
            .defaultValue("UTF-8")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    public static final PropertyDescriptor DB_NAME = new PropertyDescriptor.Builder()
            .name("rethinkdb-dbname")
            .displayName("DB Name")
            .description("RethinkDB database to connect to")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DB_HOST = new PropertyDescriptor.Builder()
            .name("rethinkdb-host")
            .displayName("Hostname")
            .description("RethinkDB hostname")
            .required(true)
            .defaultValue("localhost")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DB_PORT = new PropertyDescriptor.Builder()
            .name("rethinkdb-port")
            .displayName("DB Port")
            .description("RethinkDB database port to connect to")
            .required(true)
            .defaultValue("28015")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("rethinkdb-username")
            .displayName("Username")
            .description("Username for accessing RethinkDB")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("rethinkdb-password")
            .displayName("Password")
            .description("Password for user")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("rethinkdb-table")
            .displayName("Table name")
            .description("RethinkDB table to connect to")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor MAX_DOCUMENTS_SIZE = new PropertyDescriptor.Builder()
            .name("rethinkdb-max-document-size")
            .displayName("Max size of documents")
            .description("Maximum size of documents allowed to be posted in one batch")
            .defaultValue("1 MB")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor RETHINKDB_DOCUMENT_ID = new PropertyDescriptor.Builder()
                .displayName("Document Identifier")
                .name("rethinkdb-document-identifier")
                .description("A FlowFile attribute, or attribute expression used " +
                    "for determining RethinkDB key for the Flow File content")
                .required(true)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true))
                .expressionLanguageSupported(true)
                .build();

    public static AllowableValue DURABILITY_SOFT = new AllowableValue("soft", "Soft", "Don't save changes to disk before ack");

    public static AllowableValue DURABILITY_HARD = new AllowableValue("hard", "Hard", "Save change to disk before ack");

    protected static final PropertyDescriptor DURABILITY = new PropertyDescriptor.Builder()
                .name("rethinkdb-durability")
                .displayName("Durablity of documents")
                .description("Durability of documents being inserted")
                .required(true)
                .defaultValue("hard")
                .allowableValues(DURABILITY_HARD, DURABILITY_SOFT)
                .expressionLanguageSupported(true)
                .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Sucessful FlowFiles are routed to this relationship").build();

    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed FlowFiles are routed to this relationship").build();

    static final Relationship REL_NOT_FOUND = new Relationship.Builder().name("not_found")
            .description("Document not found are routed to this relationship").build();

    public static final String RESULT_ERROR_KEY = "errors";
    public static final String RESULT_DELETED_KEY = "deleted";
    public static final String RESULT_GENERATED_KEYS_KEY = "generated_keys";
    public static final String RESULT_INSERTED_KEY = "inserted";
    public static final String RESULT_REPLACED_KEY = "replaced";
    public static final String RESULT_SKIPPED_KEY = "skipped";
    public static final String RESULT_UNCHANGED_KEY = "unchanged";
    public static final String RESULT_FIRST_ERROR_KEY = "first_error";
    public static final String RESULT_WARNINGS_KEY = "warnings";

    public static final String DURABILITY_OPTION_KEY = "durability";

    public static final String RETHINKDB_ERROR_MESSAGE = "rethinkdb.error.message";
    public static final String DOCUMENT_ID_EMPTY_MESSAGE = "Document Id cannot be empty";

    protected Connection rethinkDbConnection;
    protected String databaseName;
    protected String tableName;
    protected String username;
    protected String password;
    protected String hostname;
    protected Integer port;
    protected long maxDocumentsSize;

    /**
     * Helper method to help testability
     * @return RethinkDB instance
     */
    protected RethinkDB getRethinkDB() {
        return RethinkDB.r;
    }

    /**
     * @return the rdbTable
     */
    protected Table getRdbTable() {
        return getRethinkDB().db(databaseName).table(tableName);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        hostname = context.getProperty(DB_HOST).getValue();
        port = context.getProperty(DB_PORT).asInteger();
        username = context.getProperty(USERNAME).getValue();
        password = context.getProperty(PASSWORD).getValue();
        databaseName = context.getProperty(DB_NAME).getValue();
        tableName = context.getProperty(TABLE_NAME).getValue();

        try {
            rethinkDbConnection = makeConnection();
        } catch(Exception e) {
            getLogger().error("Error while getting connection " + e.getLocalizedMessage(),e);
            throw new RuntimeException("Error while getting connection" + e.getLocalizedMessage(),e);
        }
        getLogger().info("RethinkDB connection created for host {} port {} and db {}",
                new Object[] {hostname, port,databaseName});
    }

    protected Connection makeConnection() {
        return getRethinkDB().connection().hostname(hostname)
            .port(port).user(username,
                    password).connect();
    }

    @OnStopped
    public void close() {
        getLogger().info("Closing connection");
        if ( rethinkDbConnection != null )
            rethinkDbConnection.close();
    }
}