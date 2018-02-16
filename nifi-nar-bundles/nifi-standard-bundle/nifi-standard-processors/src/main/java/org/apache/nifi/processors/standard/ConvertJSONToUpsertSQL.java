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
package org.apache.nifi.processors.standard;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;

import static org.apache.nifi.processors.standard.util.JdbcCommon.*;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_COUNT;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_ID;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_INDEX;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.copyAttributesToOriginal;

@SideEffectFree
@SupportsBatching
@SeeAlso(PutSQL.class)
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"json", "sql", "database", "rdbms", "insert", "update", "delete", "relational", "flat","upsert"})
@CapabilityDescription("Splits a JSON-formatted FlowFile into an UPDATE and an INSERT SQL statement. The incoming FlowFile is expected to be "
        + "\"flat\" JSON message, meaning that it consists of a single JSON element and each field maps to a simple type. If a field maps to "
        + "a JSON object, that JSON object will be interpreted as Text. If the input is an array of JSON elements, each element in the array is "
        + "output as a separate FlowFile to the 'sql' relationship. Upon successful conversion, the original FlowFile is routed to the 'original' "
        + "relationship ,the Update SQL is routed to the 'update_sql' relationship and the Insert SQL is routed to the 'insert_sql' relationship")
@WritesAttributes({
        @WritesAttribute(attribute="mime.type", description="Sets mime.type of FlowFile that is routed to 'sql' to 'text/plain'."),
        @WritesAttribute(attribute = "<sql>.table", description = "Sets the <sql>.table attribute of FlowFile that is routed to 'sql' to the name of the table that is updated by the SQL statement. "
                + "The prefix for this attribute ('sql', e.g.) is determined by the SQL Parameter Attribute Prefix property."),
        @WritesAttribute(attribute="<sql>.catalog", description="If the Catalog name is set for this database, specifies the name of the catalog that the SQL statement will update. "
                + "If no catalog is used, this attribute will not be added. The prefix for this attribute ('sql', e.g.) is determined by the SQL Parameter Attribute Prefix property."),
        @WritesAttribute(attribute="fragment.identifier", description="All FlowFiles routed to the 'sql' relationship for the same incoming FlowFile (multiple will be output for the same incoming "
                + "FlowFile if the incoming FlowFile is a JSON Array) will have the same value for the fragment.identifier attribute. This can then be used to correlate the results."),
        @WritesAttribute(attribute="fragment.count", description="The number of SQL FlowFiles that were produced for same incoming FlowFile. This can be used in conjunction with the "
                + "fragment.identifier attribute in order to know how many FlowFiles belonged to the same incoming FlowFile."),
        @WritesAttribute(attribute="fragment.index", description="The position of this FlowFile in the list of outgoing FlowFiles that were all derived from the same incoming FlowFile. This can be "
                + "used in conjunction with the fragment.identifier and fragment.count attributes to know which FlowFiles originated from the same incoming FlowFile and in what order the SQL "
                + "FlowFiles were produced"),
        @WritesAttribute(attribute="<sql>.args.N.type", description="The output SQL statements are parametrized in order to avoid SQL Injection Attacks. The types of the Parameters "
                + "to use are stored in attributes named <sql>.args.1.type, <sql>.args.2.type, <sql>.args.3.type, and so on. The type is a number representing a JDBC Type constant. "
                + "Generally, this is useful only for software to read and interpret but is added so that a processor such as PutSQL can understand how to interpret the values. "
                + "The prefix for this attribute ('sql', e.g.) is determined by the SQL Parameter Attribute Prefix property."),
        @WritesAttribute(attribute="<sql>.args.N.value", description="The output SQL statements are parametrized in order to avoid SQL Injection Attacks. The values of the Parameters "
                + "to use are stored in the attributes named sql.args.1.value, sql.args.2.value, sql.args.3.value, and so on. Each of these attributes has a corresponding "
                + "<sql>.args.N.type attribute that indicates how the value should be interpreted when inserting it into the database."
                + "The prefix for this attribute ('sql', e.g.) is determined by the SQL Parameter Attribute Prefix property.")
})
public class ConvertJSONToUpsertSQL extends AbstractProcessor {


    static final AllowableValue IGNORE_UNMATCHED_FIELD = new AllowableValue("Ignore Unmatched Fields", "Ignore Unmatched Fields",
            "Any field in the JSON document that cannot be mapped to a column in the database is ignored");
    static final AllowableValue FAIL_UNMATCHED_FIELD = new AllowableValue("Fail", "Fail",
            "If the JSON document has any field that cannot be mapped to a column in the database, the FlowFile will be routed to the failure relationship");
    static final AllowableValue IGNORE_UNMATCHED_COLUMN = new AllowableValue("Ignore Unmatched Columns",
            "Ignore Unmatched Columns",
            "Any column in the database that does not have a field in the JSON document will be assumed to not be required.  No notification will be logged");
    static final AllowableValue WARNING_UNMATCHED_COLUMN = new AllowableValue("Warn on Unmatched Columns",
            "Warn on Unmatched Columns",
            "Any column in the database that does not have a field in the JSON document will be assumed to not be required.  A warning will be logged");
    static final AllowableValue FAIL_UNMATCHED_COLUMN = new AllowableValue("Fail on Unmatched Columns",
            "Fail on Unmatched Columns",
            "A flow will fail if any column in the database that does not have a field in the JSON document.  An error will be logged");

    static final PropertyDescriptor CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("JDBC Connection Pool")
            .description("Specifies the JDBC Connection Pool to use in order to convert the JSON message to a SQL statement. "
                    + "The Connection Pool is necessary in order to determine the appropriate database column types.")
            .identifiesControllerService(DBCPService.class)
            .required(true)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the table that the statement should update")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor CATALOG_NAME = new PropertyDescriptor.Builder()
            .name("Catalog Name")
            .description("The name of the catalog that the statement should update. This may not apply for the database that you are updating. In this case, leave the field empty")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
            .name("Schema Name")
            .description("The name of the schema that the table belongs to. This may not apply for the database that you are updating. In this case, leave the field empty")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor TRANSLATE_FIELD_NAMES = new PropertyDescriptor.Builder()
            .name("Translate Field Names")
            .description("If true, the Processor will attempt to translate JSON field names into the appropriate column names for the table specified. "
                    + "If false, the JSON field names must match the column names exactly, or the column will not be updated")
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    static final PropertyDescriptor UNMATCHED_FIELD_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("Unmatched Field Behavior")
            .description("If an incoming JSON element has a field that does not map to any of the database table's columns, this property specifies how to handle the situation")
            .allowableValues(IGNORE_UNMATCHED_FIELD, FAIL_UNMATCHED_FIELD)
            .defaultValue(IGNORE_UNMATCHED_FIELD.getValue())
            .build();
    static final PropertyDescriptor UNMATCHED_COLUMN_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("Unmatched Column Behavior")
            .description("If an incoming JSON element does not have a field mapping for all of the database table's columns, this property specifies how to handle the situation")
            .allowableValues(IGNORE_UNMATCHED_COLUMN, WARNING_UNMATCHED_COLUMN ,FAIL_UNMATCHED_COLUMN)
            .defaultValue(FAIL_UNMATCHED_COLUMN.getValue())
            .build();
    static final PropertyDescriptor UPDATE_KEY = new PropertyDescriptor.Builder()
            .name("Update Keys")
            .description("A comma-separated list of column names that uniquely identifies a row in the database for UPDATE statements. "
                    + "If the Statement Type is UPDATE and this property is not set, the table's Primary Keys are used. "
                    + "In this case, if no Primary Key exists, the conversion to SQL will fail if Unmatched Column Behaviour is set to FAIL. "
                    + "This property is ignored if the Statement Type is INSERT")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .expressionLanguageSupported(true)
            .build();

    static final PropertyDescriptor QUOTED_IDENTIFIERS = new PropertyDescriptor.Builder()
            .name("jts-quoted-identifiers")
            .displayName("Quote Column Identifiers")
            .description("Enabling this option will cause all column names to be quoted, allowing you to "
                    + "use reserved words as column names in your tables.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor QUOTED_TABLE_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("jts-quoted-table-identifiers")
            .displayName("Quote Table Identifiers")
            .description("Enabling this option will cause the table name to be quoted to support the "
                    + "use of special characters in the table name")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor SQL_PARAM_ATTR_PREFIX = new PropertyDescriptor.Builder()
            .name("jts-sql-param-attr-prefix")
            .displayName("SQL Parameter Attribute Prefix")
            .description("The string to be prepended to the outgoing flow file attributes, such as <sql>.args.1.value, where <sql> is replaced with the specified value")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .required(true)
            .defaultValue("sql")
            .build();

    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("When a FlowFile is converted to SQL, the original JSON FlowFile is routed to this relationship")
            .build();



    /**
     * A FlowFile is routed to this relationship when its contents have successfully been converted into a Update SQL statement.
     */
    static final Relationship REL_UPDATE_SQL = new Relationship.Builder()
            .name("update_sql")
            .description("A FlowFile is routed to this relationship when its contents have successfully been converted into a Update SQL statement")
            .build();


    /**
     * A FlowFile is routed to this relationship when its contents have successfully been converted into a Insert SQL statement.
     */
    static final Relationship REL_INSERT_SQL = new Relationship.Builder()
            .name("insert_sql")
            .description("A FlowFile is routed to this relationship when its contents have successfully been converted into a Insert SQL statement")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be converted into a SQL statement. Common causes include invalid JSON "
                    + "content or the JSON content missing a required field (if using an INSERT statement type).")
            .build();

    private final Map<SchemaKey, TableSchema> schemaCache = new LinkedHashMap<SchemaKey, TableSchema>(100) {
        private static final long serialVersionUID = 1L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<SchemaKey,TableSchema> eldest) {
            return size() >= 100;
        }
    };

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CONNECTION_POOL);
        properties.add(TABLE_NAME);
        properties.add(CATALOG_NAME);
        properties.add(SCHEMA_NAME);
        properties.add(TRANSLATE_FIELD_NAMES);
        properties.add(UNMATCHED_FIELD_BEHAVIOR);
        properties.add(UNMATCHED_COLUMN_BEHAVIOR);
        properties.add(UPDATE_KEY);
        properties.add(QUOTED_IDENTIFIERS);
        properties.add(QUOTED_TABLE_IDENTIFIER);
        properties.add(SQL_PARAM_ATTR_PREFIX);
        return properties;
    }


    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_ORIGINAL);
        rels.add(REL_UPDATE_SQL);
        rels.add(REL_INSERT_SQL);
        rels.add(REL_FAILURE);

        return rels;
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        synchronized (this) {
            schemaCache.clear();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final boolean translateFieldNames = context.getProperty(TRANSLATE_FIELD_NAMES).asBoolean();
        final boolean ignoreUnmappedFields = IGNORE_UNMATCHED_FIELD.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_FIELD_BEHAVIOR).getValue());
        final String updateKeys = context.getProperty(UPDATE_KEY).evaluateAttributeExpressions(flowFile).getValue();
        getLogger().debug("updateKeys {}", new Object[] {updateKeys});


        final String catalog = context.getProperty(CATALOG_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final SchemaKey schemaKey = new SchemaKey(catalog, tableName);
        final boolean includePrimaryKeys = true;

        // Is the unmatched column behaviour fail or warning?
        final boolean failUnmappedColumns = FAIL_UNMATCHED_COLUMN.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_COLUMN_BEHAVIOR).getValue());
        final boolean warningUnmappedColumns = WARNING_UNMATCHED_COLUMN.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_COLUMN_BEHAVIOR).getValue());

        //Escape column names?
        final boolean escapeColumnNames = context.getProperty(QUOTED_IDENTIFIERS).asBoolean();

        // Quote table name?
        final boolean quoteTableName = context.getProperty(QUOTED_TABLE_IDENTIFIER).asBoolean();

        // Attribute prefix
        final String attributePrefix = context.getProperty(SQL_PARAM_ATTR_PREFIX).evaluateAttributeExpressions(flowFile).getValue();

        // get the database schema from the cache, if one exists. We do this in a synchronized block, rather than
        // using a ConcurrentMap because the Map that we are using is a LinkedHashMap with a capacity such that if
        // the Map grows beyond this capacity, old elements are evicted. We do this in order to avoid filling the
        // Java Heap if there are a lot of different SQL statements being generated that reference different tables.
        TableSchema schema;
        synchronized (this) {
            schema = schemaCache.get(schemaKey);
            if (schema == null) {
                // No schema exists for this table yet. Query the database to determine the schema and put it into the cache.
                final DBCPService dbcpService = context.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class);
                try (final Connection conn = dbcpService.getConnection()) {
                    schema = TableSchema.from(conn, catalog, schemaName, tableName, translateFieldNames, includePrimaryKeys);
                    schemaCache.put(schemaKey, schema);
                } catch (final SQLException e) {
                    getLogger().error("Failed to convert {} into a SQL statement due to {}; routing to failure", new Object[] {flowFile, e.toString()}, e);
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }
            }
        }

        // Parse the JSON document
        final ObjectMapper mapper = new ObjectMapper();
        final AtomicReference<JsonNode> rootNodeRef = new AtomicReference<>(null);
        try {
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    try (final InputStream bufferedIn = new BufferedInputStream(in)) {
                        rootNodeRef.set(mapper.readTree(bufferedIn));
                    }
                }
            });
        } catch (final ProcessException pe) {
            getLogger().error("Failed to parse {} as JSON due to {}; routing to failure", new Object[] {flowFile, pe.toString()}, pe);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final JsonNode rootNode = rootNodeRef.get();

        // The node may or may not be a Json Array. If it isn't, we will create an
        // ArrayNode and add just the root node to it. We do this so that we can easily iterate
        // over the array node, rather than duplicating the logic or creating another function that takes many variables
        // in order to implement the logic.
        final ArrayNode arrayNode;
        if (rootNode.isArray()) {
            arrayNode = (ArrayNode) rootNode;
        } else {
            final JsonNodeFactory nodeFactory = JsonNodeFactory.instance;
            arrayNode = new ArrayNode(nodeFactory);
            arrayNode.add(rootNode);
        }

        final String fragmentIdentifier = UUID.randomUUID().toString();

        getLogger().debug("ArrayNode size: {} ",new Object[]{arrayNode.size()});



        PreparedStatement stmt = null;
        final Set<FlowFile> created = new HashSet<>();

        // Get or create the appropriate PreparedStatement to use.
        final DBCPService dbcpService = context.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class);
        try (final Connection conn = dbcpService.getConnection()) {

            ArrayList <Integer>updateList = new ArrayList<Integer>();
            ArrayList <Integer>insertList = new ArrayList<Integer>();

            // build the fully qualified table name
            final StringBuilder tableNameBuilder = new StringBuilder();
            if (catalog != null) {
                tableNameBuilder.append(catalog).append(".");
            }
            if (schemaName != null) {
                tableNameBuilder.append(schemaName).append(".");
            }
            tableNameBuilder.append(tableName);
            final String fqTableName = tableNameBuilder.toString();



            final String sqlSelectCount = generateSelectCount(fqTableName,schema,quoteTableName);
            // table empty ? just insert
            PreparedStatement statTmp =  conn.prepareStatement(sqlSelectCount, Statement.RETURN_GENERATED_KEYS);
            ResultSet resultTmp = statTmp.executeQuery();

            getLogger().debug("If target table is empty sql {}",new Object[]{sqlSelectCount});

            boolean emptyTargetTable = false;
            if(resultTmp.next()){
                if(resultTmp.getInt("cnt")==0){
                    emptyTargetTable = true;
                }
            }

            getLogger().debug("If target table is empty result {}",new Object[]{emptyTargetTable});

            if(!emptyTargetTable){
                for (int i=0; i < arrayNode.size(); i++) {
                    final JsonNode jsonNode = arrayNode.get(i);


                    final String sqlSelect;


                    final Map<String, String> attributesSelect = new HashMap<>();

                    try {


                        sqlSelect =  generateSelect(jsonNode,attributesSelect , fqTableName,updateKeys, schema, translateFieldNames, ignoreUnmappedFields,
                                failUnmappedColumns, warningUnmappedColumns, escapeColumnNames, quoteTableName, attributePrefix);

                        getLogger().debug("SQL select: {} ",new Object[]{sqlSelect});

                    } catch (final ProcessException pe) {
                        getLogger().error("Failed to convert {} to a SQL  statement due to {}; routing to failure",
                                new Object[] { flowFile, pe.toString() }, pe);
                        session.remove(created);
                        session.transfer(flowFile, REL_FAILURE);
                        return;
                    }


                    // select to determine which way to go
                    if(stmt == null) {

                        stmt = conn.prepareStatement(sqlSelect, Statement.RETURN_GENERATED_KEYS);
                    }
                    setParameters(stmt, attributesSelect);
                    stmt.addBatch();
                    ResultSet result = stmt.executeQuery();
                    int way = -1;
                    if(result.next()){
                        way = result.getInt("cnt");
                    }

                    if(way>0)
                        updateList.add(i);
                    else
                        insertList.add(i);
                }
            }else{
                for (int i=0; i < arrayNode.size(); i++) {
                    insertList.add(i);
                }
            }



            for (int i=0; i < arrayNode.size(); i++) {
                final JsonNode jsonNode = arrayNode.get(i);

                final String sqlInsert;
                final String sqlUpdate;

                final Map<String, String> attributesUpdate = new HashMap<>();
                final Map<String, String> attributesInsert = new HashMap<>();

                try {


                    sqlInsert = generateInsert(jsonNode, attributesInsert, fqTableName, schema, translateFieldNames, ignoreUnmappedFields,
                            failUnmappedColumns, warningUnmappedColumns, escapeColumnNames, quoteTableName, attributePrefix);
                    sqlUpdate = generateUpdate(jsonNode, attributesUpdate, fqTableName, updateKeys, schema, translateFieldNames, ignoreUnmappedFields,
                            failUnmappedColumns, warningUnmappedColumns, escapeColumnNames, quoteTableName, attributePrefix);

                    getLogger().debug("Upsert SQLs: {},{} ",new Object[]{sqlInsert,sqlUpdate});

                } catch (final ProcessException pe) {
                    getLogger().error("Failed to convert {} to a SQL  statement due to {}; routing to failure",
                            new Object[] { flowFile, pe.toString() }, pe);
                    session.remove(created);
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }



                if(updateList.contains(i)){
                    // update
                    FlowFile sqlUpdateFlowFile = session.create(flowFile);
                    created.add(sqlUpdateFlowFile);

                    sqlUpdateFlowFile = session.write(sqlUpdateFlowFile, new OutputStreamCallback() {
                        @Override
                        public void process(final OutputStream out) throws IOException {
                            out.write(sqlUpdate.getBytes(StandardCharsets.UTF_8));
                        }
                    });

                    attributesUpdate.put(CoreAttributes.MIME_TYPE.key(), "text/plain");
                    attributesUpdate.put(attributePrefix + ".table", tableName);
                    attributesUpdate.put(FRAGMENT_ID.key(), fragmentIdentifier);
                    attributesUpdate.put(FRAGMENT_COUNT.key(), String.valueOf(updateList.size()));
                    attributesUpdate.put(FRAGMENT_INDEX.key(), String.valueOf(updateList.indexOf(i)));

                    if (catalog != null) {
                        attributesUpdate.put(attributePrefix + ".catalog", catalog);
                    }

                    sqlUpdateFlowFile = session.putAllAttributes(sqlUpdateFlowFile, attributesUpdate);
                    session.transfer(sqlUpdateFlowFile, REL_UPDATE_SQL);
                }else {

                    // insert
                    FlowFile sqlInsertFlowFile = session.create(flowFile);
                    created.add(sqlInsertFlowFile);

                    sqlInsertFlowFile = session.write(sqlInsertFlowFile, new OutputStreamCallback() {
                        @Override
                        public void process(final OutputStream out) throws IOException {
                            out.write(sqlInsert.getBytes(StandardCharsets.UTF_8));
                        }
                    });

                    attributesInsert.put(CoreAttributes.MIME_TYPE.key(), "text/plain");
                    attributesInsert.put(attributePrefix + ".table", tableName);
                    attributesInsert.put(FRAGMENT_ID.key(), fragmentIdentifier);
                    attributesInsert.put(FRAGMENT_COUNT.key(), String.valueOf(insertList.size()));
                    attributesInsert.put(FRAGMENT_INDEX.key(), String.valueOf(insertList.indexOf(i)));

                    if (catalog != null) {
                        attributesInsert.put(attributePrefix + ".catalog", catalog);
                    }

                    sqlInsertFlowFile = session.putAllAttributes(sqlInsertFlowFile, attributesInsert);
                    session.transfer(sqlInsertFlowFile, REL_INSERT_SQL);
                }
            }

        } catch (final SQLException e) {
            getLogger().error("Failed to convert {} into a SQL statement due to {}; routing to failure", new Object[] {flowFile, e.toString()}, e);
            session.remove(created);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        flowFile = copyAttributesToOriginal(session, flowFile, fragmentIdentifier, arrayNode.size());
        session.transfer(flowFile, REL_ORIGINAL);
    }

}
