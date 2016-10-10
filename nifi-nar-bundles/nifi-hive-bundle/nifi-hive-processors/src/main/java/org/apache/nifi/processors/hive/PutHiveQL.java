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
package org.apache.nifi.processors.hive;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.hive.HiveDBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SeeAlso(SelectHiveQL.class)
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"sql", "hive", "put", "database", "update", "insert"})
@CapabilityDescription("Executes a HiveQL DDL/DML command (UPDATE, INSERT, e.g.). The content of an incoming FlowFile is expected to be the HiveQL command "
        + "to execute. The HiveQL command may use the ? to escape parameters. In this case, the parameters to use must exist as FlowFile attributes "
        + "with the naming convention hiveql.args.N.type and hiveql.args.N.value, where N is a positive integer. The hiveql.args.N.type is expected to be "
        + "a number indicating the JDBC Type. The content of the FlowFile is expected to be in UTF-8 format.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "hiveql.args.N.type", description = "Incoming FlowFiles are expected to be parametrized HiveQL statements. The type of each Parameter is specified as an integer "
                + "that represents the JDBC Type of the parameter."),
        @ReadsAttribute(attribute = "hiveql.args.N.value", description = "Incoming FlowFiles are expected to be parametrized HiveQL statements. The value of the Parameters are specified as "
                + "hiveql.args.1.value, hiveql.args.2.value, hiveql.args.3.value, and so on. The type of the hiveql.args.1.value Parameter is specified by the hiveql.args.1.type attribute.")
})
public class PutHiveQL extends AbstractHiveQLProcessor {

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("hive-batch-size")
            .displayName("Batch Size")
            .description("The preferred number of FlowFiles to put to the database in a single transaction")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("hive-charset")
            .displayName("Character Set")
            .description("Specifies the character set of the record data.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the database is successfully updated")
            .build();
    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the database cannot be updated and retrying the operation will also fail, "
                    + "such as an invalid query or an integrity constraint violation")
            .build();

    private static final Pattern HIVEQL_TYPE_ATTRIBUTE_PATTERN = Pattern.compile("hiveql\\.args\\.(\\d+)\\.type");
    private static final Pattern NUMBER_PATTERN = Pattern.compile("-?\\d+");

    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    /*
     * Will ensure that the list of property descriptors is build only once.
     * Will also create a Set of relationships
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(HIVE_DBCP_SERVICE);
        _propertyDescriptors.add(BATCH_SIZE);
        _propertyDescriptors.add(CHARSET);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        _relationships.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final List<FlowFile> flowFiles = session.get(batchSize);

        if (flowFiles.isEmpty()) {
            return;
        }

        final long startNanos = System.nanoTime();
        final Charset charset = Charset.forName(context.getProperty(CHARSET).getValue());
        final HiveDBCPService dbcpService = context.getProperty(HIVE_DBCP_SERVICE).asControllerService(HiveDBCPService.class);
        try (final Connection conn = dbcpService.getConnection()) {

            for (FlowFile flowFile : flowFiles) {
                try {
                    final String hiveQL = getHiveQL(session, flowFile, charset);
                    final PreparedStatement stmt = conn.prepareStatement(hiveQL);
                    setParameters(stmt, flowFile.getAttributes());

                    // Execute the statement
                    stmt.execute();

                    // Emit a Provenance SEND event
                    final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                    session.getProvenanceReporter().send(flowFile, dbcpService.getConnectionURL(), transmissionMillis, true);
                    session.transfer(flowFile, REL_SUCCESS);

                } catch (final SQLException e) {

                    if (e instanceof SQLNonTransientException) {
                        getLogger().error("Failed to update Hive for {} due to {}; routing to failure", new Object[]{flowFile, e});
                        session.transfer(flowFile, REL_FAILURE);
                    } else {
                        getLogger().error("Failed to update Hive for {} due to {}; it is possible that retrying the operation will succeed, so routing to retry", new Object[]{flowFile, e});
                        flowFile = session.penalize(flowFile);
                        session.transfer(flowFile, REL_RETRY);
                    }

                }
            }
        } catch (final SQLException sqle) {
            // There was a problem getting the connection, yield and retry the flowfiles
            getLogger().error("Failed to get Hive connection due to {}; it is possible that retrying the operation will succeed, so routing to retry", new Object[]{sqle});
            session.transfer(flowFiles, REL_RETRY);
            context.yield();
        }
    }

    /**
     * Determines the HiveQL statement that should be executed for the given FlowFile
     *
     * @param session  the session that can be used to access the given FlowFile
     * @param flowFile the FlowFile whose HiveQL statement should be executed
     * @return the HiveQL that is associated with the given FlowFile
     */
    private String getHiveQL(final ProcessSession session, final FlowFile flowFile, final Charset charset) {
        // Read the HiveQL from the FlowFile's content
        final byte[] buffer = new byte[(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, buffer);
            }
        });

        // Create the PreparedStatement to use for this FlowFile.
        return new String(buffer, charset);
    }


    /**
     * Sets all of the appropriate parameters on the given PreparedStatement, based on the given FlowFile attributes.
     *
     * @param stmt       the statement to set the parameters on
     * @param attributes the attributes from which to derive parameter indices, values, and types
     * @throws SQLException if the PreparedStatement throws a SQLException when the appropriate setter is called
     */
    private void setParameters(final PreparedStatement stmt, final Map<String, String> attributes) throws SQLException {
        for (final Map.Entry<String, String> entry : attributes.entrySet()) {
            final String key = entry.getKey();
            final Matcher matcher = HIVEQL_TYPE_ATTRIBUTE_PATTERN.matcher(key);
            if (matcher.matches()) {
                final int parameterIndex = Integer.parseInt(matcher.group(1));

                final boolean isNumeric = NUMBER_PATTERN.matcher(entry.getValue()).matches();
                if (!isNumeric) {
                    throw new ProcessException("Value of the " + key + " attribute is '" + entry.getValue() + "', which is not a valid JDBC numeral type");
                }

                final int jdbcType = Integer.parseInt(entry.getValue());
                final String valueAttrName = "hiveql.args." + parameterIndex + ".value";
                final String parameterValue = attributes.get(valueAttrName);

                try {
                    setParameter(stmt, valueAttrName, parameterIndex, parameterValue, jdbcType);
                } catch (final NumberFormatException nfe) {
                    throw new ProcessException("The value of the " + valueAttrName + " is '" + parameterValue + "', which cannot be converted into the necessary data type", nfe);
                }
            }
        }
    }

    /**
     * Determines how to map the given value to the appropriate JDBC data type and sets the parameter on the
     * provided PreparedStatement
     *
     * @param stmt           the PreparedStatement to set the parameter on
     * @param attrName       the name of the attribute that the parameter is coming from - for logging purposes
     * @param parameterIndex the index of the HiveQL parameter to set
     * @param parameterValue the value of the HiveQL parameter to set
     * @param jdbcType       the JDBC Type of the HiveQL parameter to set
     * @throws SQLException if the PreparedStatement throws a SQLException when calling the appropriate setter
     */
    private void setParameter(final PreparedStatement stmt, final String attrName, final int parameterIndex, final String parameterValue, final int jdbcType) throws SQLException {
        if (parameterValue == null) {
            stmt.setNull(parameterIndex, jdbcType);
        } else {
            try {
                switch (jdbcType) {
                    case Types.BIT:
                    case Types.BOOLEAN:
                        stmt.setBoolean(parameterIndex, Boolean.parseBoolean(parameterValue));
                        break;
                    case Types.TINYINT:
                        stmt.setByte(parameterIndex, Byte.parseByte(parameterValue));
                        break;
                    case Types.SMALLINT:
                        stmt.setShort(parameterIndex, Short.parseShort(parameterValue));
                        break;
                    case Types.INTEGER:
                        stmt.setInt(parameterIndex, Integer.parseInt(parameterValue));
                        break;
                    case Types.BIGINT:
                        stmt.setLong(parameterIndex, Long.parseLong(parameterValue));
                        break;
                    case Types.REAL:
                        stmt.setFloat(parameterIndex, Float.parseFloat(parameterValue));
                        break;
                    case Types.FLOAT:
                    case Types.DOUBLE:
                        stmt.setDouble(parameterIndex, Double.parseDouble(parameterValue));
                        break;
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        stmt.setBigDecimal(parameterIndex, new BigDecimal(parameterValue));
                        break;
                    case Types.DATE:
                        stmt.setDate(parameterIndex, new Date(Long.parseLong(parameterValue)));
                        break;
                    case Types.TIME:
                        stmt.setTime(parameterIndex, new Time(Long.parseLong(parameterValue)));
                        break;
                    case Types.TIMESTAMP:
                        stmt.setTimestamp(parameterIndex, new Timestamp(Long.parseLong(parameterValue)));
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGNVARCHAR:
                    case Types.LONGVARCHAR:
                        stmt.setString(parameterIndex, parameterValue);
                        break;
                    default:
                        stmt.setObject(parameterIndex, parameterValue, jdbcType);
                        break;
                }
            } catch (SQLException e) {
                // Log which attribute/parameter had an error, then rethrow to be handled at the top level
                getLogger().error("Error setting parameter {} to value from {} ({})", new Object[]{parameterIndex, attrName, parameterValue}, e);
                throw e;
            }
        }
    }

}
