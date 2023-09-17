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

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.hive.Hive3DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An abstract base class for HiveQL processors to share common data, methods, etc.
 */
public abstract class AbstractHive3QLProcessor extends AbstractSessionFactoryProcessor {

    protected static final Pattern HIVEQL_TYPE_ATTRIBUTE_PATTERN = Pattern.compile("hiveql\\.args\\.(\\d+)\\.type");
    protected static final Pattern NUMBER_PATTERN = Pattern.compile("-?\\d+");
    static String ATTR_INPUT_TABLES = "query.input.tables";
    static String ATTR_OUTPUT_TABLES = "query.output.tables";


    public static final PropertyDescriptor HIVE_DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("hive3-dbcp-service")
            .displayName("Hive Database Connection Pooling Service")
            .description("The Hive Controller Service that is used to obtain connection(s) to the Hive database")
            .required(true)
            .identifiesControllerService(Hive3DBCPService.class)
            .build();

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("hive3-charset")
            .displayName("Character Set")
            .description("Specifies the character set of the record data.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    public static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("hive3-query-timeout")
            .displayName("Query timeout")
            .description("Sets the number of seconds the driver will wait for a query to execute. "
                    + "A value of 0 means no timeout. NOTE: Non-zero values may not be supported by the driver.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    /**
     * Determines the HiveQL statement that should be executed for the given FlowFile
     *
     * @param session  the session that can be used to access the given FlowFile
     * @param flowFile the FlowFile whose HiveQL statement should be executed
     * @return the HiveQL that is associated with the given FlowFile
     */
    protected String getHiveQL(final ProcessSession session, final FlowFile flowFile, final Charset charset) {
        // Read the HiveQL from the FlowFile's content
        final byte[] buffer = new byte[(int) flowFile.getSize()];
        session.read(flowFile, in -> StreamUtils.fillBuffer(in, buffer));

        // Create the PreparedStatement to use for this FlowFile.
        return new String(buffer, charset);
    }

    private class ParameterHolder {
        String attributeName;
        int jdbcType;
        String value;
    }

    /**
     * Sets all of the appropriate parameters on the given PreparedStatement, based on the given FlowFile attributes.
     *
     * @param stmt       the statement to set the parameters on
     * @param attributes the attributes from which to derive parameter indices, values, and types
     * @throws SQLException if the PreparedStatement throws a SQLException when the appropriate setter is called
     */
    protected int setParameters(int base, final PreparedStatement stmt, int paramCount, final Map<String, String> attributes) throws SQLException {

        Map<Integer, ParameterHolder> parmMap = new TreeMap<Integer, ParameterHolder>();

        for (final Map.Entry<String, String> entry : attributes.entrySet()) {
                final String key = entry.getKey();
                final Matcher matcher = HIVEQL_TYPE_ATTRIBUTE_PATTERN.matcher(key);
                if (matcher.matches()) {
                    final int parameterIndex = Integer.parseInt(matcher.group(1));
                    if (parameterIndex >= base && parameterIndex < base + paramCount) {
                        final boolean isNumeric = NUMBER_PATTERN.matcher(entry.getValue()).matches();
                        if (!isNumeric) {
                            throw new SQLDataException("Value of the " + key + " attribute is '" + entry.getValue() + "', which is not a valid JDBC numeral jdbcType");
                        }

                        final String valueAttrName = "hiveql.args." + parameterIndex + ".value";

                        ParameterHolder ph = new ParameterHolder();
                        int realIndexLoc = parameterIndex - base +1;

                        ph.jdbcType = Integer.parseInt(entry.getValue());
                        ph.value = attributes.get(valueAttrName);
                        ph.attributeName = valueAttrName;

                        parmMap.put(realIndexLoc, ph);

                    }
                }
        }


        // Now that's we've retrieved the correct number of parameters and it's sorted, let's set them.
        for (final Map.Entry<Integer, ParameterHolder> entry : parmMap.entrySet()) {
            final Integer index = entry.getKey();
            final ParameterHolder ph = entry.getValue();

            try {
                setParameter(stmt, ph.attributeName, index, ph.value, ph.jdbcType);
            } catch (final NumberFormatException nfe) {
                throw new SQLDataException("The value of the " + ph.attributeName + " is '" + ph.value + "', which cannot be converted into the necessary data jdbcType", nfe);
            }
        }
        return base + paramCount;
    }

    /**
     * Determines how to map the given value to the appropriate JDBC data jdbcType and sets the parameter on the
     * provided PreparedStatement
     *
     * @param stmt           the PreparedStatement to set the parameter on
     * @param attrName       the name of the attribute that the parameter is coming from - for logging purposes
     * @param parameterIndex the index of the HiveQL parameter to set
     * @param parameterValue the value of the HiveQL parameter to set
     * @param jdbcType       the JDBC Type of the HiveQL parameter to set
     * @throws SQLException if the PreparedStatement throws a SQLException when calling the appropriate setter
     */
    protected void setParameter(final PreparedStatement stmt, final String attrName, final int parameterIndex, final String parameterValue, final int jdbcType) throws SQLException {
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
                getLogger().error("Error setting parameter {} to value from {} ({})", parameterIndex, attrName, parameterValue, e);
                throw e;
            }
        }
    }

    protected static class TableName {
        private final String database;
        private final String table;
        private final boolean input;

        TableName(String database, String table, boolean input) {
            this.database = database;
            this.table = table;
            this.input = input;
        }

        public String getDatabase() {
            return database;
        }

        public String getTable() {
            return table;
        }

        public boolean isInput() {
            return input;
        }

        @Override
        public String toString() {
            return database == null || database.isEmpty() ? table : database + '.' + table;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TableName tableName = (TableName) o;

            if (input != tableName.input) return false;
            if (database != null ? !database.equals(tableName.database) : tableName.database != null) return false;
            return table.equals(tableName.table);
        }

        @Override
        public int hashCode() {
            int result = database != null ? database.hashCode() : 0;
            result = 31 * result + table.hashCode();
            result = 31 * result + (input ? 1 : 0);
            return result;
        }
    }

    protected Set<TableName> findTableNames(final String query) {
        final ASTNode node;
        try {
            node = new ParseDriver().parse(normalize(query));
        } catch (ParseException e) {
            // If failed to parse the query, just log a message, but continue.
            getLogger().debug("Failed to parse query: {} due to {}", query, e, e);
            return Collections.emptySet();
        }
        final HashSet<TableName> tableNames = new HashSet<>();
        findTableNames(node, tableNames);
        return tableNames;
    }

    /**
     * Normalize query.
     * Hive resolves prepared statement parameters before executing a query,
     * see {@link org.apache.hive.jdbc.HivePreparedStatement#updateSql(String, HashMap)} for detail.
     * HiveParser does not expect '?' to be in a query string, and throws an Exception if there is one.
     * In this normalize method, '?' is replaced to 'x' to avoid that.
     */
    private String normalize(String query) {
        return query.replace('?', 'x');
    }

    private void findTableNames(final Object obj, final Set<TableName> tableNames) {
        if (!(obj instanceof CommonTree)) {
            return;
        }
        final CommonTree tree = (CommonTree) obj;
        final int childCount = tree.getChildCount();
        if ("TOK_TABNAME".equals(tree.getText())) {
            final TableName tableName;
            final boolean isInput = "TOK_TABREF".equals(tree.getParent().getText());
            switch (childCount) {
                case 1 :
                    tableName = new TableName(null, tree.getChild(0).getText(), isInput);
                    break;
                case 2:
                    tableName = new TableName(tree.getChild(0).getText(), tree.getChild(1).getText(), isInput);
                    break;
                default:
                    throw new IllegalStateException("TOK_TABNAME does not have expected children, childCount=" + childCount);
            }
            // If parent is TOK_TABREF, then it is an input table.
            tableNames.add(tableName);
            return;
        }
        for (int i = 0; i < childCount; i++) {
            findTableNames(tree.getChild(i), tableNames);
        }
    }

    protected Map<String, String> toQueryTableAttributes(Set<TableName> tableNames) {
        final Map<String, String> attributes = new HashMap<>();
        for (TableName tableName : tableNames) {
            final String attributeName = tableName.isInput() ? ATTR_INPUT_TABLES : ATTR_OUTPUT_TABLES;
            if (attributes.containsKey(attributeName)) {
                attributes.put(attributeName, attributes.get(attributeName) + "," + tableName);
            } else {
                attributes.put(attributeName, tableName.toString());
            }
        }
        return attributes;
    }
}
