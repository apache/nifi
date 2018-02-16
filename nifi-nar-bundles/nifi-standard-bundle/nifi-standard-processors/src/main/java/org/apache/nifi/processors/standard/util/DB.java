package org.apache.nifi.processors.standard.util;

import org.apache.nifi.processor.exception.ProcessException;
import org.codehaus.jackson.JsonNode;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.regex.Matcher;
import org.apache.nifi.processors.standard.util.JdbcCommon;


public class DB {



    /**
     * Sets all of the appropriate parameters on the given PreparedStatement, based on the given FlowFile attributes.
     *
     * @param stmt the statement to set the parameters on
     * @param attributes the attributes from which to derive parameter indices, values, and types
     * @throws SQLException if the PreparedStatement throws a SQLException when the appropriate setter is called
     */
    public static void setParameters(final PreparedStatement stmt, final Map<String, String> attributes) throws SQLException {
        for (final Map.Entry<String, String> entry : attributes.entrySet()) {
            final String key = entry.getKey();
            final Matcher matcher = JdbcCommon.SQL_TYPE_ATTRIBUTE_PATTERN.matcher(key);
            if (matcher.matches()) {
                final int parameterIndex = Integer.parseInt(matcher.group(1));

                final boolean isNumeric = JdbcCommon.NUMBER_PATTERN.matcher(entry.getValue()).matches();
                if (!isNumeric) {
                    throw new SQLDataException("Value of the " + key + " attribute is '" + entry.getValue() + "', which is not a valid JDBC numeral type");
                }

                final int jdbcType = Integer.parseInt(entry.getValue());
                final String valueAttrName = "sql.args." + parameterIndex + ".value";
                final String parameterValue = attributes.get(valueAttrName);
                final String formatAttrName = "sql.args." + parameterIndex + ".format";
                final String parameterFormat = attributes.containsKey(formatAttrName)? attributes.get(formatAttrName):"";

                try {
                    setParameter(stmt, valueAttrName, parameterIndex, parameterValue, jdbcType, parameterFormat);
                } catch (final NumberFormatException nfe) {
                    throw new SQLDataException("The value of the " + valueAttrName + " is '" + parameterValue + "', which cannot be converted into the necessary data type", nfe);
                } catch (ParseException pe) {
                    throw new SQLDataException("The value of the " + valueAttrName + " is '" + parameterValue + "', which cannot be converted to a timestamp", pe);
                } catch (UnsupportedEncodingException uee) {
                    throw new SQLDataException("The value of the " + valueAttrName + " is '" + parameterValue + "', which cannot be converted to UTF-8", uee);
                }
            }
        }
    }






    /**
     * Determines how to map the given value to the appropriate JDBC data type and sets the parameter on the
     * provided PreparedStatement
     *
     * @param stmt the PreparedStatement to set the parameter on
     * @param attrName the name of the attribute that the parameter is coming from - for logging purposes
     * @param parameterIndex the index of the SQL parameter to set
     * @param parameterValue the value of the SQL parameter to set
     * @param jdbcType the JDBC Type of the SQL parameter to set
     * @throws SQLException if the PreparedStatement throws a SQLException when calling the appropriate setter
     */
    public static void setParameter(final PreparedStatement stmt, final String attrName, final int parameterIndex, final String parameterValue, final int jdbcType,
                              final String valueFormat)
            throws SQLException, ParseException, UnsupportedEncodingException {
        if (parameterValue == null) {
            stmt.setNull(parameterIndex, jdbcType);
        } else {
            switch (jdbcType) {
                case Types.BIT:
                    stmt.setBoolean(parameterIndex, "1".equals(parameterValue) || "t".equalsIgnoreCase(parameterValue) || Boolean.parseBoolean(parameterValue));
                    break;
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
                    Date date;

                    if (valueFormat.equals("")) {
                        if(JdbcCommon.LONG_PATTERN.matcher(parameterValue).matches()){
                            date = new Date(Long.parseLong(parameterValue));
                        }else {
                            String dateFormatString = "yyyy-MM-dd";
                            SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatString);
                            java.util.Date parsedDate = dateFormat.parse(parameterValue);
                            date = new Date(parsedDate.getTime());
                        }
                    } else {
                        final DateTimeFormatter dtFormatter = getDateTimeFormatter(valueFormat);
                        LocalDate parsedDate = LocalDate.parse(parameterValue, dtFormatter);
                        date = new Date(Date.from(parsedDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant()).getTime());
                    }

                    stmt.setDate(parameterIndex, date);
                    break;
                case Types.TIME:
                    Time time;

                    if (valueFormat.equals("")) {
                        if (JdbcCommon.LONG_PATTERN.matcher(parameterValue).matches()) {
                            time = new Time(Long.parseLong(parameterValue));
                        } else {
                            String timeFormatString = "HH:mm:ss.SSS";
                            SimpleDateFormat dateFormat = new SimpleDateFormat(timeFormatString);
                            java.util.Date parsedDate = dateFormat.parse(parameterValue);
                            time = new Time(parsedDate.getTime());
                        }
                    } else {
                        final DateTimeFormatter dtFormatter = getDateTimeFormatter(valueFormat);
                        LocalTime parsedTime = LocalTime.parse(parameterValue, dtFormatter);
                        LocalDateTime localDateTime = parsedTime.atDate(LocalDate.ofEpochDay(0));
                        Instant instant = localDateTime.atZone(ZoneId.systemDefault()).toInstant();
                        time = new Time(instant.toEpochMilli());
                    }

                    stmt.setTime(parameterIndex, time);
                    break;
                case Types.TIMESTAMP:
                    long lTimestamp=0L;

                    // Backwards compatibility note: Format was unsupported for a timestamp field.
                    if (valueFormat.equals("")) {
                        if(JdbcCommon.LONG_PATTERN.matcher(parameterValue).matches()){
                            lTimestamp = Long.parseLong(parameterValue);
                        } else {
                            final SimpleDateFormat dateFormat  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                            java.util.Date parsedDate = dateFormat.parse(parameterValue);
                            lTimestamp = parsedDate.getTime();
                        }
                    } else {
                        final DateTimeFormatter dtFormatter = getDateTimeFormatter(valueFormat);
                        TemporalAccessor accessor = dtFormatter.parse(parameterValue);
                        java.util.Date parsedDate = java.util.Date.from(Instant.from(accessor));
                        lTimestamp = parsedDate.getTime();
                    }

                    stmt.setTimestamp(parameterIndex, new Timestamp(lTimestamp));

                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    byte[] bValue;

                    switch(valueFormat){
                        case "":
                        case "ascii":
                            bValue = parameterValue.getBytes("ASCII");
                            break;
                        case "hex":
                            bValue = DatatypeConverter.parseHexBinary(parameterValue);
                            break;
                        case "base64":
                            bValue = DatatypeConverter.parseBase64Binary(parameterValue);
                            break;
                        default:
                            throw new ParseException("Unable to parse binary data using the formatter `" + valueFormat + "`.",0);
                    }

                    stmt.setBinaryStream(parameterIndex, new ByteArrayInputStream(bValue), bValue.length);

                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGNVARCHAR:
                case Types.LONGVARCHAR:
                    stmt.setString(parameterIndex, parameterValue);
                    break;
                case Types.CLOB:
                    try (final StringReader reader = new StringReader(parameterValue)) {
                        stmt.setCharacterStream(parameterIndex, reader);
                    }
                    break;
                case Types.NCLOB:
                    try (final StringReader reader = new StringReader(parameterValue)) {
                        stmt.setNCharacterStream(parameterIndex, reader);
                    }
                    break;
                default:
                    stmt.setObject(parameterIndex, parameterValue, jdbcType);
                    break;
            }
        }
    }


    private  static DateTimeFormatter getDateTimeFormatter(String pattern) {
        switch(pattern) {
            case "BASIC_ISO_DATE": return DateTimeFormatter.BASIC_ISO_DATE;
            case "ISO_LOCAL_DATE": return DateTimeFormatter.ISO_LOCAL_DATE;
            case "ISO_OFFSET_DATE": return DateTimeFormatter.ISO_OFFSET_DATE;
            case "ISO_DATE": return DateTimeFormatter.ISO_DATE;
            case "ISO_LOCAL_TIME": return DateTimeFormatter.ISO_LOCAL_TIME;
            case "ISO_OFFSET_TIME": return DateTimeFormatter.ISO_OFFSET_TIME;
            case "ISO_TIME": return DateTimeFormatter.ISO_TIME;
            case "ISO_LOCAL_DATE_TIME": return DateTimeFormatter.ISO_LOCAL_DATE_TIME;
            case "ISO_OFFSET_DATE_TIME": return DateTimeFormatter.ISO_OFFSET_DATE_TIME;
            case "ISO_ZONED_DATE_TIME": return DateTimeFormatter.ISO_ZONED_DATE_TIME;
            case "ISO_DATE_TIME": return DateTimeFormatter.ISO_DATE_TIME;
            case "ISO_ORDINAL_DATE": return DateTimeFormatter.ISO_ORDINAL_DATE;
            case "ISO_WEEK_DATE": return DateTimeFormatter.ISO_WEEK_DATE;
            case "ISO_INSTANT": return DateTimeFormatter.ISO_INSTANT;
            case "RFC_1123_DATE_TIME": return DateTimeFormatter.RFC_1123_DATE_TIME;
            default: return DateTimeFormatter.ofPattern(pattern);
        }
    }



    public static class ColumnDescription {
        private final String columnName;
        private final int dataType;
        private final boolean required;
        private final Integer columnSize;

        private ColumnDescription(final String columnName, final int dataType, final boolean required, final Integer columnSize) {
            this.columnName = columnName;
            this.dataType = dataType;
            this.required = required;
            this.columnSize = columnSize;
        }

        public int getDataType() {
            return dataType;
        }

        public Integer getColumnSize() {
            return columnSize;
        }

        public String getColumnName() {
            return columnName;
        }

        public boolean isRequired() {
            return required;
        }

        public static ColumnDescription from(final ResultSet resultSet) throws SQLException {
            final ResultSetMetaData md = resultSet.getMetaData();
            List<String> columns = new ArrayList<>();

            for (int i = 1; i < md.getColumnCount() + 1; i++) {
                columns.add(md.getColumnName(i));
            }

            final String columnName = resultSet.getString("COLUMN_NAME");
            final int dataType = resultSet.getInt("DATA_TYPE");
            final int colSize = resultSet.getInt("COLUMN_SIZE");

            final String nullableValue = resultSet.getString("IS_NULLABLE");
            final boolean isNullable = "YES".equalsIgnoreCase(nullableValue) || nullableValue.isEmpty();
            final String defaultValue = resultSet.getString("COLUMN_DEF");
            String autoIncrementValue = "NO";

            if(columns.contains("IS_AUTOINCREMENT")){
                autoIncrementValue = resultSet.getString("IS_AUTOINCREMENT");
            }

            final boolean isAutoIncrement = "YES".equalsIgnoreCase(autoIncrementValue);
            final boolean required = !isNullable && !isAutoIncrement && defaultValue == null;

            return new ColumnDescription(columnName, dataType, required, colSize == 0 ? null : colSize);
        }
    }

    public static class SchemaKey {
        private final String catalog;
        private final String tableName;

        public SchemaKey(final String catalog, final String tableName) {
            this.catalog = catalog;
            this.tableName = tableName;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((catalog == null) ? 0 : catalog.hashCode());
            result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
            return result;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }

            final SchemaKey other = (SchemaKey) obj;
            if (catalog == null) {
                if (other.catalog != null) {
                    return false;
                }
            } else if (!catalog.equals(other.catalog)) {
                return false;
            }


            if (tableName == null) {
                if (other.tableName != null) {
                    return false;
                }
            } else if (!tableName.equals(other.tableName)) {
                return false;
            }

            return true;
        }
    }
    
    public static class TableSchema {
        private List<String> requiredColumnNames;
        private Set<String> primaryKeyColumnNames;
        private Map<String, ColumnDescription> columns;
        private String quotedIdentifierString;

        private TableSchema(final List<ColumnDescription> columnDescriptions, final boolean translateColumnNames,
                            final Set<String> primaryKeyColumnNames, final String quotedIdentifierString) {
            this.columns = new HashMap<>();
            this.primaryKeyColumnNames = primaryKeyColumnNames;
            this.quotedIdentifierString = quotedIdentifierString;

            this.requiredColumnNames = new ArrayList<>();
            for (final ColumnDescription desc : columnDescriptions) {
                columns.put(normalizeColumnName(desc.columnName, translateColumnNames), desc);
                if (desc.isRequired()) {
                    requiredColumnNames.add(desc.columnName);
                }
            }
        }

        public Map<String, ColumnDescription> getColumns() {
            return columns;
        }

        public List<String> getRequiredColumnNames() {
            return requiredColumnNames;
        }

        public Set<String> getPrimaryKeyColumnNames() {
            return primaryKeyColumnNames;
        }

        public String getQuotedIdentifierString() {
            return quotedIdentifierString;
        }

        public static TableSchema from(final Connection conn, final String catalog, final String schema, final String tableName,
                                                        final boolean translateColumnNames, final boolean includePrimaryKeys) throws SQLException {
            final DatabaseMetaData dmd = conn.getMetaData();

            try (final ResultSet colrs = dmd.getColumns(catalog, schema, tableName, "%")) {
                final List<ColumnDescription> cols = new ArrayList<>();
                while (colrs.next()) {
                    final ColumnDescription col = ColumnDescription.from(colrs);
                    cols.add(col);
                }

                final Set<String> primaryKeyColumns = new HashSet<>();
                if (includePrimaryKeys) {
                    try (final ResultSet pkrs = conn.getMetaData().getPrimaryKeys(catalog, null, tableName)) {

                        while (pkrs.next()) {
                            final String colName = pkrs.getString("COLUMN_NAME");
                            primaryKeyColumns.add(normalizeColumnName(colName, translateColumnNames));
                        }
                    }
                }

                return new TableSchema(cols, translateColumnNames, primaryKeyColumns, dmd.getIdentifierQuoteString());
            }
        }
    }




    public static String generateSelect(final JsonNode rootNode, final Map<String, String> attributes, final String tableName, final String updateKeys,
                                  final TableSchema schema, final boolean translateFieldNames, final boolean ignoreUnmappedFields, final boolean failUnmappedColumns,
                                  final boolean warningUnmappedColumns, boolean escapeColumnNames, boolean quoteTableName, final String attributePrefix) {

        final Set<String> updateKeyNames;
        if (updateKeys == null) {
            updateKeyNames = schema.getPrimaryKeyColumnNames();
        } else {
            updateKeyNames = new HashSet<>();
            for (final String updateKey : updateKeys.split(",")) {
                updateKeyNames.add(updateKey.trim());
            }
        }

        if (updateKeyNames.isEmpty()) {
            throw new ProcessException("Table '" + tableName + "' does not have a Primary Key and no Update Keys were specified");
        }

        final StringBuilder sqlBuilder = new StringBuilder();
        int fieldCount = 0;
        sqlBuilder.append("SELECT count(*) cnt FROM ");
        if (quoteTableName) {
            sqlBuilder.append(schema.getQuotedIdentifierString())
                    .append(tableName)
                    .append(schema.getQuotedIdentifierString());
        } else {
            sqlBuilder.append(tableName);
        }


        // Create a Set of all normalized Update Key names, and ensure that there is a field in the JSON
        // for each of the Update Key fields.
        final Set<String> normalizedFieldNames = getNormalizedColumnNames(rootNode, translateFieldNames);
        final Set<String> normalizedUpdateNames = new HashSet<>();
//        for (final String uk : updateKeyNames) {
//            final String normalizedUK = normalizeColumnName(uk, translateFieldNames);
//            normalizedUpdateNames.add(normalizedUK);
//
//            if (!normalizedFieldNames.contains(normalizedUK)) {
//                String missingColMessage = "JSON does not have a value for the " + (updateKeys == null ? "Primary" : "Update") + "Key column '" + uk + "'";
//                if (failUnmappedColumns) {
//                    getLogger().error(missingColMessage);
//                    throw new ProcessException(missingColMessage);
//                } else if (warningUnmappedColumns) {
//                    getLogger().warn(missingColMessage);
//                }
//            }
//        }


        // Set the WHERE clause based on the Update Key values
        sqlBuilder.append(" WHERE ");

        Iterator<String> fieldNames  = rootNode.getFieldNames();
        int whereFieldCount = 0;
        while (fieldNames.hasNext()) {
            final String fieldName = fieldNames.next();

            final String normalizedColName = normalizeColumnName(fieldName, translateFieldNames);
            final ColumnDescription desc = schema.getColumns().get(normalizedColName);
            if (desc == null) {
                continue;
            }

            // Check if this column is a Update Key. If so, skip it for now. We will come
            // back to it after we finish the SET clause
            if (!normalizedUpdateNames.contains(normalizedColName)) {
                continue;
            }

            if (whereFieldCount++ > 0) {
                sqlBuilder.append(" AND ");
            }
            fieldCount++;

            if(escapeColumnNames){
                sqlBuilder.append(schema.getQuotedIdentifierString())
                        .append(normalizedColName)
                        .append(schema.getQuotedIdentifierString());
            } else {
                sqlBuilder.append(normalizedColName);
            }
            sqlBuilder.append(" = ?");
            final int sqlType = desc.getDataType();
            attributes.put(attributePrefix + ".args." + fieldCount + ".type", String.valueOf(sqlType));

            final Integer colSize = desc.getColumnSize();
            String fieldValue = rootNode.get(fieldName).asText();
            if (colSize != null && fieldValue.length() > colSize) {
                fieldValue = fieldValue.substring(0, colSize);
            }
            attributes.put(attributePrefix + ".args." + fieldCount + ".value", fieldValue);
        }

        return sqlBuilder.toString();
    }




    public static String generateUpdate(final JsonNode rootNode, final Map<String, String> attributes, final String tableName, final String updateKeys,
                                  final TableSchema schema, final boolean translateFieldNames, final boolean ignoreUnmappedFields, final boolean failUnmappedColumns,
                                  final boolean warningUnmappedColumns, boolean escapeColumnNames, boolean quoteTableName, final String attributePrefix) {

        final Set<String> updateKeyNames;
        if (updateKeys == null) {
            updateKeyNames = schema.getPrimaryKeyColumnNames();
        } else {
            updateKeyNames = new HashSet<>();
            for (final String updateKey : updateKeys.split(",")) {
                updateKeyNames.add(updateKey.trim());
            }
        }

        if (updateKeyNames.isEmpty()) {
            throw new ProcessException("Table '" + tableName + "' does not have a Primary Key and no Update Keys were specified");
        }

        final StringBuilder sqlBuilder = new StringBuilder();
        int fieldCount = 0;
        sqlBuilder.append("UPDATE ");
        if (quoteTableName) {
            sqlBuilder.append(schema.getQuotedIdentifierString())
                    .append(tableName)
                    .append(schema.getQuotedIdentifierString());
        } else {
            sqlBuilder.append(tableName);
        }

        sqlBuilder.append(" SET ");


        // Create a Set of all normalized Update Key names, and ensure that there is a field in the JSON
        // for each of the Update Key fields.
        final Set<String> normalizedFieldNames = getNormalizedColumnNames(rootNode, translateFieldNames);
        final Set<String> normalizedUpdateNames = new HashSet<>();
        for (final String uk : updateKeyNames) {
            final String normalizedUK = normalizeColumnName(uk, translateFieldNames);
            normalizedUpdateNames.add(normalizedUK);
//
//            if (!normalizedFieldNames.contains(normalizedUK)) {
//                String missingColMessage = "JSON does not have a value for the " + (updateKeys == null ? "Primary" : "Update") + "Key column '" + uk + "'";
//                if (failUnmappedColumns) {
//                    getLogger().error(missingColMessage);
//                    throw new ProcessException(missingColMessage);
//                } else if (warningUnmappedColumns) {
//                    getLogger().warn(missingColMessage);
//                }
//            }
        }

        // iterate over all of the elements in the JSON, building the SQL statement by adding the column names, as well as
        // adding the column value to a "<sql>.args.N.value" attribute and the type of a "<sql>.args.N.type" attribute add the
        // columns that we are inserting into
        Iterator<String> fieldNames = rootNode.getFieldNames();
        while (fieldNames.hasNext()) {
            final String fieldName = fieldNames.next();

            final String normalizedColName = normalizeColumnName(fieldName, translateFieldNames);
            final ColumnDescription desc = schema.getColumns().get(normalizedColName);
            if (desc == null) {
                if (!ignoreUnmappedFields) {
                    throw new ProcessException("Cannot map JSON field '" + fieldName + "' to any column in the database");
                } else {
                    continue;
                }
            }

            // Check if this column is an Update Key. If so, skip it for now. We will come
            // back to it after we finish the SET clause
            if (normalizedUpdateNames.contains(normalizedColName)) {
                continue;
            }

            if (fieldCount++ > 0) {
                sqlBuilder.append(", ");
            }

            if(escapeColumnNames){
                sqlBuilder.append(schema.getQuotedIdentifierString())
                        .append(desc.getColumnName())
                        .append(schema.getQuotedIdentifierString());
            } else {
                sqlBuilder.append(desc.getColumnName());
            }

            sqlBuilder.append(" = ?");
            final int sqlType = desc.getDataType();
            attributes.put(attributePrefix + ".args." + fieldCount + ".type", String.valueOf(sqlType));

            final Integer colSize = desc.getColumnSize();

            final JsonNode fieldNode = rootNode.get(fieldName);
            if (!fieldNode.isNull()) {
                String fieldValue = createSqlStringValue(fieldNode, colSize, sqlType);
                attributes.put(attributePrefix + ".args." + fieldCount + ".value", fieldValue);
            }
        }

        // Set the WHERE clause based on the Update Key values
        sqlBuilder.append(" WHERE ");

        fieldNames = rootNode.getFieldNames();
        int whereFieldCount = 0;
        while (fieldNames.hasNext()) {
            final String fieldName = fieldNames.next();

            final String normalizedColName = normalizeColumnName(fieldName, translateFieldNames);
            final ColumnDescription desc = schema.getColumns().get(normalizedColName);
            if (desc == null) {
                continue;
            }

            // Check if this column is a Update Key. If so, skip it for now. We will come
            // back to it after we finish the SET clause
            if (!normalizedUpdateNames.contains(normalizedColName)) {
                continue;
            }

            if (whereFieldCount++ > 0) {
                sqlBuilder.append(" AND ");
            }
            fieldCount++;

            if(escapeColumnNames){
                sqlBuilder.append(schema.getQuotedIdentifierString())
                        .append(normalizedColName)
                        .append(schema.getQuotedIdentifierString());
            } else {
                sqlBuilder.append(normalizedColName);
            }
            sqlBuilder.append(" = ?");
            final int sqlType = desc.getDataType();
            attributes.put(attributePrefix + ".args." + fieldCount + ".type", String.valueOf(sqlType));

            final Integer colSize = desc.getColumnSize();
            String fieldValue = rootNode.get(fieldName).asText();
            if (colSize != null && fieldValue.length() > colSize) {
                fieldValue = fieldValue.substring(0, colSize);
            }
            attributes.put(attributePrefix + ".args." + fieldCount + ".value", fieldValue);
        }

        return sqlBuilder.toString();
    }

    public static String generateDelete(final JsonNode rootNode, final Map<String, String> attributes, final String tableName,
                                  final TableSchema schema, final boolean translateFieldNames, final boolean ignoreUnmappedFields, final boolean failUnmappedColumns,
                                  final boolean warningUnmappedColumns, boolean escapeColumnNames, boolean quoteTableName, final String attributePrefix) {
        final Set<String> normalizedFieldNames = getNormalizedColumnNames(rootNode, translateFieldNames);
        for (final String requiredColName : schema.getRequiredColumnNames()) {
            final String normalizedColName = normalizeColumnName(requiredColName, translateFieldNames);
            if (!normalizedFieldNames.contains(normalizedColName)) {
                String missingColMessage = "JSON does not have a value for the Required column '" + requiredColName + "'";
//                if (failUnmappedColumns) {
//                    getLogger().error(missingColMessage);
//                    throw new ProcessException(missingColMessage);
//                } else if (warningUnmappedColumns) {
//                    getLogger().warn(missingColMessage);
//                }
            }
        }

        final StringBuilder sqlBuilder = new StringBuilder();
        int fieldCount = 0;
        sqlBuilder.append("DELETE FROM ");
        if (quoteTableName) {
            sqlBuilder.append(schema.getQuotedIdentifierString())
                    .append(tableName)
                    .append(schema.getQuotedIdentifierString());
        } else {
            sqlBuilder.append(tableName);
        }

        sqlBuilder.append(" WHERE ");

        // iterate over all of the elements in the JSON, building the SQL statement by adding the column names, as well as
        // adding the column value to a "<sql>.args.N.value" attribute and the type of a "<sql>.args.N.type" attribute add the
        // columns that we are inserting into
        final Iterator<String> fieldNames = rootNode.getFieldNames();
        while (fieldNames.hasNext()) {
            final String fieldName = fieldNames.next();

            final ColumnDescription desc = schema.getColumns().get(normalizeColumnName(fieldName, translateFieldNames));
            if (desc == null && !ignoreUnmappedFields) {
                throw new ProcessException("Cannot map JSON field '" + fieldName + "' to any column in the database");
            }

            if (desc != null) {
                if (fieldCount++ > 0) {
                    sqlBuilder.append(" AND ");
                }

                if (escapeColumnNames) {
                    sqlBuilder.append(schema.getQuotedIdentifierString())
                            .append(desc.getColumnName())
                            .append(schema.getQuotedIdentifierString());
                } else {
                    sqlBuilder.append(desc.getColumnName());
                }
                sqlBuilder.append(" = ?");

                final int sqlType = desc.getDataType();
                attributes.put(attributePrefix + ".args." + fieldCount + ".type", String.valueOf(sqlType));

                final Integer colSize = desc.getColumnSize();
                final JsonNode fieldNode = rootNode.get(fieldName);
                if (!fieldNode.isNull()) {
                    String fieldValue = fieldNode.asText();
                    if (colSize != null && fieldValue.length() > colSize) {
                        fieldValue = fieldValue.substring(0, colSize);
                    }
                    attributes.put(attributePrefix + ".args." + fieldCount + ".value", fieldValue);
                }
            }
        }

        if (fieldCount == 0) {
            throw new ProcessException("None of the fields in the JSON map to the columns defined by the " + tableName + " table");
        }

        return sqlBuilder.toString();
    }



    public static String generateInsert(final JsonNode rootNode, final Map<String, String> attributes, final String tableName,
                                  final TableSchema schema, final boolean translateFieldNames, final boolean ignoreUnmappedFields, final boolean failUnmappedColumns,
                                  final boolean warningUnmappedColumns, boolean escapeColumnNames, boolean quoteTableName, final String attributePrefix) {

        final Set<String> normalizedFieldNames = getNormalizedColumnNames(rootNode, translateFieldNames);
        for (final String requiredColName : schema.getRequiredColumnNames()) {
            final String normalizedColName = normalizeColumnName(requiredColName, translateFieldNames);
//            if (!normalizedFieldNames.contains(normalizedColName)) {
//                String missingColMessage = "JSON does not have a value for the Required column '" + requiredColName + "'";
//                if (failUnmappedColumns) {
//                    getLogger().error(missingColMessage);
//                    throw new ProcessException(missingColMessage);
//                } else if (warningUnmappedColumns) {
//                    getLogger().warn(missingColMessage);
//                }
//            }
        }

        final StringBuilder sqlBuilder = new StringBuilder();
        int fieldCount = 0;
        sqlBuilder.append("INSERT INTO ");
        if (quoteTableName) {
            sqlBuilder.append(schema.getQuotedIdentifierString())
                    .append(tableName)
                    .append(schema.getQuotedIdentifierString());
        } else {
            sqlBuilder.append(tableName);
        }
        sqlBuilder.append(" (");

        // iterate over all of the elements in the JSON, building the SQL statement by adding the column names, as well as
        // adding the column value to a "<sql>.args.N.value" attribute and the type of a "<sql>.args.N.type" attribute add the
        // columns that we are inserting into
        final Iterator<String> fieldNames = rootNode.getFieldNames();
        while (fieldNames.hasNext()) {
            final String fieldName = fieldNames.next();

            final ColumnDescription desc = schema.getColumns().get(normalizeColumnName(fieldName, translateFieldNames));
            if (desc == null && !ignoreUnmappedFields) {
                throw new ProcessException("Cannot map JSON field '" + fieldName + "' to any column in the database");
            }

            if (desc != null) {
                if (fieldCount++ > 0) {
                    sqlBuilder.append(", ");
                }

                if(escapeColumnNames){
                    sqlBuilder.append(schema.getQuotedIdentifierString())
                            .append(desc.getColumnName())
                            .append(schema.getQuotedIdentifierString());
                } else {
                    sqlBuilder.append(desc.getColumnName());
                }

                final int sqlType = desc.getDataType();
                attributes.put(attributePrefix + ".args." + fieldCount + ".type", String.valueOf(sqlType));

                final Integer colSize = desc.getColumnSize();
                final JsonNode fieldNode = rootNode.get(fieldName);
                if (!fieldNode.isNull()) {
                    String fieldValue = createSqlStringValue(fieldNode, colSize, sqlType);
                    attributes.put(attributePrefix + ".args." + fieldCount + ".value", fieldValue);
                }
            }
        }

        // complete the SQL statements by adding ?'s for all of the values to be escaped.
        sqlBuilder.append(") VALUES (");
        for (int i=0; i < fieldCount; i++) {
            if (i > 0) {
                sqlBuilder.append(", ");
            }

            sqlBuilder.append("?");
        }
        sqlBuilder.append(")");

        if (fieldCount == 0) {
            throw new ProcessException("None of the fields in the JSON map to the columns defined by the " + tableName + " table");
        }

        return sqlBuilder.toString();
    }


    public static String generateSelectCount( final String tableName,
                                        final TableSchema schema,
                                        boolean quoteTableName ) {


        final StringBuilder sqlBuilder = new StringBuilder();
        int fieldCount = 0;
        sqlBuilder.append("SELECT count(*) cnt FROM ");
        if (quoteTableName) {
            sqlBuilder.append(schema.getQuotedIdentifierString())
                    .append(tableName)
                    .append(schema.getQuotedIdentifierString());
        } else {
            sqlBuilder.append(tableName);
        }

        return sqlBuilder.toString();
    }

    public static String normalizeColumnName(final String colName, final boolean translateColumnNames) {
        return translateColumnNames ? colName.toUpperCase().replace("_", "") : colName;
    }

    /**
     *  Try to create correct SQL String representation of value.
     *
     */
    public static String createSqlStringValue(final JsonNode fieldNode, final Integer colSize, final int sqlType) {
        String fieldValue = fieldNode.asText();

        switch (sqlType) {

            // only "true" is considered true, everything else is false
            case Types.BOOLEAN:
                fieldValue = Boolean.valueOf(fieldValue).toString();
                break;

            // Don't truncate numeric types.
            case Types.BIT:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.DECIMAL:
            case Types.NUMERIC:
                if (fieldNode.isBoolean()) {
                    // Convert boolean to number representation for databases those don't support boolean type.
                    fieldValue = fieldNode.asBoolean() ? "1" : "0";
                }
                break;

            // Don't truncate DATE, TIME and TIMESTAMP types. We assume date and time is already correct in long representation.
            // Specifically, milliseconds since January 1, 1970, 00:00:00 GMT
            // However, for TIMESTAMP, PutSQL accepts optional timestamp format via FlowFile attribute.
            // See PutSQL.setParameter method and NIFI-3430 for detail.
            // Alternatively, user can use JSONTreeReader and PutDatabaseRecord to handle date format more efficiently.
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                break;

            // Truncate string data types only.
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                if (colSize != null && fieldValue.length() > colSize) {
                    fieldValue = fieldValue.substring(0, colSize);
                }
                break;
        }

        return fieldValue;
    }

    public static Set<String> getNormalizedColumnNames(final JsonNode node, final boolean translateFieldNames) {
        final Set<String> normalizedFieldNames = new HashSet<>();
        final Iterator<String> fieldNameItr = node.getFieldNames();
        while (fieldNameItr.hasNext()) {
            normalizedFieldNames.add(normalizeColumnName(fieldNameItr.next(), translateFieldNames));
        }

        return normalizedFieldNames;
    }


}
