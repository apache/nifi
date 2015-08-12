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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
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
import org.apache.nifi.util.ObjectHolder;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;


@SideEffectFree
@SupportsBatching
@SeeAlso(PutSQL.class)
@Tags({"json", "sql", "database", "rdbms", "insert", "update", "relational", "flat"})
@CapabilityDescription("Converts a JSON-formatted FlowFile into an UPDATE or INSERT SQL statement. The incoming FlowFile is expected to be "
		+ "\"flat\" JSON message, meaning that it consists of a single JSON element and each field maps to a simple type. If a field maps to "
		+ "a JSON object, that JSON object will be interpreted as Text. Upon successful conversion, the original FlowFile is routed to the 'original' "
		+ "relationship and the SQL is routed to the 'sql' relationship.")
public class ConvertFlatJSONToSQL extends AbstractProcessor {
	private static final String UPDATE_TYPE = "UPDATE";
	private static final String INSERT_TYPE = "INSERT";

	static final PropertyDescriptor CONNECTION_POOL = new PropertyDescriptor.Builder()
			.name("JDBC Connection Pool")
			.description("Specifies the JDBC Connection Pool to use in order to convert the JSON message to a SQL statement. "
					+ "The Connection Pool is necessary in order to determine the appropriate database column types.")
			.identifiesControllerService(DBCPService.class)
			.required(true)
			.build();
	static final PropertyDescriptor STATEMENT_TYPE = new PropertyDescriptor.Builder()
			.name("Statement Type")
			.description("Specifies the type of SQL Statement to generate")
			.required(true)
			.allowableValues(UPDATE_TYPE, INSERT_TYPE)
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
	static final PropertyDescriptor TRANSLATE_FIELD_NAMES = new PropertyDescriptor.Builder()
			.name("Translate Field Names")
			.description("If true, the Processor will attempt to translate JSON field names into the appropriate column names for the table specified. "
					+ "If false, the JSON field names must match the column names exactly, or the column will not be updated")
			.allowableValues("true", "false")
			.defaultValue("true")
			.build();
	static final PropertyDescriptor UPDATE_KEY = new PropertyDescriptor.Builder()
			.name("Update Keys")
			.description("A comma-separated list of column names that uniquely identifies a row in the database for UPDATE statements. "
					+ "If the Statement Type is UPDATE and this property is not set, the table's Primary Keys are used. "
					+ "In this case, if no Primary Key exists, the conversion to SQL will fail. "
					+ "This property is ignored if the Statement Type is INSERT")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.required(false)
			.expressionLanguageSupported(true)
			.build();


	static final Relationship REL_ORIGINAL = new Relationship.Builder()
			.name("original")
			.description("When a FlowFile is converted to SQL, the original JSON FlowFile is routed to this relationship")
			.build();
	static final Relationship REL_SQL = new Relationship.Builder()
			.name("sql")
			.description("A FlowFile is routed to this relationship when its contents have successfully been converted into a SQL statement")
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
			return true;
		}
	};

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		final List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(CONNECTION_POOL);
		properties.add(STATEMENT_TYPE);
		properties.add(TABLE_NAME);
		properties.add(CATALOG_NAME);
		properties.add(TRANSLATE_FIELD_NAMES);
		properties.add(UPDATE_KEY);
		return properties;
	}


	@Override
	public Set<Relationship> getRelationships() {
		final Set<Relationship> rels = new HashSet<>();
		rels.add(REL_ORIGINAL);
		rels.add(REL_SQL);
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
		final String statementType = context.getProperty(STATEMENT_TYPE).getValue();
		final String updateKeys = context.getProperty(UPDATE_KEY).evaluateAttributeExpressions(flowFile).getValue();

		final String catalog = context.getProperty(CATALOG_NAME).evaluateAttributeExpressions(flowFile).getValue();
		final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
		final SchemaKey schemaKey = new SchemaKey(catalog, tableName);
		final boolean includePrimaryKeys = UPDATE_TYPE.equals(statementType) && updateKeys == null;

		// get the database schema from the cache, if one exists
		TableSchema schema;
		synchronized (this) {
			schema = schemaCache.get(schemaKey);
			if (schema == null) {
				// No schema exists for this table yet. Query the database to determine the schema and put it into the cache.
				final DBCPService dbcpService = context.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class);
				try (final Connection conn = dbcpService.getConnection()) {
					schema = TableSchema.from(conn, catalog, tableName, translateFieldNames, includePrimaryKeys);
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
		final ObjectHolder<JsonNode> rootNodeRef = new ObjectHolder<>(null);
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
		final String sql;
		final Map<String, String> attributes = new HashMap<>();
		if (INSERT_TYPE.equals(statementType)) {
			try {
				sql = generateInsert(rootNode, attributes, tableName, schema, translateFieldNames);
			} catch (final ProcessException pe) {
				getLogger().error("Failed to convert {} to a SQL INSERT statement due to {}; routing to failure",
						new Object[] { flowFile, pe.toString() }, pe);
				session.transfer(flowFile, REL_FAILURE);
				return;
			}
		} else {
			try {
				sql = generateUpdate(rootNode, attributes, tableName, updateKeys, schema, translateFieldNames);
			} catch (final ProcessException pe) {
				getLogger().error("Failed to convert {} to a SQL UPDATE statement due to {}; routing to failure",
						new Object[] { flowFile, pe.toString() }, pe);
				session.transfer(flowFile, REL_FAILURE);
				return;
			}
		}

		FlowFile sqlFlowFile = session.create(flowFile);
		sqlFlowFile = session.write(sqlFlowFile, new OutputStreamCallback() {
			@Override
			public void process(final OutputStream out) throws IOException {
				out.write(sql.getBytes(StandardCharsets.UTF_8));
			}
		});

		attributes.put(CoreAttributes.MIME_TYPE.key(), "text/plain");
		attributes.put("sql.table", tableName);
		if (catalog != null) {
			attributes.put("sql.catalog", catalog);
		}

		sqlFlowFile = session.putAllAttributes(sqlFlowFile, attributes);

		session.transfer(flowFile, REL_ORIGINAL);
		session.transfer(sqlFlowFile, REL_SQL);
	}

	private Set<String> getNormalizedColumnNames(final JsonNode node, final boolean translateFieldNames) {
		final Set<String> normalizedFieldNames = new HashSet<>();
		final Iterator<String> fieldNameItr = node.getFieldNames();
		while (fieldNameItr.hasNext()) {
			normalizedFieldNames.add(normalizeColumnName(fieldNameItr.next(), translateFieldNames));
		}

		return normalizedFieldNames;
	}

	private String generateInsert(final JsonNode rootNode, final Map<String, String> attributes, final String tableName,
			final TableSchema schema, final boolean translateFieldNames) {

		final Set<String> normalizedFieldNames = getNormalizedColumnNames(rootNode, translateFieldNames);
		for (final String requiredColName : schema.getRequiredColumnNames()) {
			final String normalizedColName = normalizeColumnName(requiredColName, translateFieldNames);
			if (!normalizedFieldNames.contains(normalizedColName)) {
				throw new ProcessException("JSON does not have a value for the Required column '" + requiredColName + "'");
			}
		}

		final StringBuilder sqlBuilder = new StringBuilder();
		int fieldCount = 0;
		sqlBuilder.append("INSERT INTO ").append(tableName).append(" (");

		// iterate over all of the elements in the JSON, building the SQL statement by adding the column names, as well as
		// adding the column value to a "sql.args.N.value" attribute and the type of a "sql.args.N.type" attribute add the
		// columns that we are inserting into
		final Iterator<String> fieldNames = rootNode.getFieldNames();
		while (fieldNames.hasNext()) {
			final String fieldName = fieldNames.next();

			final ColumnDescription desc = schema.getColumns().get(normalizeColumnName(fieldName, translateFieldNames));
			if (desc != null) {
				if (fieldCount++ > 0) {
					sqlBuilder.append(", ");
				}

				sqlBuilder.append(desc.getColumnName());

				final int sqlType = desc.getDataType();
				attributes.put("sql.args." + fieldCount + ".type", String.valueOf(sqlType));

				final Integer colSize = desc.getColumnSize();
				String fieldValue = rootNode.get(fieldName).asText();
				if (colSize != null && fieldValue.length() > colSize) {
					fieldValue = fieldValue.substring(0, colSize);
				}
				attributes.put("sql.args." + fieldCount + ".value", fieldValue);
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

	private String generateUpdate(final JsonNode rootNode, final Map<String, String> attributes, final String tableName, final String updateKeys,
			final TableSchema schema, final boolean translateFieldNames) {

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
		sqlBuilder.append("UPDATE ").append(tableName).append(" SET ");


		// Create a Set of all normalized Update Key names, and ensure that there is a field in the JSON
		// for each of the Update Key fields.
		final Set<String> normalizedFieldNames = getNormalizedColumnNames(rootNode, translateFieldNames);
		final Set<String> normalizedUpdateNames = new HashSet<>();
		for (final String uk : updateKeyNames) {
			final String normalizedUK = normalizeColumnName(uk, translateFieldNames);
			normalizedUpdateNames.add(normalizedUK);

			if (!normalizedFieldNames.contains(normalizedUK)) {
				throw new ProcessException("JSON does not have a value for the " + (updateKeys == null ? "Primary" : "Update") + "Key column '" + uk + "'");
			}
		}

		// iterate over all of the elements in the JSON, building the SQL statement by adding the column names, as well as
		// adding the column value to a "sql.args.N.value" attribute and the type of a "sql.args.N.type" attribute add the
		// columns that we are inserting into
		Iterator<String> fieldNames = rootNode.getFieldNames();
		while (fieldNames.hasNext()) {
			final String fieldName = fieldNames.next();

			final String normalizedColName = normalizeColumnName(fieldName, translateFieldNames);
			final ColumnDescription desc = schema.getColumns().get(normalizedColName);
			if (desc == null) {
				continue;
			}

			// Check if this column is an Update Key. If so, skip it for now. We will come
			// back to it after we finish the SET clause
			if (normalizedUpdateNames.contains(normalizedColName)) {
				continue;
			}

			if (fieldCount++ > 0) {
				sqlBuilder.append(", ");
			}

			sqlBuilder.append(desc.getColumnName()).append(" = ?");
			final int sqlType = desc.getDataType();
			attributes.put("sql.args." + fieldCount + ".type", String.valueOf(sqlType));

			final Integer colSize = desc.getColumnSize();
			String fieldValue = rootNode.get(fieldName).asText();
			if (colSize != null && fieldValue.length() > colSize) {
				fieldValue = fieldValue.substring(0, colSize);
			}
			attributes.put("sql.args." + fieldCount + ".value", fieldValue);
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

			sqlBuilder.append(normalizedColName).append(" = ?");
			final int sqlType = desc.getDataType();
			attributes.put("sql.args." + fieldCount + ".type", String.valueOf(sqlType));

			final Integer colSize = desc.getColumnSize();
			String fieldValue = rootNode.get(fieldName).asText();
			if (colSize != null && fieldValue.length() > colSize) {
				fieldValue = fieldValue.substring(0, colSize);
			}
			attributes.put("sql.args." + fieldCount + ".value", fieldValue);
		}

		return sqlBuilder.toString();
	}

	private static String normalizeColumnName(final String colName, final boolean translateColumnNames) {
		return translateColumnNames ? colName.toUpperCase().replace("_", "") : colName;
	}

	private static class TableSchema {
		private List<String> requiredColumnNames;
		private Set<String> primaryKeyColumnNames;
		private Map<String, ColumnDescription> columns;

		private TableSchema(final List<ColumnDescription> columnDescriptions, final boolean translateColumnNames,
				final Set<String> primaryKeyColumnNames) {
			this.columns = new HashMap<>();
			this.primaryKeyColumnNames = primaryKeyColumnNames;

			this.requiredColumnNames = new ArrayList<>();
			for (final ColumnDescription desc : columnDescriptions) {
				columns.put(ConvertFlatJSONToSQL.normalizeColumnName(desc.columnName, translateColumnNames), desc);
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

		public static TableSchema from(final Connection conn, final String catalog, final String tableName,
				final boolean translateColumnNames, final boolean includePrimaryKeys) throws SQLException {
			final ResultSet colrs = conn.getMetaData().getColumns(catalog, null, tableName, "%");

			final List<ColumnDescription> cols = new ArrayList<>();
			while (colrs.next()) {
				final ColumnDescription col = ColumnDescription.from(colrs);
				cols.add(col);
			}

			final Set<String> primaryKeyColumns = new HashSet<>();
			if (includePrimaryKeys) {
				final ResultSet pkrs = conn.getMetaData().getPrimaryKeys(catalog, null, tableName);

				while (pkrs.next()) {
					final String colName = pkrs.getString("COLUMN_NAME");
					primaryKeyColumns.add(normalizeColumnName(colName, translateColumnNames));
				}
			}

			return new TableSchema(cols, translateColumnNames, primaryKeyColumns);
		}
	}

	private static class ColumnDescription {
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
			final String columnName = resultSet.getString("COLUMN_NAME");
			final int dataType = resultSet.getInt("DATA_TYPE");
			final int colSize = resultSet.getInt("COLUMN_SIZE");

			final String nullableValue = resultSet.getString("IS_NULLABLE");
			final boolean isNullable = "YES".equalsIgnoreCase(nullableValue) || nullableValue.isEmpty();
			final String defaultValue = resultSet.getString("COLUMN_DEF");
			final String autoIncrementValue = resultSet.getString("IS_AUTOINCREMENT");
			final boolean isAutoIncrement = "YES".equalsIgnoreCase(autoIncrementValue);
			final boolean required = !isNullable && !isAutoIncrement && defaultValue == null;

			return new ColumnDescription(columnName, dataType, required, colSize == 0 ? null : colSize);
		}
	}

	private static class SchemaKey {
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
}
