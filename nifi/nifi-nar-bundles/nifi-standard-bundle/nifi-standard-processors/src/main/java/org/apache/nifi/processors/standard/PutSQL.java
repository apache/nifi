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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;

@SupportsBatching
@SeeAlso(ConvertFlatJSONToSQL.class)
@Tags({"sql", "put", "rdbms", "database", "update", "insert", "relational"})
@CapabilityDescription("Executes a SQL UPDATE or INSERT command. The content of an incoming FlowFile is expected to be the SQL command "
		+ "to execute. The SQL command may use the ? to escape parameters. In this case, the parameters to use must exist as FlowFile attributes "
		+ "with the naming convention sql.args.N.type and sql.args.N.value, where N is a positive integer. The sql.args.N.type is expected to be "
		+ "a number indicating the JDBC Type. The content of the FlowFile is expected to be in UTF-8 format.")
public class PutSQL extends AbstractProcessor {

	static final PropertyDescriptor CONNECTION_POOL = new PropertyDescriptor.Builder()
			.name("JDBC Connection Pool")
			.description("Specifies the JDBC Connection Pool to use in order to convert the JSON message to a SQL statement. "
					+ "The Connection Pool is necessary in order to determine the appropriate database column types.")
			.identifiesControllerService(DBCPService.class)
			.required(true)
			.build();


	static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success")
			.description("A FlowFile is routed to this relationship after the database is successfully updated")
			.build();
	static final Relationship REL_FAILURE = new Relationship.Builder()
			.name("failure")
			.description("A FlowFile is routed to this relationship if the database cannot be updated for any reason")
			.build();

	private static final Pattern SQL_TYPE_ATTRIBUTE_PATTERN = Pattern.compile("sql\\.args\\.(\\d+)\\.type");
	private static final Pattern NUMBER_PATTERN = Pattern.compile("\\d+");

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		final List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(CONNECTION_POOL);
		return properties;
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}

		// TODO: Batch. Pull in 50 or 100 at a time and map content of FlowFile to Set<FlowFile> that have that
		// same content. Then execute updates in batches.

		final DBCPService dbcpService = context.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class);
		try (final Connection conn = dbcpService.getConnection()) {
			// Read the SQL from the FlowFile's content
			final byte[] buffer = new byte[(int) flowFile.getSize()];
			session.read(flowFile, new InputStreamCallback() {
				@Override
				public void process(final InputStream in) throws IOException {
					StreamUtils.fillBuffer(in, buffer);
				}
			});

			final String sql = new String(buffer, StandardCharsets.UTF_8);

			// Create a prepared statement and set the appropriate parameters on the statement.
			try (final PreparedStatement stmt = conn.prepareStatement(sql)) {
				for (final Map.Entry<String, String> entry : flowFile.getAttributes().entrySet()) {
					final String key = entry.getKey();
					final Matcher matcher = SQL_TYPE_ATTRIBUTE_PATTERN.matcher(key);
					if (matcher.matches()) {
						final int parameterIndex = Integer.parseInt(matcher.group(1));

						final boolean isNumeric = NUMBER_PATTERN.matcher(entry.getValue()).matches();
						if (!isNumeric) {
							getLogger().error("Cannot update database for {} because the value of the '{}' attribute is '{}', which is not a valid JDBC numeral type; routing to failure",
									new Object[] {flowFile, key, entry.getValue()});
							session.transfer(flowFile, REL_FAILURE);
							return;
						}

						final int jdbcType = Integer.parseInt(entry.getValue());
						final String valueAttrName = "sql.args." + parameterIndex + ".value";
						final String parameterValue = flowFile.getAttribute(valueAttrName);
						if (parameterValue == null) {
							getLogger().error("Cannot update database for {} because the '{}' attribute exists but the '{}' attribute does not", new Object[] {flowFile, key, valueAttrName});
							session.transfer(flowFile, REL_FAILURE);
							return;
						}

						try {
							setParameter(stmt, valueAttrName, parameterIndex, parameterValue, jdbcType);
						} catch (final NumberFormatException nfe) {
							getLogger().error("Cannot update database for {} because the '{}' attribute has a value of '{}', "
									+ "which cannot be converted into the necessary data type; routing to failure", new Object[] {flowFile, valueAttrName, parameterValue});
							session.transfer(flowFile, REL_FAILURE);
							return;
						}
					}
				}

				final int updatedRowCount = stmt.executeUpdate();
				flowFile = session.putAttribute(flowFile, "sql.update.count", String.valueOf(updatedRowCount));
			}

			// TODO: Need to expose Connection URL from DBCP Service and use it to emit a Provenance Event
			session.transfer(flowFile, REL_SUCCESS);
		} catch (final SQLException e) {
			getLogger().error("Failed to update database for {} due to {}; routing to failure", new Object[] {flowFile, e});
			session.transfer(flowFile, REL_FAILURE);
			return;
		}
	}

	private void setParameter(final PreparedStatement stmt, final String attrName, final int parameterIndex, final String parameterValue, final int jdbcType) throws SQLException {
		switch (jdbcType) {
			case Types.BIT:
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
				throw new SQLException("The '" + attrName + "' attribute has a value of '" + parameterValue
						+ "' and a type of '" + jdbcType + "' but this is not a known data type");
		}
	}
}
