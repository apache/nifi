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

import static java.sql.Types.CHAR;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.VARCHAR;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.adapter.csv.CsvSchemaFactory2;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.util.StopWatch;

import com.google.common.collect.ImmutableMap;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"xml", "xslt", "transform"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Filter out specific columns from CSV data. Some other transformations are also supported."
        + "Columns can be renamed, simple calculations performed, aggregations, etc."
        + "SQL select statement is used to specify how CSV data should be transformed."
        + "SQL statement follows standard SQL, some restrictions may apply."
        + "Successfully transformed CSV data is routed to the 'success' relationship."
        + "If transform fails, the original FlowFile is routed to the 'failure' relationship")
public class FilterCSVColumns  extends AbstractProcessor {

    public static final PropertyDescriptor SQL_SELECT = new PropertyDescriptor.Builder()
            .name("SQL select statement")
            .description("SQL select statement specifies how CSV data should be transformed. "
                       + "Sql select should select from CSV.A table")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The FlowFile with transformed content will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the SQL statement contains columns not present in CSV), it will be routed to this relationship")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SQL_SELECT);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final ProcessorLog logger = getLogger();
        final StopWatch stopWatch = new StopWatch(true);

        try {
            FlowFile transformed = session.write(original, new StreamCallback() {
                @Override
                public void process(final InputStream rawIn, final OutputStream out) throws IOException {
                    try (final InputStream in = new BufferedInputStream(rawIn)) {

                        String sql = context.getProperty(SQL_SELECT).getValue();
                        final ResultSet resultSet = transform(rawIn, sql);
                        convertToCSV(resultSet, out);

                    } catch (final Exception e) {
                        throw new IOException(e);
                    }
                }
            });
            session.transfer(transformed, REL_SUCCESS);
            session.getProvenanceReporter().modifyContent(transformed, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            logger.info("Transformed {}", new Object[]{original});
        } catch (ProcessException e) {
            logger.error("Unable to transform {} due to {}", new Object[]{original, e});
            session.transfer(original, REL_FAILURE);
        }
    }

    static protected ResultSet transform(InputStream rawIn, String sql) throws SQLException {

        Reader readerx = new InputStreamReader(rawIn);
        HashMap<String, Reader> inputs = new HashMap<>();
        inputs.put("A", readerx);

        Statement statement = null;
        final Properties properties = new Properties();
//      properties.setProperty("caseSensitive", "true");
        try (final Connection connection = DriverManager.getConnection("jdbc:calcite:", properties)) {
            final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

            final SchemaPlus rootSchema = calciteConnection.getRootSchema();
            final Schema schema =
              new CsvSchemaFactory2(inputs)
                  .create(rootSchema, "CSV", ImmutableMap.<String, Object>of("flavor", "TRANSLATABLE"));

            calciteConnection.getRootSchema().add("CSV", schema);
            rootSchema.add("default", schema);

            statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery(sql);
            return resultSet;
        }
    }

    static protected void convertToCSV(ResultSet resultSet, OutputStream out) throws SQLException, IOException {

        convertToCsvStream(resultSet, out);
    }

    public static long convertToCsvStream(final ResultSet rs, final OutputStream outStream) throws SQLException, IOException {
        return convertToCsvStream(rs, outStream, null, null);
    }

    public static long convertToCsvStream(final ResultSet rs, final OutputStream outStream, String recordName, ResultSetRowCallback callback)
            throws SQLException, IOException {

        final ResultSetMetaData meta = rs.getMetaData();
        final int nrOfColumns = meta.getColumnCount();
        List<String> columnNames = new ArrayList<>(nrOfColumns);

        for (int i = 1; i <= nrOfColumns; i++) {
            String columnNameFromMeta = meta.getColumnName(i);
            // Hive returns table.column for column name. Grab the column name as the string after the last period
            int columnNameDelimiter = columnNameFromMeta.lastIndexOf(".");
            columnNames.add(columnNameFromMeta.substring(columnNameDelimiter + 1));
        }

        // Write column names as header row
        outStream.write(StringUtils.join(columnNames, ",").getBytes(StandardCharsets.UTF_8));
        outStream.write("\n".getBytes(StandardCharsets.UTF_8));

        // Iterate over the rows
        long nrOfRows = 0;
        while (rs.next()) {
            if (callback != null) {
                callback.processRow(rs);
            }
            List<String> rowValues = new ArrayList<>(nrOfColumns);
            for (int i = 1; i <= nrOfColumns; i++) {
                final int javaSqlType = meta.getColumnType(i);
                final Object value = rs.getObject(i);

                switch (javaSqlType) {
                    case CHAR:
                    case LONGNVARCHAR:
                    case LONGVARCHAR:
                    case NCHAR:
                    case NVARCHAR:
                    case VARCHAR:
                        rowValues.add("\"" + StringEscapeUtils.escapeCsv(rs.getString(i)) + "\"");
                        break;
                    default:
                        rowValues.add(value.toString());
                }
            }
            // Write row values
            outStream.write(StringUtils.join(rowValues, ",").getBytes(StandardCharsets.UTF_8));
            outStream.write("\n".getBytes(StandardCharsets.UTF_8));
            nrOfRows++;
        }
        return nrOfRows;
    }

    /**
     * An interface for callback methods which allows processing of a row during the convertToXYZStream() processing.
     * <b>IMPORTANT:</b> This method should only work on the row pointed at by the current ResultSet reference.
     * Advancing the cursor (e.g.) can cause rows to be skipped during Avro transformation.
     */
    public interface ResultSetRowCallback {
        void processRow(ResultSet resultSet) throws IOException;
    }
}
