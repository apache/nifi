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
package org.apache.nifi.hbase;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hbase.io.JsonRowSerializer;
import org.apache.nifi.hbase.io.RowSerializer;
import org.apache.nifi.hbase.scan.Column;
import org.apache.nifi.hbase.scan.ResultCell;
import org.apache.nifi.hbase.scan.ResultHandler;
import org.apache.nifi.hbase.util.ObjectSerDe;
import org.apache.nifi.hbase.util.StringSerDe;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.ObjectHolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

@TriggerWhenEmpty
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"hbase", "get", "ingest"})
@CapabilityDescription("This Processor polls HBase for any records in the specified table. The processor keeps track of the timestamp of the cells that "
        + "it receives, so that as new records are pushed to HBase, they will automatically be pulled. Each record is output in JSON format, as "
        + "{\"row\": \"<row key>\", \"cells\": { \"<column 1 family>:<column 1 qualifier>\": \"<cell 1 value>\", \"<column 2 family>:<column 2 qualifier>\": \"<cell 2 value>\", ... }}. "
        + "For each record received, a Provenance RECEIVE event is emitted with the format hbase://<table name>/<row key>, where <row key> is the UTF-8 encoded value of the row's key.")
@WritesAttributes({
    @WritesAttribute(attribute = "hbase.table", description = "The name of the HBase table that the data was pulled from"),
    @WritesAttribute(attribute = "mime.type", description = "Set to application/json to indicate that output is JSON")
})
public class GetHBase extends AbstractHBaseProcessor {

    static final Pattern COLUMNS_PATTERN = Pattern.compile("\\w+(:\\w+)?(?:,\\w+(:\\w+)?)*");

    static final AllowableValue NONE = new AllowableValue("None", "None");
    static final AllowableValue CURRENT_TIME = new AllowableValue("Current Time", "Current Time");

    static final PropertyDescriptor HBASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("HBase Client Service")
            .description("Specifies the Controller Service to use for accessing HBase.")
            .required(true)
            .identifiesControllerService(HBaseClientService.class)
            .build();
    static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name("Distributed Cache Service")
            .description("Specifies the Controller Service that should be used to maintain state about what has been pulled from HBase" +
                    " so that if a new node begins pulling data, it won't duplicate all of the work that has been done.")
            .required(true)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .build();
    static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("Specifies which character set is used to encode the data in HBase")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();
    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the HBase Table to put data into")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor COLUMNS = new PropertyDescriptor.Builder()
            .name("Columns")
            .description("A comma-separated list of \"<colFamily>:<colQualifier>\" pairs to return when scanning. To return all columns " +
                    "for a given family, leave off the qualifier such as \"<colFamily1>,<colFamily2>\".")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.createRegexMatchingValidator(COLUMNS_PATTERN))
            .build();
    static final PropertyDescriptor FILTER_EXPRESSION = new PropertyDescriptor.Builder()
            .name("Filter Expression")
            .description("An HBase filter expression that will be applied to the scan. This property can not be used when also using the Columns property.")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor INITIAL_TIMERANGE = new PropertyDescriptor.Builder()
            .name("Initial Time Range")
            .description("The time range to use on the first scan of a table. None will pull the entire table on the first scan, " +
                    "Current Time will pull entries from that point forward.")
            .required(true)
            .expressionLanguageSupported(false)
            .allowableValues(NONE, CURRENT_TIME)
            .defaultValue(NONE.getValue())
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are routed to this relationship")
            .build();

    private volatile ScanResult lastResult = null;
    private volatile List<Column> columns = new ArrayList<>();
    private volatile boolean electedPrimaryNode = false;
    private volatile String previousTable = null;

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HBASE_CLIENT_SERVICE);
        properties.add(DISTRIBUTED_CACHE_SERVICE);
        properties.add(TABLE_NAME);
        properties.add(COLUMNS);
        properties.add(FILTER_EXPRESSION);
        properties.add(INITIAL_TIMERANGE);
        properties.add(CHARSET);
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final String columns = validationContext.getProperty(COLUMNS).getValue();
        final String filter = validationContext.getProperty(FILTER_EXPRESSION).getValue();

        final List<ValidationResult> problems = new ArrayList<>();

        if (!StringUtils.isBlank(columns) && !StringUtils.isBlank(filter)) {
            problems.add(new ValidationResult.Builder()
                    .subject(FILTER_EXPRESSION.getDisplayName())
                    .input(filter).valid(false)
                    .explanation("a filter expression can not be used in conjunction with the Columns property")
                    .build());
        }

        return problems;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.equals(TABLE_NAME)) {
            lastResult = null;
        }
    }

    @OnScheduled
    public void parseColumns(final ProcessContext context) {
        final String columnsValue = context.getProperty(COLUMNS).getValue();
        final String[] columns = (columnsValue == null || columnsValue.isEmpty() ? new String[0] : columnsValue.split(","));

        this.columns.clear();
        for (final String column : columns) {
            if (column.contains(":"))  {
                final String[] parts = column.split(":");
                final byte[] cf = parts[0].getBytes(Charset.forName("UTF-8"));
                final byte[] cq = parts[1].getBytes(Charset.forName("UTF-8"));
                this.columns.add(new Column(cf, cq));
            } else {
                final byte[] cf = column.getBytes(Charset.forName("UTF-8"));
                this.columns.add(new Column(cf, null));
            }
        }
    }

    @OnPrimaryNodeStateChange
    public void onPrimaryNodeChange(final PrimaryNodeState newState) {
        if (newState == PrimaryNodeState.ELECTED_PRIMARY_NODE) {
            electedPrimaryNode = true;
        }
    }

    @OnRemoved
    public void onRemoved(final ProcessContext context) {
        final DistributedMapCacheClient client = context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);
        clearState(client);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final String tableName = context.getProperty(TABLE_NAME).getValue();
        final String initialTimeRange = context.getProperty(INITIAL_TIMERANGE).getValue();
        final String filterExpression = context.getProperty(FILTER_EXPRESSION).getValue();
        final HBaseClientService hBaseClientService = context.getProperty(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class);
        final DistributedMapCacheClient client = context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);

        // if the table was changed then remove any previous state
        if (previousTable != null && !tableName.equals(previousTable)) {
            clearState(client);
            previousTable = tableName;
        }

        try {
            final Charset charset = Charset.forName(context.getProperty(CHARSET).getValue());
            final RowSerializer serializer = new JsonRowSerializer(charset);

            this.lastResult = getState(client);
            final long defaultMinTime = (initialTimeRange.equals(NONE.getValue()) ? 0L : System.currentTimeMillis());
            final long minTime = (lastResult == null ? defaultMinTime : lastResult.getTimestamp());

            final Map<String, Set<String>> cellsMatchingTimestamp = new HashMap<>();

            final ObjectHolder<Long> rowsPulledHolder = new ObjectHolder<>(0L);
            final ObjectHolder<Long> latestTimestampHolder = new ObjectHolder<>(minTime);


            hBaseClientService.scan(tableName, columns, filterExpression, minTime, new ResultHandler() {
                @Override
                public void handle(final byte[] rowKey, final ResultCell[] resultCells) {

                    final String rowKeyString = new String(rowKey, StandardCharsets.UTF_8);

                    // check if latest cell timestamp is equal to our cutoff.
                    // if any of the cells have a timestamp later than our cutoff, then we
                    // want the row. But if the cell with the latest timestamp is equal to
                    // our cutoff, then we want to check if that's one of the cells that
                    // we have already seen.
                    long latestCellTimestamp = 0L;
                    for (final ResultCell cell : resultCells) {
                        if (cell.getTimestamp() > latestCellTimestamp) {
                            latestCellTimestamp = cell.getTimestamp();
                        }
                    }

                    // we've already seen this.
                    if (latestCellTimestamp < minTime) {
                        getLogger().debug("latest cell timestamp for row {} is {}, which is earlier than the minimum time of {}",
                                new Object[] {rowKeyString, latestCellTimestamp, minTime});
                        return;
                    }

                    if (latestCellTimestamp == minTime) {
                        // latest cell timestamp is equal to our minimum time. Check if all cells that have
                        // that timestamp are in our list of previously seen cells.
                        boolean allSeen = true;
                        for (final ResultCell cell : resultCells) {
                            if (cell.getTimestamp() == latestCellTimestamp) {
                                if (lastResult == null || !lastResult.contains(cell)) {
                                    allSeen = false;
                                    break;
                                }
                            }
                        }

                        if (allSeen) {
                            // we have already seen all of the cells for this row. We do not want to
                            // include this cell in our output.
                            getLogger().debug("all cells for row {} have already been seen", new Object[] { rowKeyString });
                            return;
                        }
                    }

                    // If the latest timestamp of the cell is later than the latest timestamp we have already seen,
                    // we want to keep track of the cells that match this timestamp so that the next time we scan,
                    // we can ignore these cells.
                    if (latestCellTimestamp >= latestTimestampHolder.get()) {
                        // new timestamp, so clear all of the 'matching cells'
                        if (latestCellTimestamp > latestTimestampHolder.get()) {
                            latestTimestampHolder.set(latestCellTimestamp);
                            cellsMatchingTimestamp.clear();
                        }

                        for (final ResultCell cell : resultCells) {
                            final long ts = cell.getTimestamp();
                            if (ts == latestCellTimestamp) {
                                final byte[] rowValue = Arrays.copyOfRange(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength() + cell.getRowOffset());
                                final byte[] cellValue = Arrays.copyOfRange(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength() + cell.getValueOffset());

                                final String rowHash = new String(rowValue, StandardCharsets.UTF_8);
                                Set<String> cellHashes = cellsMatchingTimestamp.get(rowHash);
                                if (cellHashes == null) {
                                    cellHashes = new HashSet<>();
                                    cellsMatchingTimestamp.put(rowHash, cellHashes);
                                }
                                cellHashes.add(new String(cellValue, StandardCharsets.UTF_8));
                            }
                        }
                    }

                    // write the row to a new FlowFile.
                    FlowFile flowFile = session.create();
                    flowFile = session.write(flowFile, new OutputStreamCallback() {
                        @Override
                        public void process(final OutputStream out) throws IOException {
                            serializer.serialize(rowKey, resultCells, out);
                        }
                    });

                    final Map<String, String> attributes = new HashMap<>();
                    attributes.put("hbase.table", tableName);
                    attributes.put("mime.type", "application/json");
                    flowFile = session.putAllAttributes(flowFile, attributes);

                    session.getProvenanceReporter().receive(flowFile, "hbase://" + tableName + "/" + rowKeyString);
                    session.transfer(flowFile, REL_SUCCESS);
                    getLogger().debug("Received {} from HBase with row key {}", new Object[]{flowFile, rowKeyString});

                    // we could potentially have a huge number of rows. If we get to 500, go ahead and commit the
                    // session so that we can avoid buffering tons of FlowFiles without ever sending any out.
                    long rowsPulled = rowsPulledHolder.get();
                    rowsPulledHolder.set(++rowsPulled);

                    if (++rowsPulled % getBatchSize() == 0) {
                        session.commit();
                    }
                }
            });

            final ScanResult scanResults = new ScanResult(latestTimestampHolder.get(), cellsMatchingTimestamp);

            // Commit session before we replace the lastResult; if session commit fails, we want
            // to pull these records again.
            session.commit();
            if (lastResult == null || scanResults.getTimestamp() > lastResult.getTimestamp()) {
                lastResult = scanResults;
            } else if (scanResults.getTimestamp() == lastResult.getTimestamp()) {
                final Map<String, Set<String>> combinedResults = new HashMap<>(scanResults.getMatchingCells());

                // copy the results of result.getMatchingCells() to combinedResults.
                // do a deep copy because the Set may be modified below.
                for (final Map.Entry<String, Set<String>> entry : scanResults.getMatchingCells().entrySet()) {
                    combinedResults.put(entry.getKey(), new HashSet<>(entry.getValue()));
                }

                // combined the results from 'lastResult'
                for (final Map.Entry<String, Set<String>> entry : lastResult.getMatchingCells().entrySet()) {
                    final Set<String> existing = combinedResults.get(entry.getKey());
                    if (existing == null) {
                        combinedResults.put(entry.getKey(), new HashSet<>(entry.getValue()));
                    } else {
                        existing.addAll(entry.getValue());
                    }
                }
                final ScanResult scanResult = new ScanResult(scanResults.getTimestamp(), combinedResults);
                lastResult = scanResult;
            }

            // save state to local storage and to distributed cache
            persistState(client, lastResult);

        } catch (final IOException e) {
            getLogger().error("Failed to receive data from HBase due to {}", e);
            session.rollback();
        } finally {
            // if we failed, we want to yield so that we don't hammer hbase. If we succeed, then we have
            // pulled all of the records, so we want to wait a bit before hitting hbase again anyway.
            context.yield();
        }
    }

    // present for tests
    protected int getBatchSize() {
        return 500;
    }

    protected File getStateDir() {
        return new File("conf/state");
    }

    protected File getStateFile() {
        return new File(getStateDir(), "getHBase-" + getIdentifier());
    }

    protected String getKey() {
        return "getHBase-" + getIdentifier() + "-state";
    }

    protected List<Column> getColumns() {
        return columns;
    }

    private void persistState(final DistributedMapCacheClient client, final ScanResult scanResult) {
        final File stateDir = getStateDir();
        if (!stateDir.exists()) {
            stateDir.mkdirs();
        }

        final File file = getStateFile();
        try (final OutputStream fos = new FileOutputStream(file);
                final ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.writeObject(scanResult);
        } catch (final IOException ioe) {
            getLogger().warn("Unable to save state locally. If the node is restarted now, data may be duplicated. Failure is due to {}", ioe);
        }

        try {
            client.put(getKey(), scanResult, new StringSerDe(), new ObjectSerDe());
        } catch (final IOException ioe) {
            getLogger().warn("Unable to communicate with distributed cache server due to {}. Persisting state locally instead.", ioe);
        }
    }

    private void clearState(final DistributedMapCacheClient client) {
        final File localState = getStateFile();
        if (localState.exists()) {
            localState.delete();
        }

        try {
            client.remove(getKey(), new StringSerDe());
        } catch (IOException e) {
            getLogger().warn("Processor state was not cleared from distributed cache due to {}", new Object[]{e});
        }
    }

    private ScanResult getState(final DistributedMapCacheClient client) throws IOException {
        final StringSerDe stringSerDe = new StringSerDe();
        final ObjectSerDe objectSerDe = new ObjectSerDe();

        ScanResult scanResult = lastResult;
        // if we have no previous result, or we just became primary, pull from distributed cache
        if (scanResult == null || electedPrimaryNode) {
            final Object obj = client.get(getKey(), stringSerDe, objectSerDe);
            if (obj == null || !(obj instanceof ScanResult)) {
                scanResult = null;
            } else {
                scanResult = (ScanResult) obj;
                getLogger().debug("Retrieved state from the distributed cache, previous timestamp was {}" , new Object[] {scanResult.getTimestamp()});
            }

            // no requirement to pull an update from the distributed cache anymore.
            electedPrimaryNode = false;
        }

        // Check the persistence file. We want to use the latest timestamp that we have so that
        // we don't duplicate data.
        final File file = getStateFile();
        if (file.exists()) {
            try (final InputStream fis = new FileInputStream(file);
                 final ObjectInputStream ois = new ObjectInputStream(fis)) {

                final Object obj = ois.readObject();
                if (obj != null && (obj instanceof ScanResult)) {
                    final ScanResult localScanResult = (ScanResult) obj;
                    if (scanResult == null || localScanResult.getTimestamp() > scanResult.getTimestamp()) {
                        scanResult = localScanResult;
                        getLogger().debug("Using last timestamp from local state because it was newer than the distributed cache, or no value existed in the cache");

                        // Our local persistence file shows a later time than the Distributed service.
                        // Update the distributed service to match our local state.
                        try {
                            client.put(getKey(), localScanResult, stringSerDe, objectSerDe);
                        } catch (final IOException ioe) {
                            getLogger().warn("Local timestamp is {}, which is later than Distributed state but failed to update Distributed "
                                            + "state due to {}. If a new node performs GetHBase Listing, data duplication may occur",
                                    new Object[] {localScanResult.getTimestamp(), ioe});
                        }
                    }
                }
            } catch (final IOException | ClassNotFoundException ioe) {
                getLogger().warn("Failed to recover persisted state from {} due to {}. Assuming that state from distributed cache is correct.", new Object[]{file, ioe});
            }
        }

        return scanResult;
    }

    public static class ScanResult implements Serializable {

        private static final long serialVersionUID = 1L;

        private final long latestTimestamp;
        private final Map<String, Set<String>> matchingCellHashes;

        public ScanResult(final long timestamp, final Map<String, Set<String>> cellHashes) {
            latestTimestamp = timestamp;
            matchingCellHashes = cellHashes;
        }

        public long getTimestamp() {
            return latestTimestamp;
        }

        public Map<String, Set<String>> getMatchingCells() {
            return matchingCellHashes;
        }

        public boolean contains(final ResultCell cell) {
            if (cell.getTimestamp() != latestTimestamp) {
                return false;
            }

            final byte[] row = Arrays.copyOfRange(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength() + cell.getRowOffset());
            final String rowHash = new String(row, StandardCharsets.UTF_8);
            final Set<String> cellHashes = matchingCellHashes.get(rowHash);
            if (cellHashes == null) {
                return false;
            }

            final byte[] cellValue = Arrays.copyOfRange(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength() + cell.getValueOffset());
            final String cellHash = new String(cellValue, StandardCharsets.UTF_8);
            return cellHashes.contains(cellHash);
        }
    }

}
