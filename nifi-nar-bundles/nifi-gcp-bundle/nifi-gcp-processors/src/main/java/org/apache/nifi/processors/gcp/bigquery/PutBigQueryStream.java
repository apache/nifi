package org.apache.nifi.processors.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import static org.apache.nifi.processors.gcp.bigquery.AbstractBigQueryProcessor.REL_FAILURE;
import static org.apache.nifi.processors.gcp.bigquery.AbstractBigQueryProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.gcp.bigquery.AbstractBigQueryProcessor.relationships;
import static org.apache.nifi.processors.gcp.storage.AbstractGCSProcessor.REL_SUCCESS;
import org.apache.nifi.processors.gcp.storage.DeleteGCSObject;
import org.apache.nifi.processors.gcp.storage.FetchGCSObject;
import org.apache.nifi.processors.gcp.storage.ListGCSBucket;

/**
 *
 * @author Mikhail Sosonkin (Synack Inc, Synack.com)
 */

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"google", "google cloud", "bq", "bigquery"})
@CapabilityDescription("Streams flow files to a Google BigQuery table.")

@WritesAttributes({
        @WritesAttribute(attribute = BigQueryAttributes.TABLE_NAME_ATTR, description = BigQueryAttributes.TABLE_NAME_DESC)
})

public class PutBigQueryStream extends AbstractBigQueryProcessor {
    public static final Relationship REL_ROW_TOO_BIG =
        new Relationship.Builder().name("row_too_big")
                .description("FlowFiles are routed to this relationship if the row size is too big.")
                .build();
        
    public static final PropertyDescriptor DATASET = new PropertyDescriptor
        .Builder().name(BigQueryAttributes.DATASET_ATTR)
        .displayName("Dataset")
        .description(BigQueryAttributes.DATASET_DESC)
        .required(true)
        .defaultValue("${" + BigQueryAttributes.DATASET_ATTR + "}")
        .expressionLanguageSupported(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    
    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor
        .Builder().name(BigQueryAttributes.TABLE_NAME_ATTR)
        .displayName("Table Name")
        .description(BigQueryAttributes.TABLE_NAME_DESC)
        .required(true)
        .defaultValue("${" + BigQueryAttributes.TABLE_NAME_ATTR + "}")
        .expressionLanguageSupported(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    
    public static final PropertyDescriptor TABLE_SCHEMA = new PropertyDescriptor
        .Builder().name(BigQueryAttributes.TABLE_SCHEMA_ATTR)
        .displayName("Table Schema")
        .description(BigQueryAttributes.TABLE_SCHEMA_DESC)
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
        
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor
        .Builder().name(BigQueryAttributes.BATCH_SIZE_ATTR)
        .displayName("Max batch size")
        .description(BigQueryAttributes.BATCH_SIZE_DESC)
        .required(true)
        .defaultValue("100")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();
    
    public static final PropertyDescriptor MAX_ROW_SIZE = new PropertyDescriptor
        .Builder().name(BigQueryAttributes.MAX_ROW_SIZE_ATTR)
        .displayName("Max row size")
        .description(BigQueryAttributes.MAX_ROW_SIZE_DESC)
        .required(true)
        .defaultValue("1 MB")
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .build();
    
    public static final PropertyDescriptor CREATE_DISPOSITION = new PropertyDescriptor.Builder()
        .name(BigQueryAttributes.CREATE_DISPOSITION_ATTR)
        .displayName("Create Disposition")
        .description(BigQueryAttributes.CREATE_DISPOSITION_DESC)
        .required(true)
        .allowableValues(JobInfo.CreateDisposition.CREATE_IF_NEEDED.name(), JobInfo.CreateDisposition.CREATE_NEVER.name())
        .defaultValue(JobInfo.CreateDisposition.CREATE_IF_NEEDED.name())
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    
    public static final PropertyDescriptor TABLE_CACHE_RESET = new PropertyDescriptor.Builder()
        .name(BigQueryAttributes.TABLE_CACHE_RESET_ATTR)
        .displayName("Table Cache Max Age")
        .description(BigQueryAttributes.TABLE_CACHE_RESET_DESC)
        .required(true)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("1 hours")
        .build();

    private final static Type gsonParseType = new TypeToken<Map<String, Object>>(){}.getType();
    
    public final static long MAX_REQ_SIZE = 9485760L;
    
    private Schema schemaCache = null;
    private Set<TableId> tableCache = Collections.synchronizedSet(new HashSet());
    private long lastTableCacheCheck = 0L;
    private long tableCacheAge = 1 * 60 * 60 * 1000L;
    
    public PutBigQueryStream() {
        
    }
    
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .addAll(super.getSupportedPropertyDescriptors())
                .add(DATASET)
                .add(TABLE_NAME)
                .add(TABLE_SCHEMA)
                .add(BATCH_SIZE)
                .add(MAX_ROW_SIZE)
                .add(CREATE_DISPOSITION)
                .add(TABLE_CACHE_RESET)
                .build();
    }
    
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(true)
                .dynamic(true)
                .build();
    }
    
    @Override
    public Set<Relationship> getRelationships() {
        return Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE, REL_ROW_TOO_BIG)));
    }
    
    private Table createTable(BigQuery bq, TableId tableId, Schema schema) {
        TableInfo tableInfo = TableInfo.newBuilder(tableId, StandardTableDefinition.of(schema)).build();

        Table table = bq.create(tableInfo);
        
        if(table == null || !table.exists()) {
            throw new ProcessException("Unable to create table: " + table + " info: " + tableInfo);
        }
        
        getLogger().info("Created Table: {}", new Object[]{tableId});
        
        return table;
    }
    
    private void validateTables(BigQuery bq, Set<TableId> tables, Schema schema, boolean create) throws ProcessException {
        if(System.currentTimeMillis() - lastTableCacheCheck > tableCacheAge) {
            lastTableCacheCheck = System.currentTimeMillis();
            tableCache.clear();
            
            getLogger().info("Table Cache cleared");
        }
        
        for(TableId tid : tables) {
            if(tableCache.contains(tid)) {
                continue;
            }
            
            Table table = bq.getTable(tid);
            
            if(table == null || !table.exists()) {
                if(create) {
                    createTable(bq, tid, schema);
                } else {
                    throw new ProcessException("Table doesn't exist and create disposition does not allow creation: " + tid);
                }
            }
            
            tableCache.add(tid);
        }
    }
    
    @OnStopped
    public void onStopped(ProcessContext context) {
        schemaCache = null;
    }
    
    @OnScheduled
    public void initSchema(ProcessContext context) {
        if(schemaCache == null) {
            String schemaStr = context.getProperty(TABLE_SCHEMA).getValue();
            schemaCache = BqUtils.schemaFromString(schemaStr);

            lastTableCacheCheck = System.currentTimeMillis();
            tableCache.clear();

            tableCacheAge = context.getProperty(TABLE_CACHE_RESET).asTimePeriod(TimeUnit.MILLISECONDS);

            getLogger().info("Enabled StreamIntoBigQuery");
        }
    }
    
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        List<FlowFile> flows = session.get(context.getProperty(BATCH_SIZE).asInteger());
        if (flows == null || flows.size() == 0) {
            return;
        }
        
        final Gson gson = new Gson();
        final long startNanos = System.nanoTime();
        final long max_row_size = context.getProperty(MAX_ROW_SIZE).asDataSize(DataUnit.B).longValue();

        Map<TableId, List<FlowFile>> binned = new HashMap();
        long totalSize = 0;
        for(final FlowFile ff : flows) {
            if(totalSize + ff.getSize() > MAX_REQ_SIZE) {
                // total message size would be too large.
                //  limited at 10 MB, giving a buffer of 1MB
                break;
            } else if(ff.getSize() > max_row_size) {
                // can't stream such large entries
                session.transfer(ff, REL_ROW_TOO_BIG);
            }
            
            final String projectId = context.getProperty(PROJECT_ID).getValue();
            final String dataset_str = context.getProperty(DATASET).evaluateAttributeExpressions(ff).getValue();
            final String tablename_str = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(ff).getValue();
            
            // put flows into table name bins
            final TableId tableId = TableId.of(projectId, dataset_str, tablename_str);
            
            List<FlowFile> tableFlows = binned.get(tableId);
            if(tableFlows == null) {
                tableFlows = new ArrayList();
                binned.put(tableId, tableFlows);
            }
            
            tableFlows.add(ff);
            totalSize += ff.getSize();
        }
        
        if(binned.isEmpty()) {
            return;
        }
        
        final BigQuery bq = getCloudService();
        
        if(bq == null) {
            throw new ProcessException("Unable to connect to BigQuery Service");
        }
        
        boolean createTable = context.getProperty(CREATE_DISPOSITION).getValue().equals(JobInfo.CreateDisposition.CREATE_IF_NEEDED.name());
        validateTables(bq, binned.keySet(), schemaCache, createTable);
        
        for(Entry<TableId, List<FlowFile>> binnedFlows : binned.entrySet()) {
            List<FlowFile> bFlows = binnedFlows.getValue();
            InsertAllRequest.Builder insertBuilder = InsertAllRequest.newBuilder(binnedFlows.getKey());

            for(final FlowFile ff : bFlows) {
                try(InputStreamReader dataReader = new InputStreamReader(session.read(ff))) {
                    Map<String, Object> rowContent = gson.fromJson(dataReader, gsonParseType);
                   
                    insertBuilder = insertBuilder.addRow(rowContent);
                } catch (IOException ioe) {
                    throw new ProcessException(ioe);
                }
            }


            // do the actual insertion.
            InsertAllResponse response = bq.insertAll(insertBuilder.build());

            Map<Long, List<BigQueryError>> insertErrors = response.getInsertErrors();
            long i = 0;
            for(FlowFile ff : bFlows) {
                if(insertErrors.containsKey(i)) {
                    List<BigQueryError> errors = insertErrors.get(i);
                    
                    if(errors.size() > 0) {
                        BigQueryError err = insertErrors.get(i).get(0);
                        final Map<String, String> attributes = new HashMap<>();
                        
                        attributes.put(BigQueryAttributes.JOB_ERROR_MSG_ATTR, err.getMessage());
                        attributes.put(BigQueryAttributes.JOB_ERROR_REASON_ATTR, err.getReason());
                        attributes.put(BigQueryAttributes.JOB_ERROR_LOCATION_ATTR, err.getLocation());
                        
                        if (!attributes.isEmpty()) {
                            ff = session.putAllAttributes(ff, attributes);
                        }
                    }
                 
                    ff = session.penalize(ff);
                    session.transfer(ff, REL_FAILURE);
                } else {
                    session.transfer(ff, REL_SUCCESS);
                    
                    final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                }

                ++i;
            }
        }
    }
    
}
