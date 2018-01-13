package org.apache.nifi.processors.gcp.bigquery;

/**
 * Attributes associated with the BigQuery processors
 */
public class BigQueryAttributes {
    private BigQueryAttributes() {}

    public static final String DATASET_ATTR = "bq.dataset";
    public static final String DATASET_DESC = "BigQuery dataset";
    
    public static final String TABLE_NAME_ATTR = "bq.table.name";
    public static final String TABLE_NAME_DESC = "BigQuery table name";

    public static final String TABLE_SCHEMA_ATTR = "bq.table.schema";
    public static final String TABLE_SCHEMA_DESC = "BigQuery table name";
            
    public static final String CREATE_DISPOSITION_ATTR = "bq.load.create_disposition";
    public static final String CREATE_DISPOSITION_DESC = "Options for table creation";
    
    public static final String JOB_ERROR_MSG_ATTR = "bq.error.message";
    public static final String JOB_ERROR_MSG_DESC = "Load job error message";
    
    public static final String JOB_ERROR_REASON_ATTR = "bq.error.reason";
    public static final String JOB_ERROR_REASON_DESC = "Load jon error reason";
    
    public static final String JOB_ERROR_LOCATION_ATTR = "bq.error.location";
    public static final String JOB_ERROR_LOCATION_DESC = "Load jon error location";
    
    // Stream Attributes
    public static final String BATCH_SIZE_ATTR = "bq.batch.size";
    public static final String BATCH_SIZE_DESC = "BigQuery number of rows to insert at a time";
    
    public static final String MAX_ROW_SIZE_ATTR = "bq.row.size";
    public static final String MAX_ROW_SIZE_DESC = "BigQuery has a limit (1MB) on max size of a row, for streaming";

    public static final String TABLE_CACHE_RESET_ATTR = "bq.cache.reset";
    public static final String TABLE_CACHE_RESET_DESC = "How often to reset table info cache";
    
    // Batch Attributes
    public static final String SOURCE_FILE_ATTR = "bq.load.file";
    public static final String SOURCE_FILE_DESC = "URL of file to load (gs://[bucket]/[key]";
    
    public static final String SOURCE_TYPE_ATTR = "bq.load.type";
    public static final String SOURCE_TYPE_DESC = "Data type of the file to be loaded";
    
    public static final String IGNORE_UNKNOWN_ATTR = "bq.load.ignore_unknown";
    public static final String IGNORE_UNKNOWN_DESC = "Ignore fields not in table schema";

    public static final String WRITE_DISPOSITION_ATTR = "bq.load.write_disposition";
    public static final String WRITE_DISPOSITION_DESC = "Options for writing to table";
    
    public static final String MAX_BADRECORDS_ATTR = "bq.load.max_badrecords";
    public static final String MAX_BADRECORDS_DESC = "Number of erroneous records to ignore before generating an error";
    
    public static final String JOB_CREATE_TIME_ATTR = "bq.job.stat.creation_time";
    public static final String JOB_CREATE_TIME_DESC = "Time load job creation";
    
    public static final String JOB_END_TIME_ATTR = "bq.job.stat.end_time";
    public static final String JOB_END_TIME_DESC = "Time load job ended";
    
    public static final String JOB_START_TIME_ATTR = "bq.job.stat.start_time";
    public static final String JOB_START_TIME_DESC = "Time load job started";
    
    public static final String JOB_LINK_ATTR = "bq.job.link";
    public static final String JOB_LINK_DESC = "API Link to load job";
}
