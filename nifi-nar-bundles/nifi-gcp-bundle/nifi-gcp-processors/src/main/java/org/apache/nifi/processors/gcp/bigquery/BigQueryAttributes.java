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

package org.apache.nifi.processors.gcp.bigquery;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors;

import com.google.cloud.bigquery.JobInfo;

/**
 * Attributes associated with the BigQuery processors
 */
public class BigQueryAttributes {
    private BigQueryAttributes() {}

    public static final PropertyDescriptor SERVICE_ACCOUNT_JSON_FILE = CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON_FILE;

    // Properties
    public static final String SOURCE_TYPE_ATTR = "bq.load.type";
    public static final String SOURCE_TYPE_DESC = "Data type of the file to be loaded. Possible values: AVRO, "
            + "NEWLINE_DELIMITED_JSON, CSV.";

    public static final String IGNORE_UNKNOWN_ATTR = "bq.load.ignore_unknown";
    public static final String IGNORE_UNKNOWN_DESC = "Sets whether BigQuery should allow extra values that are not represented "
            + "in the table schema. If true, the extra values are ignored. If false, records with extra columns are treated as "
            + "bad records, and if there are too many bad records, an invalid error is returned in the job result. By default "
            + "unknown values are not allowed.";

    public static final String WRITE_DISPOSITION_ATTR = "bq.load.write_disposition";
    public static final String WRITE_DISPOSITION_DESC = "Sets the action that should occur if the destination table already exists.";

    public static final String MAX_BADRECORDS_ATTR = "bq.load.max_badrecords";
    public static final String MAX_BADRECORDS_DESC = "Sets the maximum number of bad records that BigQuery can ignore when running "
            + "the job. If the number of bad records exceeds this value, an invalid error is returned in the job result. By default "
            + "no bad record is ignored.";

    public static final String DATASET_ATTR = "bq.dataset";
    public static final String DATASET_DESC = "BigQuery dataset name (Note - The dataset must exist in GCP)";

    public static final String TABLE_NAME_ATTR = "bq.table.name";
    public static final String TABLE_NAME_DESC = "BigQuery table name";

    public static final String TABLE_SCHEMA_ATTR = "bq.table.schema";
    public static final String TABLE_SCHEMA_DESC = "BigQuery schema in JSON format";

    public static final String CREATE_DISPOSITION_ATTR = "bq.load.create_disposition";
    public static final String CREATE_DISPOSITION_DESC = "Sets whether the job is allowed to create new tables";

    public static final String JOB_READ_TIMEOUT_ATTR = "bq.readtimeout";
    public static final String JOB_READ_TIMEOUT_DESC = "Load Job Time Out";

    public static final String CSV_ALLOW_JAGGED_ROWS_ATTR = "bq.csv.allow.jagged.rows";
    public static final String CSV_ALLOW_JAGGED_ROWS_DESC = "Set whether BigQuery should accept rows that are missing "
            + "trailing optional columns. If true, BigQuery treats missing trailing columns as null values. If false, "
            + "records with missing trailing columns are treated as bad records, and if there are too many bad records, "
            + "an invalid error is returned in the job result. By default, rows with missing trailing columns are "
            + "considered bad records.";

    public static final String CSV_ALLOW_QUOTED_NEW_LINES_ATTR = "bq.csv.allow.quoted.new.lines";
    public static final String CSV_ALLOW_QUOTED_NEW_LINES_DESC = "Sets whether BigQuery should allow quoted data sections "
            + "that contain newline characters in a CSV file. By default quoted newline are not allowed.";

    public static final String CSV_CHARSET_ATTR = "bq.csv.charset";
    public static final String CSV_CHARSET_DESC = "Sets the character encoding of the data.";

    public static final String CSV_FIELD_DELIMITER_ATTR = "bq.csv.delimiter";
    public static final String CSV_FIELD_DELIMITER_DESC = "Sets the separator for fields in a CSV file. BigQuery converts "
            + "the string to ISO-8859-1 encoding, and then uses the first byte of the encoded string to split the data in its "
            + "raw, binary state. BigQuery also supports the escape sequence \"\t\" to specify a tab separator. The default "
            + "value is a comma (',').";

    public static final String CSV_QUOTE_ATTR = "bq.csv.quote";
    public static final String CSV_QUOTE_DESC = "Sets the value that is used to quote data sections in a CSV file. BigQuery "
            + "converts the string to ISO-8859-1 encoding, and then uses the first byte of the encoded string to split the "
            + "data in its raw, binary state. The default value is a double-quote ('\"'). If your data does not contain quoted "
            + "sections, set the property value to an empty string. If your data contains quoted newline characters, you must "
            + "also set the Allow Quoted New Lines property to true.";

    public static final String CSV_SKIP_LEADING_ROWS_ATTR = "bq.csv.skip.leading.rows";
    public static final String CSV_SKIP_LEADING_ROWS_DESC = "Sets the number of rows at the top of a CSV file that BigQuery "
            + "will skip when reading the data. The default value is 0. This property is useful if you have header rows in the "
            + "file that should be skipped.";



    // Batch Attributes
    public static final String JOB_CREATE_TIME_ATTR = "bq.job.stat.creation_time";
    public static final String JOB_CREATE_TIME_DESC = "Time load job creation";

    public static final String JOB_END_TIME_ATTR = "bq.job.stat.end_time";
    public static final String JOB_END_TIME_DESC = "Time load job ended";

    public static final String JOB_START_TIME_ATTR = "bq.job.stat.start_time";
    public static final String JOB_START_TIME_DESC = "Time load job started";

    public static final String JOB_LINK_ATTR = "bq.job.link";
    public static final String JOB_LINK_DESC = "API Link to load job";

    public static final String JOB_NB_RECORDS_ATTR = "bq.records.count";
    public static final String JOB_NB_RECORDS_DESC = "Number of records successfully inserted";

    public static final String JOB_ERROR_MSG_ATTR = "bq.error.message";
    public static final String JOB_ERROR_MSG_DESC = "Load job error message";

    public static final String JOB_ERROR_REASON_ATTR = "bq.error.reason";
    public static final String JOB_ERROR_REASON_DESC = "Load job error reason";

    public static final String JOB_ERROR_LOCATION_ATTR = "bq.error.location";
    public static final String JOB_ERROR_LOCATION_DESC = "Load job error location";


    // Allowable values
    public static final AllowableValue CREATE_IF_NEEDED = new AllowableValue(JobInfo.CreateDisposition.CREATE_IF_NEEDED.name(),
            JobInfo.CreateDisposition.CREATE_IF_NEEDED.name(), "Configures the job to create the table if it does not exist.");
    public static final AllowableValue CREATE_NEVER = new AllowableValue(JobInfo.CreateDisposition.CREATE_NEVER.name(),
            JobInfo.CreateDisposition.CREATE_NEVER.name(), "Configures the job to fail with a not-found error if the table does not exist.");

    public static final AllowableValue WRITE_EMPTY = new AllowableValue(JobInfo.WriteDisposition.WRITE_EMPTY.name(),
            JobInfo.WriteDisposition.WRITE_EMPTY.name(), "Configures the job to fail with a duplicate error if the table already exists.");
    public static final AllowableValue WRITE_APPEND = new AllowableValue(JobInfo.WriteDisposition.WRITE_APPEND.name(),
            JobInfo.WriteDisposition.WRITE_APPEND.name(), "Configures the job to append data to the table if it already exists.");
    public static final AllowableValue WRITE_TRUNCATE = new AllowableValue(JobInfo.WriteDisposition.WRITE_TRUNCATE.name(),
            JobInfo.WriteDisposition.WRITE_TRUNCATE.name(), "Configures the job to overwrite the table data if table already exists.");

}