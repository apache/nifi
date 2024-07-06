/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.gcp.bigquery;

/**
 * Attributes associated with the BigQuery processors
 */
public class BigQueryAttributes {
    private BigQueryAttributes() {
    }

    // Properties
    public static final String IGNORE_UNKNOWN_ATTR = "bq.load.ignore_unknown";
    public static final String IGNORE_UNKNOWN_DESC = "Sets whether BigQuery should allow extra values that are not represented "
            + "in the table schema. If true, the extra values are ignored. If false, records with extra columns are treated as "
            + "bad records, and if there are too many bad records, an invalid error is returned in the job result. By default "
            + "unknown values are not allowed.";

    public static final String DATASET_ATTR = "bq.dataset";
    public static final String DATASET_DESC = "BigQuery dataset name (Note - The dataset must exist in GCP)";

    public static final String TABLE_NAME_ATTR = "bq.table.name";
    public static final String TABLE_NAME_DESC = "BigQuery table name";

    public static final String RECORD_READER_ATTR = "bq.record.reader";
    public static final String RECORD_READER_DESC = "Specifies the Controller Service to use for parsing incoming data.";

    public static final String SKIP_INVALID_ROWS_ATTR = "bq.skip.invalid.rows";
    public static final String SKIP_INVALID_ROWS_DESC = "Sets whether to insert all valid rows of a request, even if invalid "
            + "rows exist. If not set the entire insert request will fail if it contains an invalid row.";

    // Batch Attributes
    public static final String JOB_NB_RECORDS_ATTR = "bq.records.count";
    public static final String JOB_NB_RECORDS_DESC = "Number of records successfully inserted";
}