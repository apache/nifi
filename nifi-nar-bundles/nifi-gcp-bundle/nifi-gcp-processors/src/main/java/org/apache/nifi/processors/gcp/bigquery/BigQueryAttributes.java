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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors;

/**
 * Attributes associated with the BigQuery processors
 */
public class BigQueryAttributes {
    private BigQueryAttributes() {}

    public static final PropertyDescriptor SERVICE_ACCOUNT_JSON_FILE = CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON_FILE;

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
    public static final String JOB_ERROR_REASON_DESC = "Load job error reason";

    public static final String JOB_ERROR_LOCATION_ATTR = "bq.error.location";
    public static final String JOB_ERROR_LOCATION_DESC = "Load job error location";

    public static final String JOB_READ_TIMEOUT_ATTR = "bq.readtimeout";
    public static final String JOB_READ_TIMEOUT_DESC = "Load Job Time Out";


    // Batch Attributes
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