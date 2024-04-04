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
package org.apache.nifi.jms.processors.ioconcept.reader.record;

public class ProvenanceEventTemplates {

    public static final String PROVENANCE_EVENT_DETAILS_ON_RECORDSET_FAILURE = "Publish failed after %d successfully published records.";
    public static final String PROVENANCE_EVENT_DETAILS_ON_RECORDSET_RECOVER = "Successfully finished publishing previously failed records. Total record count: %d";
    public static final String PROVENANCE_EVENT_DETAILS_ON_RECORDSET_SUCCESS = "Successfully published all records. Total record count: %d";

}
