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
package org.apache.nifi.processors.couchbase.utils;

public class CouchbaseAttributes {

    public static final String DEFAULT_BUCKET = "default";
    public static final String DEFAULT_SCOPE = "_default";
    public static final String DEFAULT_COLLECTION = "_default";

    public static final String BUCKET_ATTRIBUTE = "couchbase.bucket";
    public static final String BUCKET_ATTRIBUTE_DESC = "The bucket where the document is stored.";
    public static final String SCOPE_ATTRIBUTE = "couchbase.scope";
    public static final String SCOPE_ATTRIBUTE_DESC = "The scope where the document is stored.";
    public static final String COLLECTION_ATTRIBUTE = "couchbase.collection";
    public static final String COLLECTION_ATTRIBUTE_DESC = "The collection where the document is stored.";
    public static final String DOCUMENT_ID_ATTRIBUTE = "couchbase.document.id";
    public static final String DOCUMENT_ID_ATTRIBUTE_DESC = "Id of the document.";
    public static final String CAS_ATTRIBUTE = "couchbase.document.cas";
    public static final String CAS_ATTRIBUTE_DESC = "CAS of the document.";

}
