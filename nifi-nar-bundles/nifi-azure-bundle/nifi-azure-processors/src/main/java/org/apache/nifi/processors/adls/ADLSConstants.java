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
package org.apache.nifi.processors.adls;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;


public class ADLSConstants {

    public static final int CHUNK_SIZE_IN_BYTES = 4000000;

    public static final PropertyDescriptor PATH_NAME = new PropertyDescriptor.Builder()
            .name("Path")
            .description("Path for file in Azure Data Lake, e.g. /adlshome/")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RETRY_ON_FAIL = new PropertyDescriptor.Builder()
            .name("Overwrite policy")
            .description("How many times to retry if read fails per chunk read, defaults to 3")
            .required(true)
            //TODO add Integer validator
            .defaultValue("3")
            .build();

    public static final PropertyDescriptor ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .name("Account FQDN")
            .description("Azure account fully qualified domain name eg: accountname.azuredatalakestore.net")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLIENT_ID = new PropertyDescriptor.Builder()
            .name("Client ID")
            .description("Azure client ID")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLIENT_SECRET = new PropertyDescriptor.Builder()
            .name("Client secret")
            .description("Azure client secret")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor AUTH_TOKEN_ENDPOINT = new PropertyDescriptor.Builder()
            .name("Auth token endpoint")
            .description("Azure client secret")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully transferred to ADL")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to transfer file to ADL")
            .build();

    public static final String FILE_NAME_ATTRIBUTE = "filename";
    public static final String ADLS_FILE_PATH_ATTRIBUTE = "absolute.adls.path";

    public static final String ERR_ACL_ENTRY = "Invalid ACL entry";
    public static final String ERR_FLOWFILE_CORE_ATTR_FILENAME = "Filename missing in flowfile core attribute";

}
