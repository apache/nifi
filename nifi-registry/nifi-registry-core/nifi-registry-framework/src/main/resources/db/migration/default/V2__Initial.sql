-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- The NAME column has a max size of 768 because this is the largest size that MySQL allows when using a unique constraint.
CREATE TABLE BUCKET (
    ID VARCHAR(50) NOT NULL,
    NAME VARCHAR(1000) NOT NULL,
    DESCRIPTION TEXT,
    CREATED TIMESTAMP NOT NULL,
    CONSTRAINT PK__BUCKET_ID PRIMARY KEY (ID),
    CONSTRAINT UNIQUE__BUCKET_NAME UNIQUE (NAME)
);

CREATE TABLE BUCKET_ITEM (
    ID VARCHAR(50) NOT NULL,
    NAME VARCHAR(1000) NOT NULL,
    DESCRIPTION TEXT,
    CREATED TIMESTAMP NOT NULL,
    MODIFIED TIMESTAMP NOT NULL,
    ITEM_TYPE VARCHAR(50) NOT NULL,
    BUCKET_ID VARCHAR(50) NOT NULL,
    CONSTRAINT PK__BUCKET_ITEM_ID PRIMARY KEY (ID),
    CONSTRAINT FK__BUCKET_ITEM_BUCKET_ID FOREIGN KEY (BUCKET_ID) REFERENCES BUCKET(ID)
);

CREATE TABLE FLOW (
    ID VARCHAR(50) NOT NULL,
    CONSTRAINT PK__FLOW_ID PRIMARY KEY (ID),
    CONSTRAINT FK__FLOW_BUCKET_ITEM_ID FOREIGN KEY (ID) REFERENCES BUCKET_ITEM(ID)
);

CREATE TABLE FLOW_SNAPSHOT (
    FLOW_ID VARCHAR(50) NOT NULL,
    VERSION INT NOT NULL,
    CREATED TIMESTAMP NOT NULL,
    CREATED_BY VARCHAR(4096) NOT NULL,
    COMMENTS TEXT,
    CONSTRAINT PK__FLOW_SNAPSHOT_FLOW_ID_AND_VERSION PRIMARY KEY (FLOW_ID, VERSION),
    CONSTRAINT FK__FLOW_SNAPSHOT_FLOW_ID FOREIGN KEY (FLOW_ID) REFERENCES FLOW(ID)
);

CREATE TABLE SIGNING_KEY (
    ID VARCHAR(50) NOT NULL,
    TENANT_IDENTITY VARCHAR(4096) NOT NULL,
    KEY_VALUE VARCHAR(50) NOT NULL,
    CONSTRAINT PK__SIGNING_KEY_ID PRIMARY KEY (ID),
    CONSTRAINT UNIQUE__SIGNING_KEY_TENANT_IDENTITY UNIQUE (TENANT_IDENTITY)
);