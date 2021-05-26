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

CREATE TABLE BUCKET (
    ID VARCHAR2(50) NOT NULL PRIMARY KEY,
    NAME VARCHAR2(200) NOT NULL UNIQUE,
    DESCRIPTION VARCHAR(4096),
    CREATED TIMESTAMP NOT NULL
);

CREATE TABLE BUCKET_ITEM (
    ID VARCHAR2(50) NOT NULL PRIMARY KEY,
    NAME VARCHAR2(200) NOT NULL UNIQUE,
    DESCRIPTION VARCHAR(4096),
    CREATED TIMESTAMP NOT NULL,
    MODIFIED TIMESTAMP NOT NULL,
    ITEM_TYPE VARCHAR(50) NOT NULL,
    BUCKET_ID VARCHAR2(50) NOT NULL,
    FOREIGN KEY (BUCKET_ID) REFERENCES BUCKET(ID)
);

CREATE TABLE FLOW (
    ID VARCHAR2(50) NOT NULL PRIMARY KEY,
    FOREIGN KEY (ID) REFERENCES BUCKET_ITEM(ID)
);

CREATE TABLE FLOW_SNAPSHOT (
    FLOW_ID VARCHAR2(50) NOT NULL,
    VERSION INT NOT NULL,
    CREATED TIMESTAMP NOT NULL,
    CREATED_BY VARCHAR2(200) NOT NULL,
    COMMENTS VARCHAR(4096),
    PRIMARY KEY (FLOW_ID, VERSION),
    FOREIGN KEY (FLOW_ID) REFERENCES FLOW(ID)
);

CREATE TABLE SIGNING_KEY (
    ID VARCHAR2(50) NOT NULL,
    TENANT_IDENTITY VARCHAR2(50) NOT NULL UNIQUE,
    KEY_VALUE VARCHAR2(50) NOT NULL,
    PRIMARY KEY (ID)
);