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

-- Cache version table: one row per authorization domain, incremented on every write by any cluster node.
-- Nodes poll this table to detect out-of-date in-memory caches and trigger a reload.

CREATE TABLE CACHE_VERSION (
    CACHE_DOMAIN VARCHAR(50) NOT NULL,
    VERSION      BIGINT      NOT NULL DEFAULT 0,
    CONSTRAINT PK__CACHE_VERSION PRIMARY KEY (CACHE_DOMAIN)
);

-- Distributed leader-election lock table.
-- Only one row exists (LOCK_KEY = 'LEADER'). The node that holds the lease
-- before EXPIRES_AT is the cluster leader.
CREATE TABLE CLUSTER_LEADER (
                                LOCK_KEY   VARCHAR(50)  NOT NULL,
                                NODE_ID    VARCHAR(100) NOT NULL,
                                EXPIRES_AT TIMESTAMP    NOT NULL,
                                CONSTRAINT PK__CLUSTER_LEADER PRIMARY KEY (LOCK_KEY)
);

-- Durable event log for cluster-wide hook delivery.
-- Any node can INSERT an event row; the leader node delivers it to
-- EventHookProviders and marks PROCESSED = TRUE.
-- Retention: processed rows older than 7 days are deleted by the leader.
CREATE TABLE REGISTRY_EVENT (
                                EVENT_ID   VARCHAR(50)  NOT NULL,
                                EVENT_TYPE VARCHAR(100) NOT NULL,
                                EVENT_DATA TEXT         NOT NULL,
                                CREATED_AT TIMESTAMP    NOT NULL,
                                PROCESSED  BOOLEAN      NOT NULL DEFAULT FALSE,
                                CONSTRAINT PK__REGISTRY_EVENT PRIMARY KEY (EVENT_ID)
);

-- Track how many delivery attempts have been made for each event.
-- When RETRY_COUNT reaches the application-level maximum the leader logs
-- a diagnostic WARN and marks the event PROCESSED so it stops blocking delivery.
ALTER TABLE REGISTRY_EVENT ADD COLUMN RETRY_COUNT INTEGER NOT NULL DEFAULT 0;

INSERT INTO CACHE_VERSION (CACHE_DOMAIN, VERSION) VALUES ('ACCESS_POLICIES', 0);
INSERT INTO CACHE_VERSION (CACHE_DOMAIN, VERSION) VALUES ('USER_GROUPS', 0);

