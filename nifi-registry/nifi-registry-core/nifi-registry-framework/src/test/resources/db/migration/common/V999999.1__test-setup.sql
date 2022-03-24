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

-- test data for buckets

insert into BUCKET (id, name, description, created)
  values ('1', 'Bucket 1', 'This is test bucket 1', DATE'2017-09-11');

insert into BUCKET (id, name, description, created)
  values ('2', 'Bucket 2', 'This is test bucket 2', DATE'2017-09-12');

insert into BUCKET (id, name, description, created)
  values ('3', 'Bucket 3', 'This is test bucket 3', DATE'2017-09-13');

insert into BUCKET (id, name, description, created)
  values ('4', 'Bucket 4', 'This is test bucket 4', DATE'2017-09-14');

insert into BUCKET (id, name, description, created)
  values ('5', 'Bucket 5', 'This is test bucket 5', DATE'2017-09-15');

insert into BUCKET (id, name, description, created)
  values ('6', 'Bucket 6', 'This is test bucket 6', DATE'2017-09-16');


-- test data for flows

insert into BUCKET_ITEM (id, name, description, created, modified, item_type, bucket_id)
  values ('1', 'Flow 1', 'This is flow 1 bucket 1', DATE'2017-09-11', DATE'2017-09-11', 'FLOW', '1');

insert into FLOW (id) values ('1');

insert into BUCKET_ITEM (id, name, description, created, modified, item_type, bucket_id)
  values ('2', 'Flow 2', 'This is flow 2 bucket 1', DATE'2017-09-11', DATE'2017-09-11', 'FLOW', '1');

insert into FLOW (id) values ('2');

insert into BUCKET_ITEM (id, name, description, created, modified, item_type, bucket_id)
  values ('3', 'Flow 1', 'This is flow 1 bucket 2', DATE'2017-09-11', DATE'2017-09-11', 'FLOW', '2');

insert into FLOW (id) values ('3');


-- test data for flow snapshots

insert into FLOW_SNAPSHOT (flow_id, version, created, created_by, comments)
  values ('1', 1, DATE'2017-09-11', 'user1', 'This is flow 1 snapshot 1');

insert into FLOW_SNAPSHOT (flow_id, version, created, created_by, comments)
  values ('1', 2, DATE'2017-09-12', 'user1', 'This is flow 1 snapshot 2');

insert into FLOW_SNAPSHOT (flow_id, version, created, created_by, comments)
  values ('1', 3, DATE'2017-09-11', 'user1', 'This is flow 1 snapshot 3');


-- test data for signing keys

insert into SIGNING_KEY (id, tenant_identity, key_value)
  values ('1', 'unit_test_tenant_identity', '0123456789abcdef');

-- test data for extension bundles

-- processors bundle, depends on service api bundle
insert into BUCKET_ITEM (
  id,
  name,
  description,
  created,
  modified,
  item_type,
  bucket_id
) values (
  'eb1',
  'nifi-example-processors-nar',
  'Example processors bundle',
  DATE'2018-11-02',
  DATE'2018-11-02',
  'BUNDLE',
  '3'
);

insert into BUNDLE (
  id,
  bucket_id,
  bundle_type,
  group_id,
  artifact_id
) values (
  'eb1',
  '3',
  'NIFI_NAR',
  'org.apache.nifi',
  'nifi-example-processors-nar'
);

insert into BUNDLE_VERSION (
  id,
  bundle_id,
  version,
  created,
  created_by,
  description,
  sha_256_hex,
  sha_256_supplied,
  content_size
) values (
  'eb1-v1',
  'eb1',
  '1.0.0',
  DATE'2018-11-02',
  'user1',
  'First version of eb1',
  '123456789',
  '1',
  1024
);

insert into BUNDLE_VERSION_DEPENDENCY (
  id,
  bundle_version_id,
  group_id,
  artifact_id,
  version
) values (
  'eb1-v1-dep1',
  'eb1-v1',
  'org.apache.nifi',
  'nifi-example-service-api-nar',
  '2.0.0'
);

-- service impl bundle, depends on service api bundle
insert into BUCKET_ITEM (
  id,
  name,
  description,
  created,
  modified,
  item_type,
  bucket_id
) values (
  'eb2',
  'nifi-example-services-nar',
  'Example services bundle',
  DATE'2018-11-03',
  DATE'2018-11-03',
  'BUNDLE',
  '3'
);

insert into BUNDLE (
  id,
  bucket_id,
  bundle_type,
  group_id,
  artifact_id
) values (
  'eb2',
  '3',
  'NIFI_NAR',
  'com.foo',
  'nifi-example-services-nar'
);

insert into BUNDLE_VERSION (
  id,
  bundle_id,
  version,
  created,
  created_by,
  description,
  sha_256_hex,
  sha_256_supplied,
  content_size
) values (
  'eb2-v1',
  'eb2',
  '1.0.0',
  DATE'2018-11-03',
  'user1',
  'First version of eb2',
  '123456789',
  '1',
  1024
);

insert into BUNDLE_VERSION_DEPENDENCY (
  id,
  bundle_version_id,
  group_id,
  artifact_id,
  version
) values (
  'eb2-v1-dep1',
  'eb2-v1',
  'org.apache.nifi',
  'nifi-example-service-api-nar',
  '2.0.0'
);

-- service api bundle
insert into BUCKET_ITEM (
  id,
  name,
  description,
  created,
  modified,
  item_type,
  bucket_id
) values (
  'eb3',
  'nifi-example-service-api-nar',
  'Example service API bundle',
  DATE'2018-11-04',
  DATE'2017-11-04',
  'BUNDLE',
  '3'
);

insert into BUNDLE (
  id,
  bucket_id,
  bundle_type,
  group_id,
  artifact_id
) values (
  'eb3',
  '3',
  'NIFI_NAR',
  'org.apache.nifi',
  'nifi-example-service-api-nar'
);

insert into BUNDLE_VERSION (
  id,
  bundle_id,
  version,
  created,
  created_by,
  description,
  sha_256_hex,
  sha_256_supplied,
  content_size
) values (
  'eb3-v1',
  'eb3',
  '2.0.0',
  DATE'2018-11-04',
  'user1',
  'First version of eb3',
  '123456789',
  '1',
  1024
);

-- test data for extensions

insert into EXTENSION (
  id, bundle_version_id, name, display_name, type, content, has_additional_details
) values (
  'e1', 'eb1-v1', 'org.apache.nifi.ExampleProcessor', 'ExampleProcessor', 'PROCESSOR', '{ "name" : "org.apache.nifi.ExampleProcessor", "type" : "PROCESSOR" }', 0
);

insert into EXTENSION (
  id, bundle_version_id, name, display_name, type, content, has_additional_details
) values (
  'e2', 'eb1-v1', 'org.apache.nifi.ExampleProcessorRestricted', 'ExampleProcessorRestricted', 'PROCESSOR', '{ "name" : "org.apache.nifi.ExampleProcessorRestricted", "type" : "PROCESSOR" }', 0
);

insert into EXTENSION (
  id, bundle_version_id, name, display_name, type, content, additional_details, has_additional_details
) values (
  'e3', 'eb2-v1', 'org.apache.nifi.ExampleService', 'ExampleService', 'CONTROLLER_SERVICE', '{ "name" : "org.apache.nifi.ExampleService", "type" : "CONTROLLER_SERVICE" }', 'extra docs', 1
);

-- test data for extension restrictions

insert into EXTENSION_RESTRICTION (
  id, extension_id, required_permission, explanation
) values (
  'er1', 'e2', 'write filesystem', 'This writes to the filesystem'
);

-- test data for extension provided service apis

insert into EXTENSION_PROVIDED_SERVICE_API (
  id, extension_id, class_name, group_id, artifact_id, version
) values (
  'epapi1', 'e3', 'org.apache.nifi.ExampleServiceAPI', 'org.apache.nifi', 'nifi-example-service-api-nar', '2.0.0'
);

-- test data for extension tags

insert into EXTENSION_TAG (extension_id, tag) values ('e1', 'example');
insert into EXTENSION_TAG (extension_id, tag) values ('e1', 'processor');

insert into EXTENSION_TAG (extension_id, tag) values ('e2', 'example');
insert into EXTENSION_TAG (extension_id, tag) values ('e2', 'processor');
insert into EXTENSION_TAG (extension_id, tag) values ('e2', 'restricted');

insert into EXTENSION_TAG (extension_id, tag) values ('e3', 'example');
insert into EXTENSION_TAG (extension_id, tag) values ('e3', 'service');