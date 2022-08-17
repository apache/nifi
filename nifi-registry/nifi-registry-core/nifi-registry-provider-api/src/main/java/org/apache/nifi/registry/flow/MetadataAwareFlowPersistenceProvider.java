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
package org.apache.nifi.registry.flow;

import org.apache.nifi.registry.metadata.BucketMetadata;

import java.util.List;

/**
 * A FlowPersistenceProvider that is able to provide metadata about the flows and buckets.
 *
 * If the application is started with an empty metadata database AND a MetadataAwareFlowPersistenceProvider,
 * then the application will use this information to rebuild the database.
 *
 * NOTE: Some information will be lost, such as created date, last modified date, and original author.
 */
public interface MetadataAwareFlowPersistenceProvider extends FlowPersistenceProvider {

    /**
     * @return the list of metadata for each bucket
     */
    List<BucketMetadata> getMetadata();

}
