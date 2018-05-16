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
package org.apache.nifi.toolkit.cli.impl.command.registry.bucket;

import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.BucketsResult;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Command to list all buckets in the registry instance.
 */
public class ListBuckets extends AbstractNiFiRegistryCommand<BucketsResult> {

    public ListBuckets() {
        super("list-buckets", BucketsResult.class);
    }

    @Override
    public String getDescription() {
        return "Lists the buckets that the current user has access to.";
    }

    @Override
    public BucketsResult doExecute(final NiFiRegistryClient client, final Properties properties)
            throws IOException, NiFiRegistryException {
        final List<Bucket> buckets = client.getBucketClient().getAll();
        return new BucketsResult(getResultType(properties), buckets);
    }

}
