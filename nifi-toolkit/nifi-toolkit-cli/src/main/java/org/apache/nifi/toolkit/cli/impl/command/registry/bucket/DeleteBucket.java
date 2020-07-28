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

import org.apache.commons.cli.ParseException;
import org.apache.nifi.registry.client.BucketClient;
import org.apache.nifi.registry.client.FlowClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.OkResult;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Deletes a bucket from the given registry.
 */
public class DeleteBucket extends AbstractNiFiRegistryCommand<OkResult> {

    public DeleteBucket() {
        super("delete-bucket", OkResult.class);
    }

    @Override
    public String getDescription() {
        return "Deletes the bucket with the given id. If the bucket contains any items then the force option must be used.";
    }

    @Override
    protected void doInitialize(Context context) {
        addOption(CommandOption.BUCKET_ID.createOption());
        addOption(CommandOption.FORCE.createOption());
    }

    @Override
    public OkResult doExecute(final NiFiRegistryClient client, final Properties properties)
            throws IOException, NiFiRegistryException, ParseException {

        final String bucketId = getRequiredArg(properties, CommandOption.BUCKET_ID);
        final boolean forceDelete = properties.containsKey(CommandOption.FORCE.getLongName());

        final FlowClient flowClient = client.getFlowClient();
        final List<VersionedFlow> flowsInBucket = flowClient.getByBucket(bucketId);

        if (flowsInBucket != null && flowsInBucket.size() > 0 && !forceDelete) {
            throw new NiFiRegistryException("Bucket is not empty, use --" + CommandOption.FORCE.getLongName() + " to delete");
        } else {
            final BucketClient bucketClient = client.getBucketClient();
            bucketClient.delete(bucketId);
            return new OkResult(getContext().isInteractive());
        }
    }
}
