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
package org.apache.nifi.registry.provider.flow;

import org.apache.nifi.registry.flow.FlowSnapshotContext;
import org.junit.Assert;
import org.junit.Test;

public class TestStandardFlowSnapshotContext {

    @Test
    public void testBuilder() {
        final String bucketId = "1234-1234-1234-1234";
        final String bucketName = "Some Bucket";
        final String flowId = "2345-2345-2345-2345";
        final String flowName = "Some Flow";
        final int version = 2;
        final String comments = "Some Comments";
        final String author = "anonymous";
        final long timestamp = System.currentTimeMillis();

        final FlowSnapshotContext context = new StandardFlowSnapshotContext.Builder()
                .bucketId(bucketId)
                .bucketName(bucketName)
                .flowId(flowId)
                .flowName(flowName)
                .version(version)
                .comments(comments)
                .author(author)
                .snapshotTimestamp(timestamp)
                .build();

        Assert.assertEquals(bucketId, context.getBucketId());
        Assert.assertEquals(bucketName, context.getBucketName());
        Assert.assertEquals(flowId, context.getFlowId());
        Assert.assertEquals(flowName, context.getFlowName());
        Assert.assertEquals(version, context.getVersion());
        Assert.assertEquals(comments, context.getComments());
        Assert.assertEquals(author, context.getAuthor());
        Assert.assertEquals(timestamp, context.getSnapshotTimestamp());
    }

}
