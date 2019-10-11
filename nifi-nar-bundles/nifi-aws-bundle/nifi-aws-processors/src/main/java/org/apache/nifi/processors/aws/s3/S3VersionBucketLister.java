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
package org.apache.nifi.processors.aws.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.VersionListing;

public class S3VersionBucketLister implements S3BucketLister {
    private AmazonS3 client;
    private ListVersionsRequest listVersionsRequest;
    private VersionListing versionListing;

    public S3VersionBucketLister(AmazonS3 client) {
        this.client = client;
    }

    @Override
    public void setBucketName(String bucketName) {
        listVersionsRequest = new ListVersionsRequest().withBucketName(bucketName);
    }

    @Override
    public void setPrefix(String prefix) {
        listVersionsRequest.setPrefix(prefix);
    }

    @Override
    public void setDelimiter(String delimiter) {
        listVersionsRequest.setDelimiter(delimiter);
    }

    @Override
    public void setRequesterPays(boolean requesterPays) {
        // Not supported in versionListing, so this does nothing.
    }

    @Override
    public VersionListing listVersions() {
        versionListing = client.listVersions(listVersionsRequest);
        return versionListing;
    }

    @Override
    public void setNextMarker() {
        listVersionsRequest.setKeyMarker(versionListing.getNextKeyMarker());
        listVersionsRequest.setVersionIdMarker(versionListing.getNextVersionIdMarker());
    }

    @Override
    public boolean isTruncated() {
        return (versionListing == null) ? false : versionListing.isTruncated();
    }
}
