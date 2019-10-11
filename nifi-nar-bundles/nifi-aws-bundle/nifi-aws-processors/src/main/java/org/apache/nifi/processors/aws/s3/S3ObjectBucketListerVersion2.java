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
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;

public class S3ObjectBucketListerVersion2 implements S3BucketLister {
    private AmazonS3 client;
    private ListObjectsV2Request listObjectsRequest;
    private ListObjectsV2Result objectListing;

    public S3ObjectBucketListerVersion2(AmazonS3 client) {
        this.client = client;
    }

    @Override
    public void setBucketName(String bucketName) {
        listObjectsRequest = new ListObjectsV2Request().withBucketName(bucketName);
    }

    @Override
    public void setPrefix(String prefix) {
        listObjectsRequest.setPrefix(prefix);
    }

    @Override
    public void setDelimiter(String delimiter) {
        listObjectsRequest.setDelimiter(delimiter);
    }

    @Override
    public void setRequesterPays(boolean requesterPays) {
        listObjectsRequest.setRequesterPays(requesterPays);
    }

    @Override
    public VersionListing listVersions() {
        VersionListing versionListing = new VersionListing();
        this.objectListing = client.listObjectsV2(listObjectsRequest);
        for(S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            S3VersionSummary versionSummary = new S3VersionSummary();
            versionSummary.setBucketName(objectSummary.getBucketName());
            versionSummary.setETag(objectSummary.getETag());
            versionSummary.setKey(objectSummary.getKey());
            versionSummary.setLastModified(objectSummary.getLastModified());
            versionSummary.setOwner(objectSummary.getOwner());
            versionSummary.setSize(objectSummary.getSize());
            versionSummary.setStorageClass(objectSummary.getStorageClass());
            versionSummary.setIsLatest(true);

            versionListing.getVersionSummaries().add(versionSummary);
        }

        return versionListing;
    }

    @Override
    public void setNextMarker() {
        listObjectsRequest.setContinuationToken(objectListing.getNextContinuationToken());
    }

    @Override
    public boolean isTruncated() {
        return (objectListing == null) ? false : objectListing.isTruncated();
    }

}
