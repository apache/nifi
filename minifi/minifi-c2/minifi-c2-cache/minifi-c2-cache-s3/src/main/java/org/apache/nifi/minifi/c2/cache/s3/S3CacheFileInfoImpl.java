/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.minifi.c2.cache.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.IOException;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.minifi.c2.api.ConfigurationProviderException;
import org.apache.nifi.minifi.c2.api.cache.ConfigurationCacheFileInfo;
import org.apache.nifi.minifi.c2.api.cache.WriteableConfiguration;
import org.apache.nifi.minifi.c2.api.util.Pair;

public class S3CacheFileInfoImpl implements ConfigurationCacheFileInfo {

  private final AmazonS3 s3;
  private final String bucket;
  private final String prefix;
  private final String expectedFilename;

  /**
   * Creates a new S3 cache file info.
   * @param s3 The {@link AmazonS3 client}.
   * @param bucket The S3 bucket.
   * @param prefix The S3 object prefix.
   */
  public S3CacheFileInfoImpl(AmazonS3 s3, String bucket, String prefix,
    String expectedFilename) {

    this.s3 = s3;
    this.bucket = bucket;
    this.prefix = prefix;
    this.expectedFilename = expectedFilename;

  }

  @Override
  public Integer getVersionIfMatch(String objectKey) {

    String filename = objectKey.substring(prefix.length());

    int expectedFilenameLength = expectedFilename.length();
    if (!filename.startsWith(expectedFilename) || filename.length() == expectedFilenameLength) {
      return null;
    }
    try {
      return Integer.parseInt(filename.substring(expectedFilenameLength));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  @Override
  public WriteableConfiguration getConfiguration(Integer version)
      throws ConfigurationProviderException {

    if (version == null) {

      try {
        return getCachedConfigurations().findFirst()
          .orElseThrow(() -> new ConfigurationProviderException("No configurations found."));
      } catch (IOException e) {
        throw new ConfigurationProviderException("Unable to get cached configurations.", e);
      }

    } else {

      final S3Object s3Object;

      if (StringUtils.isEmpty(prefix) || StringUtils.equals(prefix, "/")) {
        s3Object = s3.getObject(new GetObjectRequest(bucket,
          expectedFilename + version.toString()));
      } else {
        s3Object = s3.getObject(new GetObjectRequest(bucket,
          prefix + expectedFilename + version.toString()));
      }

      if (s3Object == null) {
        throw new ConfigurationProviderException("No configurations found for object key.");
      }

      return new S3WritableConfiguration(s3, s3Object, Integer.toString(version));

    }

  }

  @Override
  public Stream<WriteableConfiguration> getCachedConfigurations() throws IOException {

    Iterable<S3ObjectSummary> objectSummaries = S3Objects.withPrefix(s3, bucket, prefix);
    Stream<S3ObjectSummary> objectStream = StreamSupport.stream(objectSummaries.spliterator(), false);

    return objectStream.map(p -> {
      Integer version = getVersionIfMatch(p.getKey());
      if (version == null) {
        return null;
      }
      return new Pair<>(version, p);
    }).filter(Objects::nonNull)
        .sorted(Comparator.comparing(pair -> ((Pair<Integer, S3ObjectSummary>) pair).getFirst())
              .reversed()).map(pair -> new S3WritableConfiguration(s3, pair.getSecond(), Integer.toString(pair.getFirst())));

  }

}
