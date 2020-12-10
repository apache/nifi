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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.minifi.c2.api.InvalidParameterException;
import org.apache.nifi.minifi.c2.api.cache.ConfigurationCache;
import org.apache.nifi.minifi.c2.api.cache.ConfigurationCacheFileInfo;

public class S3ConfigurationCache implements ConfigurationCache {

  private final AmazonS3 s3;
  private final String bucket;
  private final String prefix;
  private final String pathPattern;

  /**
   * Creates a new S3 configuration cache.
   * @param bucket The S3 bucket.
   * @param prefix The S3 object prefix.
   * @param pathPattern The path pattern.
   * @param accessKey The (optional) S3 access key.
   * @param secretKey The (optional) S3 secret key.
   * @param region The AWS region (e.g. us-east-1).
   * @throws IOException Thrown if the configuration cannot be read.
   */
  public S3ConfigurationCache(String bucket, String prefix, String pathPattern,
      String accessKey, String secretKey, String region) throws IOException {

    this.bucket = bucket;
    this.prefix = prefix;
    this.pathPattern = pathPattern;

    if (!StringUtils.isEmpty(accessKey)) {

      s3 = AmazonS3Client.builder()
        .withCredentials(new AWSStaticCredentialsProvider(
           new BasicAWSCredentials(accessKey, secretKey)))
          .withRegion(Regions.fromName(region))
          .build();

    } else {

      s3 = AmazonS3Client.builder()
          .withRegion(Regions.fromName(region))
          .build();
    }

  }

  @Override
  public ConfigurationCacheFileInfo getCacheFileInfo(String contentType,
      Map<String, List<String>> parameters) throws InvalidParameterException {

    String pathString = pathPattern;
    for (Map.Entry<String, List<String>> entry : parameters.entrySet()) {
      if (entry.getValue().size() != 1) {
        throw new InvalidParameterException("Multiple values for same parameter"
          + " are not supported by this provider.");
      }
      pathString = pathString.replaceAll(Pattern.quote("${" + entry.getKey() + "}"),
        entry.getValue().get(0));
    }
    pathString = pathString + "." + contentType.replace('/', '.');
    String[] split = pathString.split("/");
    for (String s1 : split) {
      int openBrace = s1.indexOf("${");
      if (openBrace >= 0 && openBrace < s1.length() + 2) {
        int closeBrace = s1.indexOf("}", openBrace + 2);
        if (closeBrace >= 0) {
          throw new InvalidParameterException("Found unsubstituted variable "
            + s1.substring(openBrace + 2, closeBrace));
        }
      }
    }

    return new S3CacheFileInfoImpl(s3, bucket, prefix, pathString + ".v");
  }

}
