/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.util.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hive.hcatalog.streaming.ConnectionError;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class HiveUtils {
    private static final Logger LOG = LoggerFactory.getLogger(HiveUtils.class);

    public static HiveEndPoint makeEndPoint(List<String> partitionVals, HiveOptions options) throws ConnectionError {
        return new HiveEndPoint(options.getMetaStoreURI(), options.getDatabaseName(), options.getTableName(), partitionVals);
    }

    public static HiveWriter makeHiveWriter(HiveEndPoint endPoint, ExecutorService callTimeoutPool, UserGroupInformation ugi, HiveOptions options, HiveConf hiveConf)
        throws HiveWriter.ConnectFailure, InterruptedException {
        return new HiveWriter(endPoint, options.getTxnsPerBatch(), options.getAutoCreatePartitions(),
                              options.getCallTimeOut(), callTimeoutPool, ugi, hiveConf);
    }

    public static void logAllHiveEndPoints(Map<HiveEndPoint, HiveWriter> allWriters) {
        for (Map.Entry<HiveEndPoint,HiveWriter> entry : allWriters.entrySet()) {
            LOG.info("cached writers {} ", entry.getValue());
        }
    }

    /**
     * Validates that one or more files exist, as specified in a single property.
     */
    public static Validator createMultipleFilesExistValidator() {
        return (subject, input, context) -> {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
            }
            final String[] files = input.split("\\s*,\\s*");
            for (String filename : files) {
                try {
                    final File file = new File(filename.trim());
                    final boolean valid = file.exists() && file.isFile();
                    if (!valid) {
                        final String message = "File " + file + " does not exist or is not a file";
                        return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(message).build();
                    }
                } catch (SecurityException e) {
                    final String message = "Unable to access " + filename + " due to " + e.getMessage();
                    return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(message).build();
                }
            }
            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        };
    }
}
