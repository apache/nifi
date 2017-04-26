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
package org.apache.nifi.processors.cybersecurity.matchers;

import info.debatty.java.spamsum.SpamSum;
import org.apache.nifi.logging.ComponentLog;

import java.util.Scanner;

public class SSDeepHashMatcher implements FuzzyHashMatcher {

    ComponentLog logger;

    public SSDeepHashMatcher() {

    }

    public SSDeepHashMatcher(ComponentLog logger) {
        this.logger = logger;
    }

    @Override
    public boolean matchExceedsThreshold(double similarity, double matchThreshold) {
        if (similarity >= matchThreshold) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public double getSimilarity(String inputHash, String existingHash) {
        String[] hashToCompare = existingHash.split(",", 2);
        if (hashToCompare.length > 0) {
            return new SpamSum().match(inputHash, hashToCompare[0]);
        } else {
            return Double.NaN;
        }
    }

    @Override
    public boolean isValidHash(String inputHash) {
        // format looks like
        // blocksize:hash:hash

        String [] fields = inputHash.split(":", 3);

        if (fields.length == 3) {
            Scanner sc = new Scanner(fields[0]);

            boolean isNumber = sc.hasNextInt();
            if (isNumber == false && logger != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Field should be numeric but got '{}'. Will tell processor to ignore.",
                            new Object[] {fields[0]});
                }
            }

            boolean hashOneIsNotEmpty = !fields[1].isEmpty();
            boolean hashTwoIsNotEmpty = !fields[2].isEmpty();

            if (isNumber && hashOneIsNotEmpty && hashTwoIsNotEmpty) {
                return true;
            }
        }
        return false;
    }
}
