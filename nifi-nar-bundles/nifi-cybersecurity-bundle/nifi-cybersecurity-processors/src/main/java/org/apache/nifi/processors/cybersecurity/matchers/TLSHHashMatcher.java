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


import com.idealista.tlsh.digests.Digest;
import com.idealista.tlsh.digests.DigestBuilder;
import org.apache.nifi.logging.ComponentLog;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.apache.nifi.processors.cybersecurity.CompareFuzzyHash.HASH_LIST_FILE;

public class TLSHHashMatcher implements FuzzyHashMatcher {

    ComponentLog logger;

    public TLSHHashMatcher(ComponentLog logger) {
        this.logger = logger;
    }

    @Override
    public BufferedReader getReader(String source) throws IOException {

        File file = new File(source);

        FileInputStream fileInputStream = new FileInputStream(file);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));

        return reader;
    }

    @Override
    public boolean matchExceedsThreshold(double similarity, double matchThreshold) {
        if (similarity <= matchThreshold) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public double getSimilarity(String inputHash, String existingHash) {
        String[] hashToCompare = existingHash.split("\t", 2);
        // This will return null in case it fails validation
        if (isValidHash(inputHash) && isValidHash(hashToCompare[0])) {
            Digest inputDigest = new DigestBuilder().withHash(inputHash).build();
            Digest existingHashDigest = new DigestBuilder().withHash(hashToCompare[0]).build();

            return inputDigest.calculateDifference(existingHashDigest, true);
        } else {
            return Double.NaN;
        }
    }

    @Override
    public boolean isValidHash(String stringFromHashList) {
        String[] hashToCompare = stringFromHashList.split("\t", 2);
        // This will return null in case it fails validation
        if (hashToCompare.length > 0) {
            // Because DigestBuilder raises all sort of exceptions, so in order to keep the onTrigger loop a
            // bit cleaner, we capture them here and return NaN to the loop above, otherwise simply return the
            // similarity score.
            try {
                Digest digest = new DigestBuilder().withHash(hashToCompare[0]).build();
                return true;
            } catch (ArrayIndexOutOfBoundsException | StringIndexOutOfBoundsException | NumberFormatException e) {
                logger.error("Got {} while processing the string '{}'. This usually means the file " +
                                "defined by '{}' property contains invalid entries.",
                        new Object[]{e.getCause(), hashToCompare[0], HASH_LIST_FILE.getDisplayName()});
            }
        }
        return false;
    }

    @Override
    public String getHash(String line) {
        if (isValidHash(line)) {
            return line.split("\t", 2)[0];
        } else {
            return null;
        }
    }

    @Override
    public String getMatch(String line) {
        if (isValidHash(line)) {
            String[] match = line.split("\t", 2);
            // Because the file can be malformed and contain an unammed match,
            // if match has a filename...
            if (match.length == 2) {
                // Return it.
                return match[1];
            }
        }
        // Or return null
        return null;
    }
}
