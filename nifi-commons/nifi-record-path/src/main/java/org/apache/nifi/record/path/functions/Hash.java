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

package org.apache.nifi.record.path.functions;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.StandardFieldValue;
import org.apache.nifi.record.path.exception.RecordPathException;
import org.apache.nifi.record.path.paths.RecordPathSegment;
import org.apache.nifi.record.path.util.RecordPathUtils;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.util.stream.Stream;

public class Hash extends RecordPathSegment {
    private static final String SUPPORTED_ALGORITHMS = String.join(", ", Security.getAlgorithms("MessageDigest"));

    private final RecordPathSegment recordPath;
    private final RecordPathSegment algorithmPath;

    public Hash(final RecordPathSegment recordPath, final RecordPathSegment algorithmPath, final boolean absolute) {
        super("hash", null, absolute);

        this.recordPath = recordPath;
        this.algorithmPath = algorithmPath;
    }

    @Override
    public Stream<FieldValue> evaluate(final RecordPathEvaluationContext context) {
        final Stream<FieldValue> fieldValues = recordPath.evaluate(context);
        return fieldValues.filter(fv -> fv.getValue() != null)
            .map(fv -> {
                final String algorithmValue = RecordPathUtils.getFirstStringValue(algorithmPath, context);
                if (algorithmValue == null || algorithmValue.isEmpty()) {
                    return fv;
                }

                final MessageDigest digest = getDigest(algorithmValue);
                final String value = DataTypeUtils.toString(fv.getValue(), (String) null);
                String encoded = new DigestUtils(digest).digestAsHex(value);
                return new StandardFieldValue(encoded, fv.getField(), fv.getParent().orElse(null));
            });
    }

    private MessageDigest getDigest(String algorithm){
        try {
            return MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new RecordPathException("Invalid hash algorithm: " + algorithm + "not in set [" + SUPPORTED_ALGORITHMS + "]", e);
        }
    }

}
