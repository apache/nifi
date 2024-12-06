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

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.StandardFieldValue;
import org.apache.nifi.record.path.paths.RecordPathSegment;
import org.apache.nifi.record.path.util.RecordPathUtils;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.encrypt.PropertyEncryptorBuilder;

import java.util.stream.Stream;

public class Encrypt extends RecordPathSegment {

    private final RecordPathSegment recordPath;
    private final RecordPathSegment encryptionKey;
    private final RecordPathSegment encryptionAlgorithm;

    public Encrypt(final RecordPathSegment recordPath, final RecordPathSegment encryptionKey, final RecordPathSegment encryptionAlgorithm,
                   final boolean absolute) {
        super("encrypt", null, absolute);
        this.recordPath = recordPath;
        this.encryptionKey = encryptionKey;
        this.encryptionAlgorithm = encryptionAlgorithm;
    }

    @Override
    public Stream<FieldValue> evaluate(RecordPathEvaluationContext context) {
        final Stream<FieldValue> fieldValues = recordPath.evaluate(context);
        return fieldValues.filter(fv -> fv.getValue() != null)
                .map(fv -> {
                    Object value = fv.getValue();
                    String encryptedValue;
                    final String encryptionKeyString = RecordPathUtils.getFirstStringValue(encryptionKey, context);
                    final String encryptionAlgorithmString = RecordPathUtils.getFirstStringValue(encryptionAlgorithm, context);
                    if (encryptionAlgorithmString == null) {
                        return fv;
                    }

                    final PropertyEncryptor encryptor = getPropertyEncryptor(encryptionKeyString, encryptionAlgorithmString);
                    final String encrypted = encryptor.encrypt(value.toString());

                    return new StandardFieldValue(encrypted, fv.getField(), fv.getParent().orElse(null));
                });
    }

    private PropertyEncryptor getPropertyEncryptor(final String propertiesKey, final String propertiesAlgorithm) {
        return new PropertyEncryptorBuilder(propertiesKey).setAlgorithm(propertiesAlgorithm).build();
    }

}
