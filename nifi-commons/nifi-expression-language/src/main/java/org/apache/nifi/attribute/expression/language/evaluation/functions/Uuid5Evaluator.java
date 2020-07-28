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
package org.apache.nifi.attribute.expression.language.evaluation.functions;

import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

// This is based on an unreleased implementation in Apache Commons Id. See http://svn.apache.org/repos/asf/commons/sandbox/id/trunk/src/java/org/apache/commons/id/uuid/UUID.java
public class Uuid5Evaluator extends StringEvaluator {

    private final Evaluator<String> subject;
    private final Evaluator<String> namespace;

    public Uuid5Evaluator(final Evaluator<String> subject, final Evaluator<String> namespace) {
        this.subject = subject;
        this.namespace = namespace;
    }

    @Override
    public QueryResult<String> evaluate(final EvaluationContext evaluationContext) {
      final String subjectValue = subject.evaluate(evaluationContext).getValue();
      if (subjectValue == null) {
          return new StringQueryResult(null);
      }
      final String nsValue = namespace.evaluate(evaluationContext).getValue();
      final UUID nsUUID = nsValue == null ? new UUID(0, 0) : UUID.fromString(nsValue);

      final byte[] nsBytes =
          ByteBuffer.wrap(new byte[16])
              .putLong(nsUUID.getMostSignificantBits())
              .putLong(nsUUID.getLeastSignificantBits())
              .array();

      final byte[] subjectBytes = subjectValue.getBytes();
      final byte[] nameBytes = ArrayUtils.addAll(nsBytes, subjectBytes);
      final byte[] nameHash = DigestUtils.sha1(nameBytes);
      final byte[] uuidBytes = Arrays.copyOf(nameHash, 16);

      uuidBytes[6] &= 0x0F;
      uuidBytes[6] |= 0x50;
      uuidBytes[8] &= 0x3f;
      uuidBytes[8] |= 0x80;

      return new StringQueryResult(toString(uuidBytes));
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }

    private static String toString(final byte[] uuid) {
        if (uuid == null) {
            return new UUID(0, 0).toString();
        }

        final String encoded = Hex.encodeHexString(Objects.requireNonNull(uuid));
        final StringBuffer sb = new StringBuffer(encoded);

        while (sb.length() != 32) {
            sb.insert(0, "0");
        }

        sb.ensureCapacity(32);
        sb.insert(8, '-');
        sb.insert(13, '-');
        sb.insert(18, '-');
        sb.insert(23, '-');

        return sb.toString();
    }

}
