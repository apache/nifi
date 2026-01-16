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
package org.apache.nifi.record.path.math;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.StandardFieldValue;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;

public class MathBinaryEvaluator extends MathEvaluator<MathBinaryOperator> {
    public MathBinaryEvaluator(MathBinaryOperator op) {
        super(op);
    }

    public static MathBinaryEvaluator divide() {
        return new MathBinaryEvaluator(new MathDivideOperator());
    }

    public static MathBinaryEvaluator multiply() {
        return new MathBinaryEvaluator(new MathMultiplyOperator());
    }

    public FieldValue evaluate(FieldValue lhs, FieldValue rhs) {
        final Number lhsValue = MathTypeUtils.coerceNumber(lhs);
        final Number rhsValue = MathTypeUtils.coerceNumber(rhs);

        Number result;
        DataType resultType;

        if (MathTypeUtils.isLongCompatible(lhsValue) && MathTypeUtils.isLongCompatible(rhsValue)) {
            result = op.operate(lhsValue.longValue(), rhsValue.longValue());
            resultType = RecordFieldType.LONG.getDataType();
        } else {
            result = op.operate(lhsValue.doubleValue(), rhsValue.doubleValue());
            resultType = RecordFieldType.DOUBLE.getDataType();
        }

        return new StandardFieldValue(result, new RecordField(op.getFieldName(), resultType), null);
    }
}
