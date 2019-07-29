package org.apache.nifi.record.path.functions;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.StandardFieldValue;
import org.apache.nifi.record.path.paths.RecordPathSegment;
import org.apache.nifi.record.path.util.RecordPathUtils;
import org.apache.nifi.record.path.util.TriFunction;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

abstract class Padding extends RecordPathSegment {

    public final static char DEFAULT_PADDING_CHAR = '_';

    private RecordPathSegment paddingCharPath;
    private RecordPathSegment inputStringPath;
    private RecordPathSegment desiredLengthPath;

    Padding(final String path,
            final RecordPathSegment parentPath,
            final RecordPathSegment inputStringPath,
            final RecordPathSegment desiredLengthPath,
            final RecordPathSegment paddingCharPath,
            final boolean absolute) {

        super(path, parentPath, absolute);
        this.paddingCharPath = paddingCharPath;
        this.inputStringPath = inputStringPath;
        this.desiredLengthPath = desiredLengthPath;
    }

    final Stream<FieldValue> evaluate(RecordPathEvaluationContext context,
                                      TriFunction<String, Integer, Character, String> paddingFunction) {
        char pad = getPaddingChar(context);

        final Stream<FieldValue> evaluatedStr = inputStringPath.evaluate(context);
        return evaluatedStr.filter(fv -> fv.getValue() != null).map(fv -> {

            final OptionalInt desiredLengthOpt = getDesiredLength(context);
            if (!desiredLengthOpt.isPresent()) {
                return new StandardFieldValue("", fv.getField(), fv.getParent().orElse(null));
            }

            int desiredLength = desiredLengthOpt.getAsInt();
            final String value = DataTypeUtils.toString(fv.getValue(), (String) null);
            return new StandardFieldValue(paddingFunction.apply(value, desiredLength, pad), fv.getField(), fv.getParent().orElse(null));
        });
    }


    private OptionalInt getDesiredLength(RecordPathEvaluationContext context) {

        Optional<FieldValue> lengthOption = desiredLengthPath.evaluate(context).findFirst();

        if (!lengthOption.isPresent()) {
            return OptionalInt.empty();
        }

        final FieldValue fieldValue = lengthOption.get();
        final Object length = fieldValue.getValue();
        if (!DataTypeUtils.isIntegerTypeCompatible(length)) {
            return OptionalInt.empty();
        }

        final String fieldName;
        final RecordField field = fieldValue.getField();
        fieldName = field == null ? "<Unknown Field>" : field.getFieldName();

        return OptionalInt.of(DataTypeUtils.toInteger(length, fieldName));
    }

    private char getPaddingChar(RecordPathEvaluationContext context){

        if (null == paddingCharPath) {
            return DEFAULT_PADDING_CHAR;
        }

        String padStr = RecordPathUtils.getFirstStringValue(paddingCharPath, context);

        if (null != padStr && !padStr.isEmpty()){
            return padStr.charAt(0);
        }
        return DEFAULT_PADDING_CHAR;
    }
}
