package org.apache.nifi.record.path.functions;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.paths.RecordPathSegment;
import org.apache.nifi.util.StringUtils;

import java.util.stream.Stream;

public class PadRight extends Padding {

    public PadRight(final RecordPathSegment inputStringPath,
                    final RecordPathSegment desiredLengthPath,
                    final RecordPathSegment paddingCharPath,
                    final boolean absolute) {
        super("padRight", null, inputStringPath, desiredLengthPath, paddingCharPath, absolute);
    }

    public PadRight(final RecordPathSegment inputStringPath,
                    final RecordPathSegment desiredLengthPath,
                   final boolean absolute) {
        super("padRight", null, inputStringPath, desiredLengthPath, null, absolute);
    }

    @Override
    public Stream<FieldValue> evaluate(RecordPathEvaluationContext context) {
        return evaluate(context, StringUtils::padRight);
    }
}
