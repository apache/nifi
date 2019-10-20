package org.apache.nifi.attribute.expression.language.evaluation.functions;

import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.StandardPreparedQuery;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;

public class StringElEvaluator extends StringEvaluator {
	
	private final Evaluator<String> subject;
	
	public StringElEvaluator(final Evaluator<String> subject) {
        this.subject = subject;
    }

	@Override
	public QueryResult<String> evaluate(EvaluationContext evaluationContext) {
		final String subjectValue = subject.evaluate(evaluationContext).getValue();
		final String evaluated = ((StandardPreparedQuery) Query.prepare(subjectValue)).evaluateExpressions(evaluationContext, null);
        return new StringQueryResult(evaluated);
	}

	@Override
	public Evaluator<?> getSubjectEvaluator() {
		return subject;
	}

}
