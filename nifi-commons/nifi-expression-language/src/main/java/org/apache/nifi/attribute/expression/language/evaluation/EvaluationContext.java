package org.apache.nifi.attribute.expression.language.evaluation;

import java.util.HashMap;
import java.util.Map;

public class EvaluationContext {

    private final Map<Evaluator<?>, Object> statePerEvaluator = new HashMap<>();

    public <T> T getState(Evaluator<?> evaluator, Class<T> clazz) {
        return clazz.cast(statePerEvaluator.get(evaluator));
    }

    public void putState(Evaluator<?> evaluator, Object state) {
        statePerEvaluator.put(evaluator, state);
    }

}
