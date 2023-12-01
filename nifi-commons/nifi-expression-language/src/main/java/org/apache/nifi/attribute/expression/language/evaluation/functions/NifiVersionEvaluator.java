package org.apache.nifi.attribute.expression.language.evaluation.functions;

import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.nar.NarClassLoadersHolder;
import org.slf4j.LoggerFactory;

import java.util.Set;


public class NifiVersionEvaluator extends StringEvaluator {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(NifiVersionEvaluator.class);

    @Override
    public QueryResult<String> evaluate(EvaluationContext evaluationContext) {
        String version = null;
        try {
            final Bundle frameworkBundle = NarClassLoadersHolder.getInstance().getFrameworkBundle();
            if (frameworkBundle != null) {
                final BundleDetails frameworkDetails = frameworkBundle.getBundleDetails();
                // set the version
                version = frameworkDetails.getCoordinate().getVersion();
            } else {
                logger.error("frameworkBundle is null");
            }
        } catch (IllegalStateException ie) {
            logger.error("Error getting nifi version", ie);
        }
        return new StringQueryResult(version);
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return null;
    }
}
