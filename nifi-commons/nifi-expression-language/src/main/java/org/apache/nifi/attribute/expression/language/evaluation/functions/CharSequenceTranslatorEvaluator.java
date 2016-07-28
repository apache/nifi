package org.apache.nifi.attribute.expression.language.evaluation.functions;

import java.util.Map;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.text.translate.CharSequenceTranslator;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;

public class CharSequenceTranslatorEvaluator extends StringEvaluator 
{
    public static StringEvaluator jsonEscapeEvaluator(final Evaluator<String> subject){
    	return new CharSequenceTranslatorEvaluator(subject, StringEscapeUtils.ESCAPE_JSON);
    }
    
    public static StringEvaluator xmlEscapeEvaluator(final Evaluator<String> subject){
    	return new CharSequenceTranslatorEvaluator(subject, StringEscapeUtils.ESCAPE_XML10);
    }
    
    public static StringEvaluator csvEscapeEvaluator(final Evaluator<String> subject){
    	return new CharSequenceTranslatorEvaluator(subject, StringEscapeUtils.ESCAPE_CSV);
    }

    public static StringEvaluator html3EscapeEvaluator(final Evaluator<String> subject){
    	return new CharSequenceTranslatorEvaluator(subject, StringEscapeUtils.ESCAPE_HTML3);
    }
    
    public static StringEvaluator html4EscapeEvaluator(final Evaluator<String> subject){
    	return new CharSequenceTranslatorEvaluator(subject, StringEscapeUtils.ESCAPE_HTML4);
    }
    
    public static StringEvaluator jsonUnescapeEvaluator(final Evaluator<String> subject){
    	return new CharSequenceTranslatorEvaluator(subject, StringEscapeUtils.UNESCAPE_JSON);
    }
    
    public static StringEvaluator xmlUnescapeEvaluator(final Evaluator<String> subject){
    	return new CharSequenceTranslatorEvaluator(subject, StringEscapeUtils.UNESCAPE_XML);
    }
    
    public static StringEvaluator csvUnescapeEvaluator(final Evaluator<String> subject){
    	return new CharSequenceTranslatorEvaluator(subject, StringEscapeUtils.UNESCAPE_CSV);
    }

    public static StringEvaluator html3UnescapeEvaluator(final Evaluator<String> subject){
    	return new CharSequenceTranslatorEvaluator(subject, StringEscapeUtils.UNESCAPE_HTML3);
    }
    
    public static StringEvaluator html4UnescapeEvaluator(final Evaluator<String> subject){
    	return new CharSequenceTranslatorEvaluator(subject, StringEscapeUtils.UNESCAPE_HTML4);
    }
    
    
    private final Evaluator<String> subject;
    private final CharSequenceTranslator method;

    public CharSequenceTranslatorEvaluator(final Evaluator<String> subject, CharSequenceTranslator method) {
        this.subject = subject;
        this.method = method;
    }

    @Override
    public QueryResult<String> evaluate(final Map<String, String> attributes) {
        final String subjectValue = subject.evaluate(attributes).getValue();
        return new StringQueryResult(subjectValue == null ? "" : method.translate(subjectValue));
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }
}