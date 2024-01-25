# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import yake

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope

class ExtractKeywords(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "2.0.0-SNAPSHOT"
        description = """Parses the content of the incoming FlowFile to extract the keywords and add those as attributes 
        using the convention 'keyword.i' where i is a number depending on how many keywords are extracted. The attribute 
        'keywords.length' will be set with the number of extracted keywords. The attribute 'keyword.0' will contain the 
        list of all extracted keywords with the associated score."""
        tags = ["text", "keyword", "vector", "extract"]
        dependencies = ['yake']


    LANGUAGE = PropertyDescriptor(
        name="Language",
        description="Language to be used for keywords extraction",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value="en",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )
    MAX_NGRAM_SIZE = PropertyDescriptor(
        name="Maximum N-gram size",
        description="Maximum number of words in a given keyword",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value="1",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )
    DEDUP_THRESHOLD = PropertyDescriptor(
        name="Deduplication Threshold",
        description="Limits the duplication of words across extracted keywords. The value must be between 0 and 1, inclusive.",
        validators=[StandardValidators._standard_validators.createNonNegativeFloatingPointValidator(1.0)],
        default_value="0.1",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )
    NUM_KEYWORDS = PropertyDescriptor(
        name="Maximum number of keywords",
        description="Maximum number of keywords to extract",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value="10",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [LANGUAGE,
                            MAX_NGRAM_SIZE,
                            DEDUP_THRESHOLD,
                            NUM_KEYWORDS]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def transform(self, context, flowFile):
        text = flowFile.getContentsAsBytes().decode('utf-8')
        language = context.getProperty(self.LANGUAGE).evaluateAttributeExpressions(flowFile).getValue()
        max_ngram_size = context.getProperty(self.MAX_NGRAM_SIZE).evaluateAttributeExpressions(flowFile).asInteger()
        dedup_threshold = context.getProperty(self.DEDUP_THRESHOLD).evaluateAttributeExpressions(flowFile).asFloat()
        num_keywords = context.getProperty(self.NUM_KEYWORDS).evaluateAttributeExpressions(flowFile).asInteger()

        kw_extractor = yake.KeywordExtractor(lan=language, n=max_ngram_size, dedupLim=dedup_threshold, top=num_keywords, features=None)
        keywords = kw_extractor.extract_keywords(text)
        attributes = {'keywords.length': str(len(keywords)), "keyword.0": str(keywords)}

        i = 1
        for kw in keywords:
            attributes["keyword." + str(i)] = kw[0]
            i += 1

        return FlowFileTransformResult("success", contents=None, attributes=attributes)
