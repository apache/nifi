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

from langchain.vectorstores import OpenSearchVectorSearch
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope, PropertyDependency
from OpenSearchVectorUtils import (L2, L1, LINF, COSINESIMIL, OPENAI_API_KEY, OPENAI_API_MODEL, HUGGING_FACE_API_KEY, HUGGING_FACE_MODEL, HTTP_HOST,
                                   USERNAME, PASSWORD, INDEX_NAME, VECTOR_FIELD, TEXT_FIELD, create_authentication_params)
from QueryUtils import OUTPUT_STRATEGY, RESULTS_FIELD, INCLUDE_METADATAS, INCLUDE_DISTANCES, QueryUtils
import json
from EmbeddingUtils import EMBEDDING_MODEL, create_embedding_service

class QueryOpenSearchVector(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '@project.version@'
        description = "Queries OpenSearch in order to gather a specified number of documents that are most closely related to the given query."
        tags = ["opensearch", "vector", "vectordb", "vectorstore", "embeddings", "ai", "artificial intelligence", "ml",
                "machine learning", "text", "LLM"]

    # Search types
    APPROXIMATE_SEARCH = ("Approximate Search", "approximate_search")
    SCRIPT_SCORING_SEARCH = ("Script Scoring Search", "script_scoring")
    PAINLESS_SCRIPTING_SEARCH = ("Painless Scripting Search", "painless_scripting")

    SEARCH_TYPE_VALUES = dict([APPROXIMATE_SEARCH, SCRIPT_SCORING_SEARCH, PAINLESS_SCRIPTING_SEARCH])

    # Script Scoring Search space types
    HAMMINGBIT = ("Hamming distance", "hammingbit")

    SCRIPT_SCORING_SPACE_TYPE_VALUES = dict([L2, L1, LINF, COSINESIMIL, HAMMINGBIT])

    # Painless Scripting Search space types
    L2_SQUARED = ("L2 (Euclidean distance)", "l2Squared")
    L1_NORM = ("L1 (Manhattan distance)", "l1Norm")
    COSINE_SIMILARITY = ("Cosine similarity", "cosineSimilarity")

    PAINLESS_SCRIPTING_SPACE_TYPE_VALUES = dict([L2_SQUARED, L1_NORM, COSINE_SIMILARITY])

    QUERY = PropertyDescriptor(
        name="Query",
        description="The text of the query to send to OpenSearch.",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )
    NUMBER_OF_RESULTS = PropertyDescriptor(
        name="Number of Results",
        description="The number of results to return from OpenSearch",
        default_value="10",
        required=True,
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )
    SEARCH_TYPE = PropertyDescriptor(
        name="Search Type",
        description="Specifies the type of the search to be performed.",
        allowable_values=SEARCH_TYPE_VALUES.keys(),
        default_value=APPROXIMATE_SEARCH[0],
        required=True
    )
    SCRIPT_SCORING_SPACE_TYPE = PropertyDescriptor(
        name="Script Scoring Space Type",
        description="Used to measure the distance between two points in order to determine the k-nearest neighbors.",
        allowable_values=SCRIPT_SCORING_SPACE_TYPE_VALUES.keys(),
        default_value=L2[0],
        required=False,
        dependencies=[PropertyDependency(SEARCH_TYPE, SCRIPT_SCORING_SEARCH[0])]
    )
    PAINLESS_SCRIPTING_SPACE_TYPE = PropertyDescriptor(
        name="Painless Scripting Space Type",
        description="Used to measure the distance between two points in order to determine the k-nearest neighbors.",
        allowable_values=PAINLESS_SCRIPTING_SPACE_TYPE_VALUES.keys(),
        default_value=L2_SQUARED[0],
        required=False,
        dependencies=[PropertyDependency(SEARCH_TYPE, PAINLESS_SCRIPTING_SEARCH[0])]
    )
    BOOLEAN_FILTER = PropertyDescriptor(
        name="Boolean Filter",
        description="A Boolean filter is a post filter consists of a Boolean query that contains a k-NN query and a filter. "
                    "The value of the field must be a JSON representation of the filter.",
        required=False,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        dependencies=[PropertyDependency(SEARCH_TYPE, APPROXIMATE_SEARCH[0])]
    )
    EFFICIENT_FILTER = PropertyDescriptor(
        name="Efficient Filter",
        description="The Lucene Engine or Faiss Engine decides whether to perform an exact k-NN search with "
                    "pre-filtering or an approximate search with modified post-filtering. The value of the field must "
                    "be a JSON representation of the filter.",
        required=False,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        dependencies=[PropertyDependency(SEARCH_TYPE, APPROXIMATE_SEARCH[0])]
    )
    PRE_FILTER = PropertyDescriptor(
        name="Pre Filter",
        description="Script Score query to pre-filter documents before identifying nearest neighbors. The value of "
                    "the field must be a JSON representation of the filter.",
        default_value="{\"match_all\": {}}",
        required=False,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        dependencies=[PropertyDependency(SEARCH_TYPE, SCRIPT_SCORING_SEARCH[0], PAINLESS_SCRIPTING_SEARCH[0])]
    )

    properties = [EMBEDDING_MODEL,
                  OPENAI_API_KEY,
                  OPENAI_API_MODEL,
                  HUGGING_FACE_API_KEY,
                  HUGGING_FACE_MODEL,
                  HTTP_HOST,
                  USERNAME,
                  PASSWORD,
                  INDEX_NAME,
                  QUERY,
                  VECTOR_FIELD,
                  TEXT_FIELD,
                  NUMBER_OF_RESULTS,
                  SEARCH_TYPE,
                  SCRIPT_SCORING_SPACE_TYPE,
                  PAINLESS_SCRIPTING_SPACE_TYPE,
                  BOOLEAN_FILTER,
                  EFFICIENT_FILTER,
                  PRE_FILTER,
                  OUTPUT_STRATEGY,
                  RESULTS_FIELD,
                  INCLUDE_METADATAS,
                  INCLUDE_DISTANCES]

    embeddings = None
    query_utils = None

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.properties

    def onScheduled(self, context):
        # initialize embedding service
        self.embeddings = create_embedding_service(context)
        self.query_utils = QueryUtils(context)

    def transform(self, context, flowfile):
        http_host = context.getProperty(HTTP_HOST).evaluateAttributeExpressions(flowfile).getValue()
        index_name = context.getProperty(INDEX_NAME).evaluateAttributeExpressions(flowfile).getValue()
        query = context.getProperty(self.QUERY).evaluateAttributeExpressions(flowfile).getValue()
        num_results = context.getProperty(self.NUMBER_OF_RESULTS).evaluateAttributeExpressions(flowfile).asInteger()
        vector_field = context.getProperty(VECTOR_FIELD).evaluateAttributeExpressions(flowfile).getValue()
        text_field = context.getProperty(TEXT_FIELD).evaluateAttributeExpressions(flowfile).getValue()
        search_type = context.getProperty(self.SEARCH_TYPE).evaluateAttributeExpressions().getValue()

        params = {"vector_field": vector_field,
                  "text_field": text_field,
                  "search_type": self.SEARCH_TYPE_VALUES.get(search_type)}
        params.update(create_authentication_params(context))

        if search_type == self.APPROXIMATE_SEARCH[0]:
            boolean_filter = context.getProperty(self.BOOLEAN_FILTER).evaluateAttributeExpressions().getValue()
            if boolean_filter is not None:
                params["boolean_filter"] = json.loads(boolean_filter)

            efficient_filter = context.getProperty(self.EFFICIENT_FILTER).evaluateAttributeExpressions().getValue()
            if efficient_filter is not None:
                params["efficient_filter"] = json.loads(efficient_filter)
        else:
            pre_filter = context.getProperty(self.PRE_FILTER).evaluateAttributeExpressions().getValue()
            if pre_filter is not None:
                params["pre_filter"] = json.loads(pre_filter)
            if search_type == self.SCRIPT_SCORING_SEARCH[0]:
                space_type = context.getProperty(self.SCRIPT_SCORING_SPACE_TYPE).evaluateAttributeExpressions().getValue()
                params["space_type"] = self.SCRIPT_SCORING_SPACE_TYPE_VALUES.get(space_type)
            elif search_type == self.PAINLESS_SCRIPTING_SEARCH[0]:
                space_type = context.getProperty(self.PAINLESS_SCRIPTING_SPACE_TYPE).evaluateAttributeExpressions().getValue()
                params["space_type"] = self.PAINLESS_SCRIPTING_SPACE_TYPE_VALUES.get(space_type)

        vectorstore = OpenSearchVectorSearch(index_name=index_name,
                                             embedding_function=self.embeddings,
                                             opensearch_url=http_host,
                                             **params
                                             )

        results = vectorstore.similarity_search_with_score(query=query, k=num_results, **params)

        documents = []
        for result in results:
            documents.append(result[0].page_content)

        if context.getProperty(INCLUDE_METADATAS):
            metadatas = []
            for result in results:
                metadatas.append(result[0].metadata)
        else:
            metadatas = None

        if context.getProperty(INCLUDE_DISTANCES):
            distances = []
            for result in results:
                distances.append(result[1])
        else:
            distances = None

        (output_contents, mime_type) = self.query_utils.create_json(flowfile, documents, metadatas, None, distances, None)
        attributes = {"mime.type": mime_type}

        return FlowFileTransformResult(relationship="success", contents=output_contents, attributes=attributes)
