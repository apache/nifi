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

import json

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope
import ChromaUtils
import EmbeddingUtils
import QueryUtils


class QueryChroma(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '@project.version@'
        description = "Queries a Chroma Vector Database in order to gather a specified number of documents that are most closely related to the given query."
        tags = ["chroma", "vector", "vectordb", "embeddings", "enrich", "enrichment", "ai", "artificial intelligence", "ml", "machine learning", "text", "LLM"]



    QUERY = PropertyDescriptor(
        name="Query",
        description="The query to issue to the Chroma VectorDB. The query is always converted into embeddings using the configured embedding function, and the embedding is " +
                    "then sent to Chroma. The text itself is not sent to Chroma.",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )
    NUMBER_OF_RESULTS = PropertyDescriptor(
        name="Number of Results",
        description="The number of results to return from Chroma",
        required=True,
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value="10",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )
    METADATA_FILTER = PropertyDescriptor(
        name="Metadata Filter",
        description="A JSON representation of a Metadata Filter that can be applied against the Chroma documents in order to narrow down the documents that can be returned. " +
                    "For example: { \"metadata_field\": \"some_value\" }",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False
    )
    DOCUMENT_FILTER = PropertyDescriptor(
        name="Document Filter",
        description="A JSON representation of a Document Filter that can be applied against the Chroma documents' text in order to narrow down the documents that can be returned. " +
                    "For example: { \"$contains\": \"search_string\" }",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False
    )

    client = None
    embedding_function = None
    include_ids = None
    include_metadatas = None
    include_documents = None
    include_distances = None
    include_embeddings = None
    results_field = None

    property_descriptors = [prop for prop in ChromaUtils.PROPERTIES] + [prop for prop in EmbeddingUtils.PROPERTIES] + [
        QUERY,
        NUMBER_OF_RESULTS,
        QueryUtils.OUTPUT_STRATEGY,
        QueryUtils.RESULTS_FIELD,
        METADATA_FILTER,
        DOCUMENT_FILTER,
        QueryUtils.INCLUDE_IDS,
        QueryUtils.INCLUDE_METADATAS,
        QueryUtils.INCLUDE_DOCUMENTS,
        QueryUtils.INCLUDE_DISTANCES,
        QueryUtils.INCLUDE_EMBEDDINGS]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors


    def onScheduled(self, context):
        self.client = ChromaUtils.create_client(context)
        self.embedding_function = EmbeddingUtils.create_embedding_function(context)
        self.include_ids = context.getProperty(QueryUtils.INCLUDE_IDS).asBoolean()
        self.include_metadatas = context.getProperty(QueryUtils.INCLUDE_METADATAS).asBoolean()
        self.include_documents = context.getProperty(QueryUtils.INCLUDE_DOCUMENTS).asBoolean()
        self.include_distances = context.getProperty(QueryUtils.INCLUDE_DISTANCES).asBoolean()
        self.include_embeddings = context.getProperty(QueryUtils.INCLUDE_EMBEDDINGS).asBoolean()
        self.results_field = context.getProperty(QueryUtils.RESULTS_FIELD).getValue()
        self.query_utils = QueryUtils.QueryUtils(context)

    def transform(self, context, flowfile):
        client = self.client
        embedding_function = self.embedding_function
        collection_name = context.getProperty(ChromaUtils.COLLECTION_NAME).evaluateAttributeExpressions(flowfile).getValue()

        collection = client.get_collection(
            name=collection_name,
            embedding_function=embedding_function)

        query_text = context.getProperty(self.QUERY).evaluateAttributeExpressions(flowfile).getValue()
        embeddings = embedding_function([query_text])

        included_fields = []
        if self.include_distances:
            included_fields.append('distances')
        if self.include_documents:
            included_fields.append('documents')
        if self.include_embeddings:
            included_fields.append('embeddings')
        if self.include_metadatas:
            included_fields.append('metadatas')

        where = None
        where_clause = context.getProperty(self.METADATA_FILTER).evaluateAttributeExpressions(flowfile).getValue()
        if where_clause is not None:
            where = json.loads(where_clause)

        where_document = None
        where_document_clause = context.getProperty(self.DOCUMENT_FILTER).evaluateAttributeExpressions(flowfile).getValue()
        if where_document_clause is not None:
            where_document = json.loads(where_document_clause)

        query_results = collection.query(
            query_embeddings=embeddings,
            n_results=context.getProperty(self.NUMBER_OF_RESULTS).evaluateAttributeExpressions(flowfile).asInteger(),
            include=included_fields,
            where_document=where_document,
            where=where
        )

        ids = query_results['ids'][0]
        distances = None if (not self.include_distances or query_results['distances'] is None) else query_results['distances'][0]
        metadatas = None if (not self.include_metadatas or query_results['metadatas'] is None) else query_results['metadatas'][0]
        documents = None if (not self.include_documents or query_results['documents'] is None) else query_results['documents'][0]
        embeddings = None if (not self.include_embeddings or query_results['embeddings'] is None) else query_results['embeddings'][0]

        (output_contents, mime_type) = self.query_utils.create_json(flowfile, documents, metadatas, embeddings, distances, ids)

        # Return the results
        attributes = {"mime.type": mime_type}
        return FlowFileTransformResult(relationship = "success", contents=output_contents, attributes=attributes)
