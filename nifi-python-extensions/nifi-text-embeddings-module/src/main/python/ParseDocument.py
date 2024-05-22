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

import io
import json
from typing import List

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, PropertyDependency

PLAIN_TEXT = "Plain Text"
HTML = "HTML"
MARKDOWN = "Markdown"
PDF = "PDF"
EXCEL = "Microsoft Excel"
POWERPOINT = "Microsoft PowerPoint"
WORD = "Microsoft Word"

PARSING_STRATEGY_AUTO = "Automatic"
PARSING_STRATEGY_HIGH_RES = "High Resolution"
PARSING_STRATEGY_OCR_ONLY = "OCR Only"
PARSING_STRATEGY_FAST = "Fast"

SINGLE_DOCUMENT = "Single Document"
DOCUMENT_PER_ELEMENT = "Document Per Element"

TEXT_KEY = "text"
METADATA_KEY = "metadata"


class ParseDocument(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = '@project.version@'
        description = """Parses incoming unstructured text documents and performs optical character recognition (OCR) in order to extract text from PDF and image files.
            The output is formatted as "json-lines" with two keys: 'text' and 'metadata'.
            Note that use of this Processor may require significant storage space and RAM utilization due to third-party dependencies necessary for processing PDF and image files.
            Also note that in order to process PDF or Images, Tesseract and Poppler must be installed on the system."""
        tags = ["text", "embeddings", "vector", "machine learning", "ML", "artificial intelligence", "ai", "document", "langchain", "pdf", "html", "markdown", "word", "excel", "powerpoint"]
        dependencies = ['pikepdf==8.12.0', 'pypdf==4.0.1', 'langchain==0.1.7', 'unstructured==0.12.4', 'unstructured-inference==0.7.24', 'unstructured_pytesseract==0.3.12', 'pillow-heif==0.15.0',
                        'numpy==1.26.4','opencv-python==4.9.0.80', 'pdf2image==1.17.0', 'pdfminer.six==20221105', 'python-docx==1.1.0', 'openpyxl==3.1.2', 'python-pptx==0.6.23']


    INPUT_FORMAT = PropertyDescriptor(
        name="Input Format",
        description="""The format of the input FlowFile. This dictates which TextLoader will be used to parse the input.
            Note that in order to process images or extract tables from PDF files,you must have both 'poppler' and 'tesseract' installed on your system.""",
        allowable_values=[PLAIN_TEXT, HTML, MARKDOWN, PDF, WORD, EXCEL, POWERPOINT],
        required=True,
        default_value=PLAIN_TEXT
    )
    PDF_PARSING_STRATEGY = PropertyDescriptor(
        name="PDF Parsing Strategy",
        display_name="Parsing Strategy",
        description="Specifies the strategy to use when parsing a PDF",
        allowable_values=[PARSING_STRATEGY_AUTO, PARSING_STRATEGY_HIGH_RES, PARSING_STRATEGY_OCR_ONLY, PARSING_STRATEGY_FAST],
        required=True,
        default_value=PARSING_STRATEGY_AUTO,
        dependencies=[PropertyDependency(INPUT_FORMAT, PDF)]
    )
    PDF_MODEL_NAME = PropertyDescriptor(
        name="PDF Parsing Model",
        description="The model to use for parsing. Different models will have their own strengths and weaknesses.",
        allowable_values=["yolox", "detectron2_onnx", "chipper"],
        required=True,
        default_value="yolox",
        dependencies=[PropertyDependency(INPUT_FORMAT, PDF)]
    )
    ELEMENT_STRATEGY = PropertyDescriptor(
        name="Element Strategy",
        description="Specifies whether the input should be loaded as a single Document, or if each element in the input should be separated out into its own Document",
        allowable_values=[SINGLE_DOCUMENT, DOCUMENT_PER_ELEMENT],
        required=True,
        default_value=DOCUMENT_PER_ELEMENT,
        dependencies=[PropertyDependency(INPUT_FORMAT, HTML, MARKDOWN)]
    )
    INCLUDE_PAGE_BREAKS = PropertyDescriptor(
        name="Include Page Breaks",
        description="Specifies whether or not page breaks should be considered when creating Documents from the input",
        allowable_values=["true", "false"],
        required=True,
        default_value="false",
        dependencies=[PropertyDependency(INPUT_FORMAT, HTML, MARKDOWN),
                      PropertyDependency(ELEMENT_STRATEGY, DOCUMENT_PER_ELEMENT)]
    )
    PDF_INFER_TABLE_STRUCTURE = PropertyDescriptor(
        name="Infer Table Structure",
        description="If true, any table that is identified in the PDF will be parsed and translated into an HTML structure. The HTML of that table will then be added to the \
                    Document's metadata in a key named 'text_as_html'. Regardless of the value of this property, the textual contents of the table will be written to the contents \
                    without the structure.",
        allowable_values=["true", "false"],
        default_value="false",
        required=True,
        dependencies=[PropertyDependency(PDF_PARSING_STRATEGY, PARSING_STRATEGY_HIGH_RES)]
    )
    LANGUAGES = PropertyDescriptor(
        name="Languages",
        description="A comma-separated list of language codes that should be used when using OCR to determine the text.",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value="Eng",
        required=True,
        dependencies=[PropertyDependency(INPUT_FORMAT, PDF)]
    )
    METADATA_FIELDS = PropertyDescriptor(
        name="Metadata Fields",
        description="A comma-separated list of FlowFile attributes that will be added to the Documents' Metadata",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value="filename, uuid",
        required=True
    )
    EXTRACT_METADATA = PropertyDescriptor(
        name="Include Extracted Metadata",
        description="Whether or not to include the metadata that is extracted from the input in each of the Documents",
        allowable_values=["true", "false"],
        default_value="true",
        required=True
    )

    property_descriptors = [INPUT_FORMAT,
                            PDF_PARSING_STRATEGY,
                            PDF_MODEL_NAME,
                            ELEMENT_STRATEGY,
                            INCLUDE_PAGE_BREAKS,
                            PDF_INFER_TABLE_STRUCTURE,
                            LANGUAGES,
                            METADATA_FIELDS,
                            EXTRACT_METADATA]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors


    def get_parsing_strategy(self, nifi_value:str, default_value: str) -> str:
        if nifi_value == PARSING_STRATEGY_OCR_ONLY:
            return "ocr_only"
        if nifi_value == PARSING_STRATEGY_HIGH_RES:
            return "hi_res"
        if nifi_value == PARSING_STRATEGY_FAST:
            return "fast"
        if nifi_value == PARSING_STRATEGY_AUTO:
            return "auto"
        return default_value

    def get_languages(self, nifi_value: str) -> List[str]:
        return [
            lang.strip()
            for lang in nifi_value.split(",")
        ]


    def create_docs(self, context, flowFile):
        from langchain.schema import Document

        metadata = {}

        for attribute_name in context.getProperty(self.METADATA_FIELDS).getValue().split(","):
            trimmed = attribute_name.strip()
            value = flowFile.getAttribute(trimmed)
            metadata[trimmed] = value

        input_format = context.getProperty(self.INPUT_FORMAT).evaluateAttributeExpressions(flowFile).getValue()
        if input_format == PLAIN_TEXT:
            return [Document(page_content=flowFile.getContentsAsBytes().decode('utf-8'), metadata=metadata)]

        element_strategy = context.getProperty(self.ELEMENT_STRATEGY).getValue()
        if element_strategy == SINGLE_DOCUMENT:
            mode = "single"
        else:
            mode = "elements"

        include_page_breaks = context.getProperty(self.INCLUDE_PAGE_BREAKS).asBoolean()
        include_metadata = context.getProperty(self.EXTRACT_METADATA).asBoolean()

        if input_format == HTML:
            from langchain.document_loaders import UnstructuredHTMLLoader
            loader = UnstructuredHTMLLoader(None, file=io.BytesIO(flowFile.getContentsAsBytes()), mode=mode, include_page_breaks=include_page_breaks, include_metadata=include_metadata)

        elif input_format == PDF:
            from langchain.document_loaders import UnstructuredPDFLoader

            infer_table_structure = context.getProperty(self.PDF_INFER_TABLE_STRUCTURE).asBoolean()
            strategy = self.get_parsing_strategy(context.getProperty(self.PDF_PARSING_STRATEGY).getValue(), PARSING_STRATEGY_AUTO)
            languages = self.get_languages(context.getProperty(self.LANGUAGES).getValue())
            model_name = context.getProperty(self.PDF_MODEL_NAME).getValue()

            loader = UnstructuredPDFLoader(None, file=io.BytesIO(flowFile.getContentsAsBytes()), mode=mode, infer_table_structure=infer_table_structure,
                                           include_page_breaks=include_page_breaks, languages=languages, strategy=strategy, include_metadata=include_metadata, model_name=model_name)

        elif input_format == MARKDOWN:
            from langchain.document_loaders import UnstructuredMarkdownLoader
            loader = UnstructuredMarkdownLoader(None, file=io.BytesIO(flowFile.getContentsAsBytes()), mode=mode, include_page_breaks=include_page_breaks, include_metadata=include_metadata)

        elif input_format == WORD:
            from langchain.document_loaders import UnstructuredWordDocumentLoader
            loader = UnstructuredWordDocumentLoader(None, file=io.BytesIO(flowFile.getContentsAsBytes()), mode=mode, include_page_breaks=include_page_breaks, include_metadata=include_metadata)

        elif input_format == EXCEL:
            from langchain.document_loaders import UnstructuredExcelLoader
            loader = UnstructuredExcelLoader(None, file=io.BytesIO(flowFile.getContentsAsBytes()), mode=mode, include_page_breaks=include_page_breaks, include_metadata=include_metadata)

        elif input_format == POWERPOINT:
            from langchain.document_loaders import UnstructuredPowerPointLoader
            loader = UnstructuredPowerPointLoader(None, file=io.BytesIO(flowFile.getContentsAsBytes()), mode=mode, include_page_breaks=include_page_breaks, include_metadata=include_metadata)

        else:
            raise ValueError("Configured Input Format is invalid: " + input_format)

        documents = loader.load()

        if len(metadata) > 0:
            for doc in documents:
                if doc.metadata is None:
                    doc.metadata = metadata
                else:
                    doc.metadata.update(metadata)

        return documents



    def to_json(self, docs) -> str:
        json_docs = []

        i = 0
        for doc in docs:
            doc.metadata['chunk_index'] = i
            doc.metadata['chunk_count'] = len(docs)
            i += 1

            json_doc = json.dumps({
                "text": doc.page_content,
                "metadata": doc.metadata
            })
            json_docs.append(json_doc)

        return "\n".join(json_docs)


    def transform(self, context, flowFile):
        documents = self.create_docs(context, flowFile)
        output_json = self.to_json(documents)

        return FlowFileTransformResult("success", contents=output_json, attributes={"mime.type": "application/json"})
