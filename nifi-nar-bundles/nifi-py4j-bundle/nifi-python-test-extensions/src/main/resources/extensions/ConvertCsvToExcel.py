import pandas as pd
import io
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

class ConvertCsvToExcel(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        dependencies = ['pandas', 'xlsxwriter']
        version = '0.0.1-SNAPSHOT'
        description = 'Converts a CSV file into a Microsoft Excel file'
        tags = ['csv', 'excel']

    def __init__(self, **kwargs):
        pass

    def transform(self, context, flowFile):
        csv_df = pd.read_csv(io.BytesIO(flowFile.getContentsAsBytes()))
        bytesIo = io.BytesIO()
        writer = pd.ExcelWriter(bytesIo, engine='xlsxwriter')
        csv_df.to_excel(writer, index=False)
        writer.close()

        return FlowFileTransformResult(relationship = "success", contents = bytesIo.getvalue())


    def getPropertyDescriptors(self):
        return []