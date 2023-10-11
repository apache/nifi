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

import cv2
import numpy as np
import json
from nifiapi.properties import PropertyDescriptor
from nifiapi.properties import ResourceDefinition
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

SCALE_FACTOR = 0.00392
NMS_THRESHOLD = 0.4     # non-maximum suppression threshold
CONFIDENCE_THRESHOLD = 0.5

class DetectObjectInImage(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        dependencies = ['numpy >= 1.23.5', 'opencv-python >= 4.6']

    def __init__(self, jvm=None, **kwargs):
        self.jvm = jvm

        # Build Property Descriptors
        self.model_file = PropertyDescriptor(
            name = 'Model File',
            description = 'The binary file containing the trained Deep Neural Network weights. Supports Caffe (*.caffemodel), TensorFlow (*.pb), Torch (*.t7, *.net), Darknet (*.weights), ' +
                          'DLDT (*.bin), and ONNX (*.onnx)',
            required = True,
            resource_definition = ResourceDefinition(allow_file = True)
        )
        self.config_file = PropertyDescriptor(
            name = 'Network Config File',
            description = 'The text file containing the Network configuration. Supports Caffe (*.prototxt), TensorFlow (*.pbtxt), Darknet (*.cfg), and DLDT (*.xml)',
            required = False,
            resource_definition = ResourceDefinition(allow_file = True)
        )
        self.class_name_file = PropertyDescriptor(
            name = 'Class Names File',
            description = 'A text file containing the names of the classes that may be detected by the model. Expected format is one class name per line, new-line terminated."',
            required = True,
            resource_definition = ResourceDefinition(allow_file = True)
        )
        self.descriptors = [self.model_file, self.config_file, self.class_name_file]

    def getPropertyDescriptors(self):
        return self.descriptors

    def onScheduled(self, context):
        # read class names from text file
        class_name_file = context.getProperty(self.class_name_file.name).getValue()
        if class_name_file is None:
            self.classes = []
        else:
            with open(class_name_file, 'r') as file:
                self.classes = [line.strip() for line in file.readlines()]

        # read pre-trained model and config file
        model_file = context.getProperty(self.model_file.name).getValue()
        config_file = context.getProperty(self.config_file.name).getValue()
        if config_file is None:
            config_file = ""
        self.net = cv2.dnn.readNet(model_file, config_file)

        # Determine the neural net's output layers, so that they can be used later
        self.output_layers = []
        layer_names = self.net.getLayerNames()
        for out_layer in self.net.getUnconnectedOutLayers():
            layer_name = layer_names[out_layer - 1]
            self.output_layers.append(layer_name)


    def transform(self, context, flowFile):
        # Read FlowFile contents into an image
        image_bytes = flowFile.getContentsAsBytes()
        image_np_array = np.fromstring(image_bytes, np.uint8)
        image = cv2.imdecode(image_np_array, cv2.IMREAD_COLOR)

        # Create a blob from the image. I.e., perform pre-processing steps
        blob = cv2.dnn.blobFromImage(image, SCALE_FACTOR, size=(416, 416), swapRB=True)

        # Perform the inference and get the predictions
        self.net.setInput(blob)
        predictions = self.net.forward(self.output_layers)

        class_ids = []
        confidences = []
        boxes = []

        img_height = image.shape[0]
        img_width = image.shape[1]
        for prediction in predictions:
            for detection in prediction:
                scores = detection[5:]
                class_id = np.argmax(scores)
                confidence = scores[class_id]

                center_x = int(detection[0] * img_width)
                center_y = int(detection[1] * img_height)
                width = int(detection[2] * img_width)
                height = int(detection[3] * img_height)
                x = center_x - width / 2
                y = center_y - height / 2

                class_ids.append(class_id)
                confidences.append(float(confidence))
                boxes.append([x, y, width, height])

        # Apply non-max suppression. This eliminates multiple overlapping boxes/predictions for the same object
        indices = cv2.dnn.NMSBoxes(boxes, confidences, CONFIDENCE_THRESHOLD, NMS_THRESHOLD)

        # For each index, get the name of the object that was detected and the bounding box. Create a map of this.
        # We will then output this mapping as JSON.
        detection_results = []
        for i in indices:
            box = boxes[i]
            class_id = class_ids[i]

            if self.classes is not None and len(self.classes) > class_id:
                class_name = self.classes[class_ids[i]]
            else:
                class_name = 'Class_' + str(class_id)

            detection_details = {
                'class': class_name,
                'confidence': confidences[i],
                'x': box[0],
                'y': box[1],
                'width': box[2],
                'height': box[3]
            }
            detection_results.append(detection_details)

        output = {'results': detection_results}
        output_json = json.dumps(output)

        attributes = {"mime.type": "application/json"}

        return FlowFileTransformResult(relationship = "success", attributes = attributes, contents = str.encode(output_json))
