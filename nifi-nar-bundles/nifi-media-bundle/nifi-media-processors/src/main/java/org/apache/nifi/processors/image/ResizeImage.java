/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.image;

import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.Transparency;
import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

@EventDriven
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({ "resize", "image", "jpg", "jpeg", "png", "bmp", "wbmp", "gif" })
@CapabilityDescription("Resizes an image to user-specified dimensions. This Processor uses the image codecs registered with the "
    + "environment that NiFi is running in. By default, this includes JPEG, PNG, BMP, WBMP, and GIF images.")
public class ResizeImage extends AbstractProcessor {
    static final AllowableValue RESIZE_DEFAULT = new AllowableValue("Default", "Default", "Use the default algorithm");
    static final AllowableValue RESIZE_FAST = new AllowableValue("Scale Fast", "Scale Fast", "Emphasize speed of the scaling over smoothness");
    static final AllowableValue RESIZE_SMOOTH = new AllowableValue("Scale Smooth", "Scale Smooth", "Emphasize smoothness of the scaling over speed");
    static final AllowableValue RESIZE_REPLICATE = new AllowableValue("Replicate Scale Filter", "Replicate Scale Filter", "Use the Replicate Scale Filter algorithm");
    static final AllowableValue RESIZE_AREA_AVERAGING = new AllowableValue("Area Averaging", "Area Averaging", "Use the Area Averaging scaling algorithm");

    static final PropertyDescriptor IMAGE_WIDTH = new PropertyDescriptor.Builder()
        .name("Image Width (in pixels)")
        .description("The desired number of pixels for the image's width")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();
    static final PropertyDescriptor IMAGE_HEIGHT = new PropertyDescriptor.Builder()
        .name("Image Height (in pixels)")
        .description("The desired number of pixels for the image's height")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();
    static final PropertyDescriptor SCALING_ALGORITHM = new PropertyDescriptor.Builder()
        .name("Scaling Algorithm")
        .description("Specifies which algorithm should be used to resize the image")
        .required(true)
        .allowableValues(RESIZE_DEFAULT, RESIZE_FAST, RESIZE_SMOOTH, RESIZE_REPLICATE, RESIZE_AREA_AVERAGING)
        .defaultValue(RESIZE_DEFAULT.getValue())
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("A FlowFile is routed to this relationship if it is successfully resized")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("A FlowFile is routed to this relationship if it is not in the specified format")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(IMAGE_WIDTH);
        properties.add(IMAGE_HEIGHT);
        properties.add(SCALING_ALGORITHM);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final int width, height;
        try {
            width = context.getProperty(IMAGE_WIDTH).evaluateAttributeExpressions(flowFile).asInteger();
            height = context.getProperty(IMAGE_HEIGHT).evaluateAttributeExpressions(flowFile).asInteger();
        } catch (final NumberFormatException nfe) {
            getLogger().error("Failed to resize {} due to {}", new Object[] { flowFile, nfe });
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final String algorithm = context.getProperty(SCALING_ALGORITHM).getValue();
        final int hints;
        if (algorithm.equalsIgnoreCase(RESIZE_DEFAULT.getValue())) {
            hints = Image.SCALE_DEFAULT;
        } else if (algorithm.equalsIgnoreCase(RESIZE_FAST.getValue())) {
            hints = Image.SCALE_FAST;
        } else if (algorithm.equalsIgnoreCase(RESIZE_SMOOTH.getValue())) {
            hints = Image.SCALE_SMOOTH;
        } else if (algorithm.equalsIgnoreCase(RESIZE_REPLICATE.getValue())) {
            hints = Image.SCALE_REPLICATE;
        } else if (algorithm.equalsIgnoreCase(RESIZE_AREA_AVERAGING.getValue())) {
            hints = Image.SCALE_AREA_AVERAGING;
        } else {
            throw new AssertionError("Invalid Scaling Algorithm: " + algorithm);
        }

        final StopWatch stopWatch = new StopWatch(true);
        try {
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(final InputStream rawIn, final OutputStream out) throws IOException {
                    try (final BufferedInputStream in = new BufferedInputStream(rawIn)) {
                        final ImageInputStream iis = ImageIO.createImageInputStream(in);
                        if (iis == null) {
                            throw new ProcessException("FlowFile is not in a valid format");
                        }

                        final Iterator<ImageReader> readers = ImageIO.getImageReaders(iis);
                        if (!readers.hasNext()) {
                            throw new ProcessException("FlowFile is not in a valid format");
                        }

                        final ImageReader reader = readers.next();
                        final String formatName = reader.getFormatName();
                        reader.setInput(iis, true);
                        final BufferedImage image = reader.read(0);

                        final Image scaledImage = image.getScaledInstance(width, height, hints);
                        final BufferedImage scaledBufferedImg;
                        if (scaledImage instanceof BufferedImage) {
                            scaledBufferedImg = (BufferedImage) scaledImage;
                        } else {
                            // Determine image type, since calling image.getType may return 0
                            int imageType = BufferedImage.TYPE_INT_ARGB;
                            if(image.getTransparency() == Transparency.OPAQUE) {
                                imageType = BufferedImage.TYPE_INT_RGB;
                            }

                            scaledBufferedImg = new BufferedImage(scaledImage.getWidth(null), scaledImage.getHeight(null), imageType);
                            final Graphics2D graphics = scaledBufferedImg.createGraphics();
                            try {
                                graphics.drawImage(scaledImage, 0, 0, null);
                            } finally {
                                graphics.dispose();
                            }
                        }

                        ImageIO.write(scaledBufferedImg, formatName, out);
                    }
                }
            });

            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final ProcessException pe) {
            getLogger().error("Failed to resize {} due to {}", new Object[] { flowFile, pe });
            session.transfer(flowFile, REL_FAILURE);
        }
    }

}
