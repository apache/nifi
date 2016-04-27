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

package org.apache.nifi.processors.ocr;

import net.sourceforge.tess4j.ITesseract;
import net.sourceforge.tess4j.Tesseract;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

@Tags({"ocr", "tesseract", "image", "text"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Provides the ability to perform OCR on the incoming images.")
public class TesseractOCRProcessor extends AbstractProcessor {

    //Languages supported by Tesseract
    public static String[] SUPPORTED_LANGUAGES;
    public static String[] PAGE_SEGMENTATION_MODES;

    static {
        SUPPORTED_LANGUAGES = new String[100];
        SUPPORTED_LANGUAGES[0] = "afr";
        SUPPORTED_LANGUAGES[1] = "amh";
        SUPPORTED_LANGUAGES[2] = "ara";
        SUPPORTED_LANGUAGES[3] = "asm";
        SUPPORTED_LANGUAGES[4] = "aze";
        SUPPORTED_LANGUAGES[5] = "bel";
        SUPPORTED_LANGUAGES[6] = "ben";
        SUPPORTED_LANGUAGES[7] = "bod";
        SUPPORTED_LANGUAGES[8] = "bos";
        SUPPORTED_LANGUAGES[9] = "bul";
        SUPPORTED_LANGUAGES[10] = "cat";
        SUPPORTED_LANGUAGES[11] = "ceb";
        SUPPORTED_LANGUAGES[12] = "ces";
        SUPPORTED_LANGUAGES[13] = "chi_sim";
        SUPPORTED_LANGUAGES[14] = "chi_tra";
        SUPPORTED_LANGUAGES[15] = "chr";
        SUPPORTED_LANGUAGES[16] = "cym";
        SUPPORTED_LANGUAGES[17] = "dan";
        SUPPORTED_LANGUAGES[18] = "dan_frak";
        SUPPORTED_LANGUAGES[19] = "deu";
        SUPPORTED_LANGUAGES[20] = "deu_frak";
        SUPPORTED_LANGUAGES[21] = "dzo";
        SUPPORTED_LANGUAGES[22] = "ell";
        SUPPORTED_LANGUAGES[23] = "eng";
        SUPPORTED_LANGUAGES[24] = "enm";
        SUPPORTED_LANGUAGES[25] = "epo";
        SUPPORTED_LANGUAGES[26] = "equ";
        SUPPORTED_LANGUAGES[27] = "est";
        SUPPORTED_LANGUAGES[28] = "eus";
        SUPPORTED_LANGUAGES[29] = "fas";
        SUPPORTED_LANGUAGES[30] = "fin";
        SUPPORTED_LANGUAGES[31] = "fra";
        SUPPORTED_LANGUAGES[32] = "frk";
        SUPPORTED_LANGUAGES[33] = "frm";
        SUPPORTED_LANGUAGES[34] = "gle";
        SUPPORTED_LANGUAGES[35] = "glg";
        SUPPORTED_LANGUAGES[36] = "grc";
        SUPPORTED_LANGUAGES[37] = "guj";
        SUPPORTED_LANGUAGES[38] = "hat";
        SUPPORTED_LANGUAGES[39] = "heb";
        SUPPORTED_LANGUAGES[40] = "hin";
        SUPPORTED_LANGUAGES[41] = "hrv";
        SUPPORTED_LANGUAGES[42] = "hun";
        SUPPORTED_LANGUAGES[43] = "iku";
        SUPPORTED_LANGUAGES[44] = "ind";
        SUPPORTED_LANGUAGES[45] = "isl";
        SUPPORTED_LANGUAGES[46] = "ita";
        SUPPORTED_LANGUAGES[47] = "jav";
        SUPPORTED_LANGUAGES[48] = "jpn";
        SUPPORTED_LANGUAGES[49] = "kan";
        SUPPORTED_LANGUAGES[50] = "kat";
        SUPPORTED_LANGUAGES[51] = "kaz";
        SUPPORTED_LANGUAGES[52] = "khm";
        SUPPORTED_LANGUAGES[53] = "kir";
        SUPPORTED_LANGUAGES[54] = "kor";
        SUPPORTED_LANGUAGES[55] = "kur";
        SUPPORTED_LANGUAGES[56] = "lao";
        SUPPORTED_LANGUAGES[57] = "lat";
        SUPPORTED_LANGUAGES[58] = "lav";
        SUPPORTED_LANGUAGES[59] = "lit";
        SUPPORTED_LANGUAGES[60] = "mal";
        SUPPORTED_LANGUAGES[61] = "mar";
        SUPPORTED_LANGUAGES[62] = "mkd";
        SUPPORTED_LANGUAGES[63] = "mlt";
        SUPPORTED_LANGUAGES[64] = "msa";
        SUPPORTED_LANGUAGES[65] = "mya";
        SUPPORTED_LANGUAGES[66] = "nep";
        SUPPORTED_LANGUAGES[67] = "nld";
        SUPPORTED_LANGUAGES[68] = "nor";
        SUPPORTED_LANGUAGES[69] = "ori";
        SUPPORTED_LANGUAGES[70] = "osd";
        SUPPORTED_LANGUAGES[71] = "pan";
        SUPPORTED_LANGUAGES[72] = "pol";
        SUPPORTED_LANGUAGES[73] = "por";
        SUPPORTED_LANGUAGES[74] = "pus";
        SUPPORTED_LANGUAGES[75] = "ron";
        SUPPORTED_LANGUAGES[76] = "rus";
        SUPPORTED_LANGUAGES[77] = "san";
        SUPPORTED_LANGUAGES[78] = "sin";
        SUPPORTED_LANGUAGES[79] = "slk";
        SUPPORTED_LANGUAGES[80] = "slk_frak";
        SUPPORTED_LANGUAGES[81] = "slv";
        SUPPORTED_LANGUAGES[82] = "spa";
        SUPPORTED_LANGUAGES[83] = "sqi";
        SUPPORTED_LANGUAGES[84] = "srp";
        SUPPORTED_LANGUAGES[84] = "swa";
        SUPPORTED_LANGUAGES[85] = "swe";
        SUPPORTED_LANGUAGES[86] = "syr";
        SUPPORTED_LANGUAGES[87] = "tam";
        SUPPORTED_LANGUAGES[88] = "tel";
        SUPPORTED_LANGUAGES[89] = "tgk";
        SUPPORTED_LANGUAGES[90] = "tgl";
        SUPPORTED_LANGUAGES[91] = "tha";
        SUPPORTED_LANGUAGES[92] = "tir";
        SUPPORTED_LANGUAGES[93] = "tur";
        SUPPORTED_LANGUAGES[94] = "uig";
        SUPPORTED_LANGUAGES[95] = "ukr";
        SUPPORTED_LANGUAGES[96] = "urd";
        SUPPORTED_LANGUAGES[97] = "uzb";
        SUPPORTED_LANGUAGES[98] = "vie";
        SUPPORTED_LANGUAGES[99] = "yid";

        PAGE_SEGMENTATION_MODES = new String[11];
        PAGE_SEGMENTATION_MODES[0] = "0 = Orientation and script detection (OSD) only";
        PAGE_SEGMENTATION_MODES[1] = "1 = Automatic page segmentation with OSD";
        PAGE_SEGMENTATION_MODES[2] = "2 = Automatic page segmentation, but no OSD, or OCR";
        PAGE_SEGMENTATION_MODES[3] = "3 = Fully automatic page segmentation, but no OSD";
        PAGE_SEGMENTATION_MODES[4] = "4 = Assume a single column of text of variable sizes";
        PAGE_SEGMENTATION_MODES[5] = "5 = Assume a single uniform block of vertically aligned text";
        PAGE_SEGMENTATION_MODES[6] = "6 = Assume a single uniform block of text";
        PAGE_SEGMENTATION_MODES[7] = "7 = Treat the image as a single text line";
        PAGE_SEGMENTATION_MODES[8] = "8 = Treat the image as a single word";
        PAGE_SEGMENTATION_MODES[9] = "9 = Treat the image as a single word in a circle";
        PAGE_SEGMENTATION_MODES[10] = "10 = Treat the image as a single character";
    }

    public static final PropertyDescriptor TESSERACT_INSTALL_PATH = new PropertyDescriptor
            .Builder().name("Tesseract Installation Directory")
            .description("Base location on the local filesystem where Tesseract is installed")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("/usr/share/tesseract-ocr/tessdata/")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TESSERACT_LANGUAGE = new PropertyDescriptor
            .Builder().name("Tesseract Language")
            .description("Language that Tesseract will use to perform OCR on image coming in the incoming FlowFile's content")
            .required(true)
            .defaultValue("eng")
            .allowableValues(SUPPORTED_LANGUAGES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TESSERACT_PAGE_SEG_MODE = new PropertyDescriptor
            .Builder().name("Tesseract Page Segmentation Mode")
            .description("Set Tesseract to only run a subset of layout analysis and assume a certain form of image.")
            .required(true)
            .defaultValue(PAGE_SEGMENTATION_MODES[3])
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TESSERACT_MIN_FILE_SIZE = new PropertyDescriptor
            .Builder().name("Tesseract Minimum FileSize")
            .description("Minimum file size to submit file to OCR")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("0")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor TESSERACT_MAX_FILE_SIZE = new PropertyDescriptor
            .Builder().name("Tesseract Maximum FileSize")
            .description("Maximum file size to submit file to OCR")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue(new Integer(Integer.MAX_VALUE).toString())
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor TESSERACT_TIMEOUT = new PropertyDescriptor
            .Builder().name("Tesseract Installation Directory")
            .description("Base location on the local filesystem where Tesseract is installed")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("/usr/share/tesseract-ocr/tessdata/")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TESSERACT_CONFIGS = new PropertyDescriptor
            .Builder().name("Tesseract Installation Directory")
            .description("Base location on the local filesystem where Tesseract is installed")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("/usr/share/tesseract-ocr/tessdata/")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("successfully completed OCR on image")
            .build();

    public static final Relationship REL_UNSUPPORTED_IMAGE_FORMAT = new Relationship.Builder()
            .name("unsupported image format")
            .description("The image format in the FlowFile content is not supported by Tesseract")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original image that OCR was performed on")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to attempt OCR on input image")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(TESSERACT_INSTALL_PATH);
        descriptors.add(TESSERACT_LANGUAGE);
        descriptors.add(TESSERACT_PAGE_SEG_MODE);
        descriptors.add(TESSERACT_MIN_FILE_SIZE);
        descriptors.add(TESSERACT_MAX_FILE_SIZE);
        descriptors.add(TESSERACT_TIMEOUT);
        descriptors.add(TESSERACT_CONFIGS);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_UNSUPPORTED_IMAGE_FORMAT);
        relationships.add(REL_ORIGINAL);
        this.relationships = Collections.unmodifiableSet(relationships);

    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        //Reads in the image data and runs it through Tesseract for OCR
        FlowFile ff = session.write(flowFile, new StreamCallback() {
            @Override
            public void process(InputStream inputStream, OutputStream outputStream) throws IOException {
                ITesseract instance = new Tesseract();
                instance.setLanguage(context.getProperty(TESSERACT_LANGUAGE).getValue());
                instance.setDatapath(context.getProperty(TESSERACT_INSTALL_PATH).evaluateAttributeExpressions(flowFile).getValue());
                instance.setPageSegMode(Integer.parseInt(context.getProperty(TESSERACT_PAGE_SEG_MODE).getValue()));

                try {
                    BufferedImage imBuff = ImageIO.read(inputStream);
                    outputStream.write(instance.doOCR(imBuff).getBytes());
                } catch (Exception ex) {
                    getLogger().error(ex.getMessage());
                    session.transfer(flowFile, REL_FAILURE);
                }
            }
        });

        session.transfer(ff, REL_SUCCESS);
    }
}