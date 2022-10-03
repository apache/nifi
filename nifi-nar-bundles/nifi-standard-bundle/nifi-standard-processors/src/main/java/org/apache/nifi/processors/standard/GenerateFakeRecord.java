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
package org.apache.nifi.processors.standard;

import com.github.javafaker.Faker;
import org.apache.avro.Schema;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.avro.AvroSchemaValidator;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.DecimalDataType;
import org.apache.nifi.serialization.record.type.EnumDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.util.StringUtils;

import java.math.BigInteger;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@SupportsBatching
@Tags({"test", "random", "generate", "fake"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer"),
        @WritesAttribute(attribute = "record.count", description = "The number of records in the FlowFile"),
})
@CapabilityDescription("This processor creates FlowFiles with records having random value for the specified fields. GenerateFakeRecord is useful " +
        "for testing, configuration, and simulation. It uses either user-defined properties to define a record schema or a provided schema and generates the specified number of records using " +
        "random data for the fields in the schema.")
public class GenerateFakeRecord extends AbstractProcessor {

    static final AllowableValue FT_ADDRESS = new AllowableValue("Address", "Address", "A full address including street, city, state, etc.");
    static final AllowableValue FT_AIRCRAFT = new AllowableValue("Aircraft", "Aircraft", "The name of a type of aircraft");
    static final AllowableValue FT_AIRPORT = new AllowableValue("Airport", "Airport", "The code for an airport (RJAF, e.g.)");
    static final AllowableValue FT_ANCIENT_GOD = new AllowableValue("Ancient God", "Ancient God", "The name of a god from ancient mythology");
    static final AllowableValue FT_ANCIENT_HERO = new AllowableValue("Ancient Hero", "Ancient Hero", "The name of a hero from ancient mythology");
    static final AllowableValue FT_ANCIENT_PRIMORDIAL = new AllowableValue("Ancient Primordial", "Primordial", "The name of a primordial from ancient mythology");
    static final AllowableValue FT_ANCIENT_TITAN = new AllowableValue("Ancient Titan", "Ancient Titan", "The name of a titan from ancient mythology");
    static final AllowableValue FT_ANIMAL = new AllowableValue("Animal", "Animal", "The name of an animal");
    static final AllowableValue FT_APP_AUTHOR = new AllowableValue("App Author", "App Author", "The name of an author of an application");
    static final AllowableValue FT_APP_NAME = new AllowableValue("App Name", "App Name", "The name of an application");
    static final AllowableValue FT_APP_VERSION = new AllowableValue("App Version", "App Version", "The version of an application");
    static final AllowableValue FT_ARTIST = new AllowableValue("Artist", "Artist", "The name of the artist");
    static final AllowableValue FT_AVATAR = new AllowableValue("Avatar URL", "Avatar URL", "The URL of a Twitter avatar");
    static final AllowableValue FT_BEER_NAME = new AllowableValue("Beer Name", "Beer Name", "The name of a beer");
    static final AllowableValue FT_BEER_STYLE = new AllowableValue("Beer Style", "Beer Style", "The style of a beer (Light Lager, e.g.)");
    static final AllowableValue FT_BEER_HOP = new AllowableValue("Beer Hop", "Beer Hop", "A hop used in making beer (Bitter Gold, e.g.)");
    static final AllowableValue FT_BEER_YEAST = new AllowableValue("Beer Yeast", "Beer Yeast", "A yeast used in making beer (3333 - German Wheat, e.g.) ");
    static final AllowableValue FT_BEER_MALT = new AllowableValue("Beer Malt", "Beer Malt", "A malt used in making beer (Victory, e.g.)");
    static final AllowableValue FT_BIRTHDAY = new AllowableValue("Birthday", "Birthday", "Generates a random birthday between 65 and 18 years ago");
    static final AllowableValue FT_BOOK_AUTHOR = new AllowableValue("Book Author", "Book Author", "The author of a book");
    static final AllowableValue FT_BOOK_TITLE = new AllowableValue("Book Title", "Book Title", "The title of a book");
    static final AllowableValue FT_BOOK_PUBLISHER = new AllowableValue("Book Publisher", "Book Publisher", "The publisher of a book");
    static final AllowableValue FT_BOOK_GENRE = new AllowableValue("Book Genre", "Book Genre", "The genre of a book");
    static final AllowableValue FT_BOOL = new AllowableValue("Boolean (true/false)", "Boolean (true/false)", "A value of 'true' or 'false'");
    static final AllowableValue FT_BIC = new AllowableValue("Business Identifier Code (BIC)", "Business Identifier Code (BIC)", "A Business Identifier Code (BIC)");
    static final AllowableValue FT_BUILDING_NUMBER = new AllowableValue("Building Number", "Building Number", "The number of a building in an address");
    static final AllowableValue FT_CHUCK_NORRIS_FACT = new AllowableValue("Chuck Norris Fact", "Chuck Norris Fact", "A fact about Chuck Norris");
    static final AllowableValue FT_CAT_NAME = new AllowableValue("Cat Name", "Cat Name", "The name of a cat");
    static final AllowableValue FT_CAT_BREED = new AllowableValue("Cat Breed", "Cat Breed", "The breed of a cat");
    static final AllowableValue FT_CAT_REGISTRY = new AllowableValue("Cat Registry", "Cat Registry", "The registry to which a cat my belong");
    static final AllowableValue FT_CITY = new AllowableValue("City", "City", "The name of a city");
    static final AllowableValue FT_COLOR = new AllowableValue("Color", "Color", "The name of a color");
    static final AllowableValue FT_COMPANY_NAME = new AllowableValue("Company Name", "Company Name", "The name of a company");
    static final AllowableValue FT_CONSTELLATION = new AllowableValue("Constellation", "Constellation", "The name of a constellation in the galaxy");
    static final AllowableValue FT_COUNTRY_CAPITOL = new AllowableValue("Country Capitol", "Country Capitol", "The name of a capitol of a country");
    static final AllowableValue FT_COUNTRY = new AllowableValue("Country", "Country", "The name of a country");
    static final AllowableValue FT_COUNTRY_CODE = new AllowableValue("Country Code", "Country Code", "A code corresponding to a country (TH, e.g.)");
    static final AllowableValue FT_COURSE = new AllowableValue("Course of Study", "Course of Study", "The name of a course of study");
    static final AllowableValue FT_CREDIT_CARD_NUMBER = new AllowableValue("Credit Card Number", "Credit Card Number", "A generated number from a random credit card type");
    static final AllowableValue FT_DEMONYM = new AllowableValue("Demonym", "Demonym", "The term for a person or thing  from a particular country (Austrian, e.g.)");
    static final AllowableValue FT_DEPARTMENT_NAME = new AllowableValue("Department Name", "Department Name", "The name of a department in a business");

    static final AllowableValue FT_DOG_BREED = new AllowableValue("Dog Breed", "Dog Breed", "The name of a breed of dog");
    static final AllowableValue FT_DOG_NAME = new AllowableValue("Dog Name", "Dog Name", "The name of a dog");
    static final AllowableValue FT_EDUCATIONAL_ATTAINMENT = new AllowableValue("Educational Attainment", "Educational Attainment", "The name of a level of education attained");
    static final AllowableValue FT_EMAIL_ADDRESS = new AllowableValue("EMail Address", "EMail Address", "A syntactically valid email address (abc@xyz.com, e.g.)");
    static final AllowableValue FT_FILE_EXTENSION = new AllowableValue("File Extension", "File Extension", "The extension (.exe for example) of a file");
    static final AllowableValue FT_FILENAME = new AllowableValue("Filename", "Filename", "The name of a file");
    static final AllowableValue FT_FIRST_NAME = new AllowableValue("First name", "First name", "The first name of a person");
    static final AllowableValue FT_FOOD = new AllowableValue("Food", "Food", "The name of a prepared dish");
    static final AllowableValue FT_FUNNY_NAME = new AllowableValue("Funny Name", "Funny Name", "A humorous name of a person");
    static final AllowableValue FT_FUTURE_DATE = new AllowableValue("Future Date", "Future Date", "Generates a date up to one year in the future from the time the " +
            "processor is executed");
    static final AllowableValue FT_GOT = new AllowableValue("Game Of Thrones Character", "Game Of Thrones Character", "A character name from Game of Thrones (GoT)");
    static final AllowableValue FT_HARRY_POTTER = new AllowableValue("Harry Potter Character", "Harry Potter Character", "A character name from the Harry Potter franchise");
    static final AllowableValue FT_IBAN = new AllowableValue("IBAN", "IBAN", "International Bank Account Number");
    static final AllowableValue FT_INDUSTRY = new AllowableValue("Industry", "Industry", "The name of an industry (Electrical / Electronic Manufacturing, e.g.)");
    static final AllowableValue FT_IPV4_ADDRESS = new AllowableValue("IPV4 Address", "IPV4 Address", "A valid Internet Protocol Version 4 (IPv4) address");
    static final AllowableValue FT_IPV6_ADDRESS = new AllowableValue("IPV6 Address", "IPV6 Address", "A valid Internet Protocol Version 6 (IPv6) address");
    static final AllowableValue FT_JOB = new AllowableValue("Job", "Job", "The name of a job");
    static final AllowableValue FT_LANGUAGE = new AllowableValue("Language", "Language", "The name of a language");
    static final AllowableValue FT_LAST_NAME = new AllowableValue("First name", "First name", "The first name of a person");
    static final AllowableValue FT_LATITUDE = new AllowableValue("Latitude", "Latitude", "A measurement of degrees of Latitude (-38, e.g.)");
    static final AllowableValue FT_LONGITUDE = new AllowableValue("Longitude", "Longitude", "A measurement of degrees of Longitude (77, e.g.)");
    static final AllowableValue FT_LOREM = new AllowableValue("Lorem", "Lorem", "A random latin word (ipsum, e.g.)");
    static final AllowableValue FT_MAC_ADDRESS = new AllowableValue("MAC Address", "MAC Address", "A syntactically valid Media Access Control (MAC) address");
    static final AllowableValue FT_MARITAL_STATUS = new AllowableValue("Marital Status", "Marital Status", "A term describing a marital status (Single, e.g.)");
    static final AllowableValue FT_MD5 = new AllowableValue("MD5", "MD5", "An MD5 hash");
    static final AllowableValue FT_METAR = new AllowableValue("METAR Weather Report", "METAR Weather Report", "The description of a METAR weather report");
    static final AllowableValue FT_MIME_TYPE = new AllowableValue("MIME Type", "MIME Type", "The MIME type of a document (text/csv, e.g.)");
    static final AllowableValue FT_NAME = new AllowableValue("Name", "Name", "A person's name");
    static final AllowableValue FT_NASDAQ_SYMBOL = new AllowableValue("Nasdaq Stock Symbol", "Nasdaq Stock Symbol", "Stock symbol for the Nasdaq Stock Exchange");

    static final AllowableValue FT_NATIONALITY = new AllowableValue("Nationality", "Nationality", "The name of a nationality");
    static final AllowableValue FT_NUMBER = new AllowableValue("Number", "Number", "A integer number");
    static final AllowableValue FT_NYSE_SYMBOL = new AllowableValue("NYSE Stock Symbol", "Nasdaq Stock Symbol", "Stock symbol for the New York Stock Exchange (NYSE)");
    static final AllowableValue FT_PASSWORD = new AllowableValue("Password", "Password", "A password guaranteed to be between 8 and 20 characters and contains " +
            "at least 1 digit, 1 uppercase letter, and 1 special character");
    static final AllowableValue FT_PAST_DATE = new AllowableValue("Past Date", "Past Date", "Generates a date up to one year in the past from the time the " +
            "processor is executed");
    static final AllowableValue FT_PHONE_NUMBER = new AllowableValue("Phone Number", "Phone Number", "A phone number, possibly with country code and/or extension");
    static final AllowableValue FT_PHONE_EXTENSION = new AllowableValue("Phone Number Extension", "Phone Number Extension", "The extension of a phone number (x4799, e.g.)");
    static final AllowableValue FT_PLANET = new AllowableValue("Planet", "Planet", "A planet in our Solar System");
    static final AllowableValue FT_PROFESSION = new AllowableValue("Profession", "Profession", "The name of a profession");
    static final AllowableValue FT_RACE = new AllowableValue("Race", "Race", "The name of a Race");
    static final AllowableValue FT_SECONDARY_ADDRESS = new AllowableValue("Secondary Address", "Secondary Address", "A secondary address (Suite 330, e.g.)");
    static final AllowableValue FT_SEX = new AllowableValue("Sex", "Sex", "A string containing either Male or Female");
    static final AllowableValue FT_SHA1 = new AllowableValue("SHA-1", "SHA-1", "A SHA-1 hash");
    static final AllowableValue FT_SHA256 = new AllowableValue("SHA-256", "SHA-256", "A SHA-256 hash");
    static final AllowableValue FT_SHA512 = new AllowableValue("SHA-512", "SHA-512", "A SHA-512 hash");
    static final AllowableValue FT_SHAKESPEARE = new AllowableValue("Shakespeare", "Shakespeare", "A quote from Shakespeare's Romeo and Juliet");
    static final AllowableValue FT_SLACK_EMOJI = new AllowableValue("Slack Emoji", "Slack Emoji", "A Slack Emoji string in the format ':name:'");
    static final AllowableValue FT_SSN = new AllowableValue("Social Security Number (SSN)", "Social Security Number (SSN)", "A string in the Social Security Number format");
    static final AllowableValue FT_SPORT = new AllowableValue("Sport", "Sport", "The name of a sport");
    static final AllowableValue FT_STAR_TREK_CHARACTER = new AllowableValue("Star Trek Character", "Star Trek Character",
            "The name of a character from the Star Trek franchise");
    static final AllowableValue FT_STATE = new AllowableValue("State", "State", "The name of a state in the United States");
    static final AllowableValue FT_STATE_ABBR = new AllowableValue("State Abbreviation", "State Abbreviation", "The two-letter abbreviation of a state (ME, e.g.)");
    static final AllowableValue FT_STREET_NAME = new AllowableValue("Street Name", "Street Name", "The name of a street in an address");
    static final AllowableValue FT_STREET_NUMBER = new AllowableValue("Street Address Number", "Street Address Number", "The number of a building on a street in an address");
    static final AllowableValue FT_STREET_ADDRESS = new AllowableValue("Street Address", "Street Address", "A street address");
    static final AllowableValue FT_SUPERHERO = new AllowableValue("Superhero Name", "Superhero Name", "The name of a superhero");

    static final AllowableValue FT_TEMP_F = new AllowableValue("Fahrenheit Temperature", "Fahrenheit Temperature",
            "A temperature between -22 degrees and 100 degrees Fahrenheit");
    static final AllowableValue FT_TEMP_C = new AllowableValue("Celsius Temperature", "Celsius Temperature",
            "A temperature between -30 degrees and 38 degrees Celsius");
    static final AllowableValue FT_TIMEZONE = new AllowableValue("Timezone", "Timezone", "The name of a timezone (Europe/Lisbon, e.g.)");
    static final AllowableValue FT_UNIVERSITY = new AllowableValue("University", "University", "The name of a university");

    static final AllowableValue FT_URL = new AllowableValue("URL", "URL", "A syntactically valid Uniform Resource Locator (URL)");
    static final AllowableValue FT_USER_AGENT = new AllowableValue("User Agent", "User Agent", "A syntactically valid User Agent value for HTTP messages e.g.");
    static final AllowableValue FT_WEATHER = new AllowableValue("Weather", "Weather", "A string description of possible weather types");
    static final AllowableValue FT_ZELDA = new AllowableValue("Zelda", "Zelda", "The name of a character in the Zelda franchise");
    static final AllowableValue FT_ZIP_CODE = new AllowableValue("ZIP Code", "ZIP Code", "A ZIP code from a mailing address");

    static final AllowableValue[] FIELD_TYPES = {
            FT_ADDRESS,
            FT_AIRCRAFT,
            FT_AIRPORT,
            FT_ANCIENT_GOD,
            FT_ANCIENT_HERO,
            FT_ANCIENT_PRIMORDIAL,
            FT_ANCIENT_TITAN,
            FT_ANIMAL,
            FT_APP_AUTHOR,
            FT_APP_NAME,
            FT_APP_VERSION,
            FT_ARTIST,
            FT_AVATAR,
            FT_BEER_HOP,
            FT_BEER_MALT,
            FT_BEER_NAME,
            FT_BEER_STYLE,
            FT_BEER_YEAST,
            FT_BIRTHDAY,
            FT_BOOK_AUTHOR,
            FT_BOOK_GENRE,
            FT_BOOK_PUBLISHER,
            FT_BOOK_TITLE,
            FT_BOOL,
            FT_BIC,
            FT_BUILDING_NUMBER,
            FT_CHUCK_NORRIS_FACT,
            FT_CAT_BREED,
            FT_CAT_NAME,
            FT_CAT_REGISTRY,
            FT_TEMP_C,
            FT_CITY,
            FT_COLOR,
            FT_COMPANY_NAME,
            FT_CONSTELLATION,
            FT_COUNTRY,
            FT_COUNTRY_CAPITOL,
            FT_COUNTRY_CODE,
            FT_COURSE,
            FT_CREDIT_CARD_NUMBER,
            FT_DEMONYM,
            FT_DEPARTMENT_NAME,
            FT_DOG_BREED,
            FT_DOG_NAME,
            FT_EDUCATIONAL_ATTAINMENT,
            FT_EMAIL_ADDRESS,
            FT_TEMP_F,
            FT_FILE_EXTENSION,
            FT_FILENAME,
            FT_FIRST_NAME,
            FT_FOOD,
            FT_FUNNY_NAME,
            FT_FUTURE_DATE,
            FT_GOT,
            FT_HARRY_POTTER,
            FT_IBAN,
            FT_INDUSTRY,
            FT_IPV4_ADDRESS,
            FT_IPV6_ADDRESS,
            FT_JOB,
            FT_LANGUAGE,
            FT_LAST_NAME,
            FT_LATITUDE,
            FT_LONGITUDE,
            FT_LOREM,
            FT_MAC_ADDRESS,
            FT_MARITAL_STATUS,
            FT_MD5,
            FT_METAR,
            FT_MIME_TYPE,
            FT_NAME,
            FT_NASDAQ_SYMBOL,
            FT_NATIONALITY,
            FT_NUMBER,
            FT_NYSE_SYMBOL,
            FT_PASSWORD,
            FT_PAST_DATE,
            FT_PHONE_NUMBER,
            FT_PHONE_EXTENSION,
            FT_PLANET,
            FT_PROFESSION,
            FT_RACE,
            FT_SECONDARY_ADDRESS,
            FT_SEX,
            FT_SHA1,
            FT_SHA256,
            FT_SHA512,
            FT_SHAKESPEARE,
            FT_SLACK_EMOJI,
            FT_SSN,
            FT_SPORT,
            FT_STAR_TREK_CHARACTER,
            FT_STATE,
            FT_STATE_ABBR,
            FT_STREET_ADDRESS,
            FT_STREET_NAME,
            FT_STREET_NUMBER,
            FT_SUPERHERO,
            FT_TIMEZONE,
            FT_UNIVERSITY,
            FT_URL,
            FT_USER_AGENT,
            FT_WEATHER,
            FT_ZELDA,
            FT_ZIP_CODE
    };

    static final String[] SUPPORTED_LOCALES = {
            "bg",
            "ca",
            "ca-CAT",
            "da-DK",
            "de",
            "de-AT",
            "de-CH",
            "en",
            "en-AU",
            "en-au-ocker",
            "en-BORK",
            "en-CA",
            "en-GB",
            "en-IND",
            "en-MS",
            "en-NEP",
            "en-NG",
            "en-NZ",
            "en-PAK",
            "en-SG",
            "en-UG",
            "en-US",
            "en-ZA",
            "es",
            "es-MX",
            "fa",
            "fi-FI",
            "fr",
            "he",
            "hu",
            "in-ID",
            "it",
            "ja",
            "ko",
            "nb-NO",
            "nl",
            "pl",
            "pt",
            "pt-BR",
            "ru",
            "sk",
            "sv",
            "sv-SE",
            "tr",
            "uk",
            "vi",
            "zh-CN",
            "zh-TW"
    };

    private volatile Faker faker = new Faker();

    static final PropertyDescriptor SCHEMA_TEXT = new PropertyDescriptor.Builder()
            .name("schema-text")
            .displayName("Schema Text")
            .description("The text of an Avro-formatted Schema used to generate record data. If this property is set, any user-defined properties are ignored.")
            .addValidator(new AvroSchemaValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor NUM_RECORDS = new PropertyDescriptor.Builder()
            .name("gen-fake-record-num-records")
            .displayName("Number of Records")
            .description("Specifies how many records will be generated for each outgoing FlowFile.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor LOCALE = new PropertyDescriptor.Builder()
            .name("gen-fake-record-locale")
            .displayName("Locale")
            .description("The locale that will be used to generate field data. For example a Locale of 'es' will generate fields (e.g. names) in Spanish.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .defaultValue("en-US")
            .allowableValues(SUPPORTED_LOCALES)
            .build();

    static final PropertyDescriptor NULLABLE_FIELDS = new PropertyDescriptor.Builder()
            .name("gen-fake-record-nullable-fields")
            .displayName("Nullable Fields")
            .description("Whether the generated fields will be nullable. Note that this property is ignored if Schema Text is set. Also it only affects the schema of the generated data, " +
                    "not whether any values will be null. If this property is true, see 'Null Value Percentage' to set the probability that any generated field will be null.")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();
    static final PropertyDescriptor NULL_PERCENTAGE = new PropertyDescriptor.Builder()
            .name("gen-fake-record-null-pct")
            .displayName("Null Value Percentage")
            .description("The percent probability (0-100%) that a generated value for any nullable field will be null. Set this property to zero to have no null values, or 100 to have all " +
                    "null values.")
            .addValidator(StandardValidators.createLongValidator(0L, 100L, true))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .defaultValue("0")
            .dependsOn(NULLABLE_FIELDS, "true")
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully transformed will be routed to this relationship")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SCHEMA_TEXT);
        properties.add(RECORD_WRITER);
        properties.add(NUM_RECORDS);
        properties.add(LOCALE);
        properties.add(NULLABLE_FIELDS);
        properties.add(NULL_PERCENTAGE);
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .allowableValues(FIELD_TYPES)
                .defaultValue(FT_ADDRESS.getValue())
                .required(false)
                .dynamic(true)
                .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final String locale = context.getProperty(LOCALE).getValue();
        faker = new Faker(new Locale(locale));
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        final String schemaText = context.getProperty(SCHEMA_TEXT).evaluateAttributeExpressions().getValue();
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final int numRecords = context.getProperty(NUM_RECORDS).evaluateAttributeExpressions().asInteger();
        FlowFile flowFile = session.create();
        final Map<String, String> attributes = new HashMap<>();
        final AtomicInteger recordCount = new AtomicInteger();

        try {
            flowFile = session.write(flowFile, out -> {
                final RecordSchema recordSchema;
                final boolean usingSchema;
                final int nullPercentage = context.getProperty(NULL_PERCENTAGE).evaluateAttributeExpressions().asInteger();
                if (!StringUtils.isEmpty(schemaText)) {
                    final Schema avroSchema = new Schema.Parser().parse(schemaText);
                    recordSchema = AvroTypeUtil.createSchema(avroSchema);
                    usingSchema = true;
                } else {
                    // Generate RecordSchema from user-defined properties
                    final boolean nullable = context.getProperty(NULLABLE_FIELDS).asBoolean();
                    final Map<String, String> fields = getFields(context);
                    recordSchema = generateRecordSchema(fields, nullable);
                    usingSchema = false;
                }
                try {
                    final RecordSchema writeSchema = writerFactory.getSchema(attributes, recordSchema);
                    try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, attributes)) {
                        writer.beginRecordSet();

                        Record record;
                        List<RecordField> writeFieldNames = writeSchema.getFields();
                        Map<String, Object> recordEntries = new HashMap<>();
                        for (int i = 0; i < numRecords; i++) {
                            for (RecordField writeRecordField : writeFieldNames) {
                                final String writeFieldName = writeRecordField.getFieldName();
                                final Object writeFieldValue;
                                if (usingSchema) {
                                    writeFieldValue = generateValueFromRecordField(writeRecordField, faker, nullPercentage);
                                } else {
                                    final boolean nullValue;
                                    if (!context.getProperty(GenerateFakeRecord.NULLABLE_FIELDS).asBoolean() || nullPercentage == 0) {
                                        nullValue = false;
                                    } else {
                                        nullValue = (faker.number().numberBetween(0, 100) <= nullPercentage);
                                    }
                                    if (nullValue) {
                                        writeFieldValue = null;
                                    } else {
                                        final String propertyValue = context.getProperty(writeFieldName).getValue();
                                        writeFieldValue = getFakeData(propertyValue, faker);
                                    }
                                }

                                recordEntries.put(writeFieldName, writeFieldValue);
                            }
                            record = new MapRecord(recordSchema, recordEntries);
                            writer.write(record);
                        }

                        final WriteResult writeResult = writer.finishRecordSet();
                        attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                        attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                        attributes.putAll(writeResult.getAttributes());
                        recordCount.set(writeResult.getRecordCount());
                    }
                } catch (final SchemaNotFoundException e) {
                    throw new ProcessException(e.getLocalizedMessage(), e);
                }
            });
        } catch (final Exception e) {
            getLogger().error("Failed to process {}; will route to failure", flowFile, e);
            if (e instanceof ProcessException) {
                throw e;
            } else {
                throw new ProcessException(e);
            }
        }

        flowFile = session.putAllAttributes(flowFile, attributes);
        session.transfer(flowFile, REL_SUCCESS);

        final int count = recordCount.get();
        session.adjustCounter("Records Processed", count, false);

        getLogger().info("Successfully generated {} records for {}", count, flowFile);
    }

    protected Map<String, String> getFields(ProcessContext context) {
        return context.getProperties().entrySet().stream()
                // filter non-null dynamic properties
                .filter(e -> e.getKey().isDynamic() && e.getValue() != null)
                // convert to Map of user-defined field names and types
                .collect(Collectors.toMap(
                        e -> e.getKey().getName(),
                        e -> context.getProperty(e.getKey()).getValue()
                ));
    }

    private Object getFakeData(String type, Faker faker) {

        if (FT_ADDRESS.getValue().equals(type)) {
            return faker.address().fullAddress();
        }
        if (FT_AIRCRAFT.getValue().equals(type)) {
            return faker.aviation().airport();
        }
        if (FT_AIRPORT.getValue().equals(type)) {
            return faker.aviation().airport();
        }
        if (FT_ANCIENT_GOD.getValue().equals(type)) {
            return faker.ancient().god();
        }
        if (FT_ANCIENT_HERO.getValue().equals(type)) {
            return faker.ancient().hero();
        }
        if (FT_ANCIENT_PRIMORDIAL.getValue().equals(type)) {
            return faker.ancient().primordial();
        }
        if (FT_ANCIENT_TITAN.getValue().equals(type)) {
            return faker.ancient().titan();
        }
        if (FT_ANIMAL.getValue().equals(type)) {
            return faker.animal().name();
        }
        if (FT_APP_AUTHOR.getValue().equals(type)) {
            return faker.app().author();
        }
        if (FT_APP_NAME.getValue().equals(type)) {
            return faker.app().name();
        }
        if (FT_APP_VERSION.getValue().equals(type)) {
            return faker.app().version();
        }
        if (FT_ARTIST.getValue().equals(type)) {
            return faker.artist().name();
        }
        if (FT_AVATAR.getValue().equals(type)) {
            return faker.avatar().image();
        }
        if (FT_BEER_HOP.getValue().equals(type)) {
            return faker.beer().hop();
        }
        if (FT_BEER_NAME.getValue().equals(type)) {
            return faker.beer().name();
        }
        if (FT_BEER_MALT.getValue().equals(type)) {
            return faker.beer().malt();
        }
        if (FT_BEER_STYLE.getValue().equals(type)) {
            return faker.beer().style();
        }
        if (FT_BEER_YEAST.getValue().equals(type)) {
            return faker.beer().yeast();
        }
        if (FT_BOOK_AUTHOR.getValue().equals(type)) {
            return faker.book().author();
        }
        if (FT_BOOK_GENRE.getValue().equals(type)) {
            return faker.book().genre();
        }
        if (FT_BOOK_PUBLISHER.getValue().equals(type)) {
            return faker.book().publisher();
        }
        if (FT_BOOK_TITLE.getValue().equals(type)) {
            return faker.book().title();
        }
        if (FT_BOOL.getValue().equals(type)) {
            return faker.bool().bool();
        }
        if (FT_BIC.getValue().equals(type)) {
            return faker.finance().bic();
        }
        if (FT_BIRTHDAY.getValue().equals(type)) {
            return faker.date().birthday();
        }
        if (FT_BUILDING_NUMBER.getValue().equals(type)) {
            return faker.address().buildingNumber();
        }
        if (FT_CHUCK_NORRIS_FACT.getValue().equals(type)) {
            return faker.chuckNorris().fact();
        }
        if (FT_CAT_BREED.getValue().equals(type)) {
            return faker.cat().breed();
        }
        if (FT_CAT_NAME.getValue().equals(type)) {
            return faker.cat().name();
        }
        if (FT_CAT_REGISTRY.getValue().equals(type)) {
            return faker.cat().registry();
        }
        if (FT_CITY.getValue().equals(type)) {
            return faker.address().city();
        }
        if (FT_COLOR.getValue().equals(type)) {
            return faker.color().name();
        }
        if (FT_COMPANY_NAME.getValue().equals(type)) {
            return faker.company().name();
        }
        if (FT_CONSTELLATION.getValue().equals(type)) {
            return faker.space().constellation();
        }
        if (FT_COUNTRY.getValue().equals(type)) {
            return faker.country().name();
        }
        if (FT_COUNTRY_CAPITOL.getValue().equals(type)) {
            return faker.country().capital();
        }
        if (FT_COUNTRY_CODE.getValue().equals(type)) {
            return faker.address().countryCode();
        }
        if (FT_COURSE.getValue().equals(type)) {
            return faker.educator().course();
        }
        if (FT_CREDIT_CARD_NUMBER.getValue().equals(type)) {
            return faker.finance().creditCard();
        }
        if (FT_DEMONYM.getValue().equals(type)) {
            return faker.demographic().demonym();
        }
        if (FT_DEPARTMENT_NAME.getValue().equals(type)) {
            return faker.commerce().department();
        }
        if (FT_DOG_BREED.getValue().equals(type)) {
            return faker.dog().breed();
        }
        if (FT_DOG_NAME.getValue().equals(type)) {
            return faker.dog().name();
        }
        if (FT_EDUCATIONAL_ATTAINMENT.getValue().equals(type)) {
            return faker.demographic().educationalAttainment();
        }
        if (FT_EMAIL_ADDRESS.getValue().equals(type)) {
            return faker.internet().emailAddress();
        }
        if (FT_FILE_EXTENSION.getValue().equals(type)) {
            return faker.file().extension();
        }
        if (FT_FILENAME.getValue().equals(type)) {
            return faker.file().fileName();
        }
        if (FT_FIRST_NAME.getValue().equals(type)) {
            return faker.name().firstName();
        }
        if (FT_FOOD.getValue().equals(type)) {
            return faker.food().dish();
        }
        if (FT_FUNNY_NAME.getValue().equals(type)) {
            return faker.funnyName().name();
        }
        if (FT_FUTURE_DATE.getValue().equals(type)) {
            return faker.date().future(365, TimeUnit.DAYS);
        }
        if (FT_GOT.getValue().equals(type)) {
            return faker.gameOfThrones().character();
        }
        if (FT_HARRY_POTTER.getValue().equals(type)) {
            return faker.harryPotter().character();
        }
        if (FT_IBAN.getValue().equals(type)) {
            return faker.finance().iban();
        }
        if (FT_INDUSTRY.getValue().equals(type)) {
            return faker.company().industry();
        }
        if (FT_IPV4_ADDRESS.getValue().equals(type)) {
            return faker.internet().ipV4Address();
        }
        if (FT_IPV6_ADDRESS.getValue().equals(type)) {
            return faker.internet().ipV6Address();
        }
        if (FT_JOB.getValue().equals(type)) {
            return faker.job().title();
        }
        if (FT_LANGUAGE.getValue().equals(type)) {
            return faker.nation().language();
        }
        if (FT_LAST_NAME.getValue().equals(type)) {
            return faker.name().lastName();
        }
        if (FT_LATITUDE.getValue().equals(type)) {
            return Double.valueOf(faker.address().latitude());
        }
        if (FT_LONGITUDE.getValue().equals(type)) {
            return Double.valueOf(faker.address().longitude());
        }
        if (FT_LOREM.getValue().equals(type)) {
            return faker.lorem().word();
        }
        if (FT_MAC_ADDRESS.getValue().equals(type)) {
            return faker.internet().macAddress();
        }
        if (FT_MD5.getValue().equals(type)) {
            return faker.crypto().md5();
        }
        if (FT_MARITAL_STATUS.getValue().equals(type)) {
            return faker.demographic().maritalStatus();
        }
        if (FT_METAR.getValue().equals(type)) {
            return faker.aviation().METAR();
        }
        if (FT_MIME_TYPE.getValue().equals(type)) {
            return faker.file().mimeType();
        }
        if (FT_NAME.getValue().equals(type)) {
            return faker.name().fullName();
        }
        if (FT_NASDAQ_SYMBOL.getValue().equals(type)) {
            return faker.stock().nsdqSymbol();
        }
        if (FT_NATIONALITY.getValue().equals(type)) {
            return faker.nation().nationality();
        }
        if (FT_NUMBER.getValue().equals(type)) {
            return faker.number().numberBetween(Integer.MIN_VALUE, Integer.MAX_VALUE);
        }
        if (FT_NYSE_SYMBOL.getValue().equals(type)) {
            return faker.stock().nyseSymbol();
        }
        if (FT_PASSWORD.getValue().equals(type)) {
            return faker.internet().password(8, 20, true, true, true);
        }
        if (FT_PAST_DATE.getValue().equals(type)) {
            return faker.date().past(365, TimeUnit.DAYS);
        }
        if (FT_PHONE_NUMBER.getValue().equals(type)) {
            return faker.phoneNumber().phoneNumber();
        }
        if (FT_PHONE_EXTENSION.getValue().equals(type)) {
            return faker.phoneNumber().extension();
        }
        if (FT_PLANET.getValue().equals(type)) {
            return faker.space().planet();
        }
        if (FT_PROFESSION.getValue().equals(type)) {
            return faker.company().profession();
        }
        if (FT_RACE.getValue().equals(type)) {
            return faker.demographic().race();
        }
        if (FT_SECONDARY_ADDRESS.getValue().equals(type)) {
            return faker.address().secondaryAddress();
        }
        if (FT_SEX.getValue().equals(type)) {
            return faker.demographic().sex();
        }
        if (FT_SHA1.getValue().equals(type)) {
            return faker.crypto().sha1();
        }
        if (FT_SHA256.getValue().equals(type)) {
            return faker.crypto().sha256();
        }
        if (FT_SHA512.getValue().equals(type)) {
            return faker.crypto().sha512();
        }
        if (FT_SHAKESPEARE.getValue().equals(type)) {
            return faker.shakespeare().romeoAndJulietQuote();
        }
        if (FT_SLACK_EMOJI.getValue().equals(type)) {
            return faker.slackEmoji().emoji();
        }
        if (FT_SSN.getValue().equals(type)) {
            return faker.idNumber().ssnValid();
        }
        if (FT_SPORT.getValue().equals(type)) {
            return faker.team().sport();
        }
        if (FT_STAR_TREK_CHARACTER.getValue().equals(type)) {
            return faker.starTrek().character();
        }
        if (FT_STATE.getValue().equals(type)) {
            return faker.address().state();
        }
        if (FT_STATE_ABBR.getValue().equals(type)) {
            return faker.address().stateAbbr();
        }
        if (FT_STREET_ADDRESS.getValue().equals(type)) {
            return faker.address().streetAddress();
        }
        if (FT_STREET_NAME.getValue().equals(type)) {
            return faker.address().streetName();
        }
        if (FT_STREET_NUMBER.getValue().equals(type)) {
            return faker.address().streetAddressNumber();
        }
        if (FT_SUPERHERO.getValue().equals(type)) {
            return faker.superhero().name();
        }
        if (FT_TEMP_C.getValue().equals(type)) {
            return faker.weather().temperatureCelsius();
        }
        if (FT_TEMP_F.getValue().equals(type)) {
            return faker.weather().temperatureFahrenheit();
        }
        if (FT_TIMEZONE.getValue().equals(type)) {
            return faker.address().timeZone();
        }
        if (FT_UNIVERSITY.getValue().equals(type)) {
            return faker.university().name();
        }
        if (FT_URL.getValue().equals(type)) {
            return faker.internet().url();
        }
        if (FT_USER_AGENT.getValue().equals(type)) {
            return faker.internet().userAgentAny();
        }
        if (FT_WEATHER.getValue().equals(type)) {
            return faker.weather().description();
        }
        if (FT_ZELDA.getValue().equals(type)) {
            return faker.zelda().character();
        }
        if (FT_ZIP_CODE.getValue().equals(type)) {
            return faker.address().zipCode();
        } else {
            throw new ProcessException(type + " is not a valid value");
        }
    }

    private DataType getDataType(final String type) {

        if (FT_FUTURE_DATE.getValue().equals(type)
                || FT_PAST_DATE.getValue().equals(type)
                || FT_BIRTHDAY.getValue().equals(type)
        ) {
            return RecordFieldType.DATE.getDataType();
        }
        if (FT_LATITUDE.getValue().equals(type)
                || FT_LONGITUDE.getValue().equals(type)) {
            return RecordFieldType.DOUBLE.getDataType("%.8g");
        }
        if (FT_BOOL.getValue().equals(type)) {
            return RecordFieldType.BOOLEAN.getDataType();
        }
        return RecordFieldType.STRING.getDataType();
    }

    private Object generateValueFromRecordField(RecordField recordField, Faker faker, int nullPercentage) {
        if (recordField.isNullable() && faker.number().numberBetween(0, 100) <= nullPercentage) {
            return null;
        }
        switch (recordField.getDataType().getFieldType()) {
            case BIGINT:
                return new BigInteger(String.valueOf(faker.number().numberBetween(Long.MIN_VALUE, Long.MAX_VALUE)));
            case BOOLEAN:
                return getFakeData(FT_BOOL.getValue(), faker);
            case BYTE:
                return faker.number().numberBetween(Byte.MIN_VALUE, Byte.MAX_VALUE);
            case CHAR:
                return (char) faker.number().numberBetween(Character.MIN_VALUE, Character.MAX_VALUE);
            case DATE:
                return getFakeData(FT_PAST_DATE.getValue(), faker);
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
                return faker.number().randomDouble(((DecimalDataType) recordField.getDataType()).getScale(), Long.MIN_VALUE, Long.MAX_VALUE);
            case INT:
                return faker.number().numberBetween(Integer.MIN_VALUE, Integer.MAX_VALUE);
            case LONG:
                return faker.number().numberBetween(Long.MIN_VALUE, Long.MAX_VALUE);
            case SHORT:
                return faker.number().numberBetween(Short.MIN_VALUE, Short.MAX_VALUE);
            case ENUM:
                List<String> enums = ((EnumDataType) recordField.getDataType()).getEnums();
                return enums.get(faker.number().numberBetween(0, enums.size() - 1));
            case STRING:
                return generateRandomString();
            case TIME:
                DateFormat df = new SimpleDateFormat("HH:mm:ss");
                return df.format((Date) getFakeData(FT_PAST_DATE.getValue(), faker));
            case TIMESTAMP:
                return ((Date) getFakeData(FT_PAST_DATE.getValue(), faker)).getTime();
            case UUID:
                return UUID.randomUUID();
            case ARRAY:
                final ArrayDataType arrayDataType = (ArrayDataType) recordField.getDataType();
                final DataType elementType = arrayDataType.getElementType();
                final int numElements = faker.number().numberBetween(0, 10);
                Object[] returnValue = new Object[numElements];
                for (int i = 0; i < numElements; i++) {
                    RecordField tempRecordField = new RecordField(recordField.getFieldName() + "[" + i + "]", elementType, arrayDataType.isElementsNullable());
                    // If the array elements are non-nullable, use zero as the nullPercentage
                    returnValue[i] = generateValueFromRecordField(tempRecordField, faker, arrayDataType.isElementsNullable() ? nullPercentage : 0);
                }
                return returnValue;
            case MAP:
                final MapDataType mapDataType = (MapDataType) recordField.getDataType();
                final DataType valueType = mapDataType.getValueType();
                // Create 4-element fake map
                Map<String, Object> returnMap = new HashMap<>(4);
                returnMap.put("test1", generateValueFromRecordField(new RecordField("test1", valueType), faker, nullPercentage));
                returnMap.put("test2", generateValueFromRecordField(new RecordField("test2", valueType), faker, nullPercentage));
                returnMap.put("test3", generateValueFromRecordField(new RecordField("test3", valueType), faker, nullPercentage));
                returnMap.put("test4", generateValueFromRecordField(new RecordField("test4", valueType), faker, nullPercentage));
                return returnMap;
            case RECORD:
                final RecordDataType recordType = (RecordDataType) recordField.getDataType();
                final RecordSchema childSchema = recordType.getChildSchema();
                final Map<String, Object> recordValues = new HashMap<>();
                for (RecordField writeRecordField : childSchema.getFields()) {
                    final String writeFieldName = writeRecordField.getFieldName();
                    final Object writeFieldValue = generateValueFromRecordField(writeRecordField, faker, nullPercentage);
                    recordValues.put(writeFieldName, writeFieldValue);
                }
                return new MapRecord(childSchema, recordValues);
            case CHOICE:
                final ChoiceDataType choiceDataType = (ChoiceDataType) recordField.getDataType();
                List<DataType> subTypes = choiceDataType.getPossibleSubTypes();
                // Pick one at random and generate a value for it
                DataType chosenType = subTypes.get(faker.number().numberBetween(0, subTypes.size() - 1));
                RecordField tempRecordField = new RecordField(recordField.getFieldName(), chosenType);
                return generateValueFromRecordField(tempRecordField, faker, nullPercentage);
            default:
                return generateRandomString();
        }
    }

    private String generateRandomString() {
        final int categoryChoice = faker.number().numberBetween(0, 10);
        switch (categoryChoice) {
            case 0:
                return faker.name().fullName();
            case 1:
                return faker.lorem().word();
            case 2:
                return faker.shakespeare().romeoAndJulietQuote();
            case 3:
                return faker.educator().university();
            case 4:
                return faker.zelda().game();
            case 5:
                return faker.company().name();
            case 6:
                return faker.chuckNorris().fact();
            case 7:
                return faker.book().title();
            case 8:
                return faker.dog().breed();
            default:
                return faker.animal().name();
        }
    }

    protected RecordSchema generateRecordSchema(final Map<String, String> fields, final boolean nullable) {
        final List<RecordField> recordFields = new ArrayList<>(fields.size());
        for (Map.Entry<String, String> field : fields.entrySet()) {
            final String fieldName = field.getKey();
            final String fieldType = field.getValue();
            final DataType fieldDataType = getDataType(fieldType);
            RecordField recordField = new RecordField(fieldName, fieldDataType, nullable);
            recordFields.add(recordField);
        }

        return new SimpleRecordSchema(recordFields);
    }
}
