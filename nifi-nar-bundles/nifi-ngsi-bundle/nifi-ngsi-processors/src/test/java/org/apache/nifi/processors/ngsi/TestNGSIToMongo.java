package org.apache.nifi.processors.ngsi;

import org.apache.nifi.processors.ngsi.ngsi.backends.MongoBackend;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSICharsets;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.Test;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class TestNGSIToMongo {

    private TestRunner runner;
    private MongoBackend backend;

    @Before
    public void setUp() throws Exception {

        runner = TestRunners.newTestRunner(NGSIToMongo.class);
        runner.setProperty(NGSIToMongo.URI, "localhost:27017");
        runner.setProperty(NGSIToMongo.NGSI_VERSION, "v2");
        runner.setProperty(NGSIToMongo.DATA_MODEL, "db-by-service-path");
        runner.setProperty(NGSIToMongo.ATTR_PERSISTENCE, "row");
        runner.setProperty(NGSIToMongo.ENABLE_ENCODING, "false");
        runner.setProperty(NGSIToMongo.ENABLE_LOWERCASE, "false");
        runner.setProperty(NGSIToMongo.BATCH_SIZE, "100");
        runner.setProperty(NGSIToMongo.DB_PREFIX, "sth_");
        runner.setProperty(NGSIToMongo.COLLECTION_PREFIX, "sth_");
        runner.setProperty(NGSIToMongo.DATA_EXPIRATION, "0");
        runner.setProperty(NGSIToMongo.COLLECTION_SIZE, "0");
        runner.setProperty(NGSIToMongo.MAX_DOCUMENTS, "0");
        backend = new MongoBackend(null,runner.getProcessContext().getProperty(NGSIToMongo.DATA_MODEL).getValue());

    }

    /**
     * [NGSIMongoSink.configure] -------- Configured 'collection_prefix' is encoded when having forbidden characters.
     */
    @Test
    public void testConfigureCollectionPrefixIsEncodedOldEncoding() {
        System.out.println("[NGSIToMongo.configure]"
                + "-------- Configured 'collection_prefix' is encoded when having forbidden characters");

        String collectionPrefix = "this\\is/a$prefix.with-forbidden,chars:-.";
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMongo.ENABLE_ENCODING).asBoolean();
        runner.setProperty(NGSIToMongo.DATA_MODEL, "db-by-service-path");
        backend.setDataModel(runner.getProcessContext().getProperty(NGSIToMongo.DATA_MODEL).getValue());

        String expectedCollectionPrefix = NGSICharsets.encodeSTHCollection(collectionPrefix);
        try{
            try {
                assertEquals(expectedCollectionPrefix, backend.buildCollectionName("", "", "", "", enableEncoding, collectionPrefix));
                System.out.println("[NGSIToMongo.configure]"
                        + "-  OK  - 'collection_prefix=" + collectionPrefix
                        + "' correctly encoded as '" + expectedCollectionPrefix + "'");
            } catch (AssertionError e) {
                System.out.println("[NGSIToMongo.configure]"
                        + "- FAIL - 'collection_prefix=" + collectionPrefix
                        + "' wrongly encoded as '" + expectedCollectionPrefix + "'");
                throw e;
            } // try catch // try catch
        } catch (Exception e) {
            System.out.println("[NGSIMySQLSink.buildCollectionName]"
                    + "- FAIL - There was some problem when building the DB name");
        }
    }

    // testConfigureCollectionPrefixIsEncodedOldEncoding

    /**
     * [NGSIMongoSink.configure] -------- Configured 'collection_prefix' is encoded when having forbidden characters.
     */
    @Test
    public void testConfigureCollectionPrefixIsEncodedNewEncoding() {
        System.out.println("[NGSIToMongo.configure]"
                + "-------- Configured 'collection_prefix' is encoded when having forbidden characters");
        String collectionPrefix = "this\\is/a$prefix.with-forbidden,chars:-.";
        runner.setProperty(NGSIToMongo.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMongo.ENABLE_ENCODING).asBoolean();
        backend.setDataModel(runner.getProcessContext().getProperty(NGSIToMongo.DATA_MODEL).getValue());

        String expectedCollectionPrefix = NGSICharsets.encodeMongoDBCollection(collectionPrefix);
        try{
            try {
                assertEquals(expectedCollectionPrefix, backend.buildCollectionName("", "", "", "", enableEncoding, collectionPrefix));
                System.out.println("[NGSIToMongo.configure]"
                        + "-  OK  - 'collection_prefix=" + collectionPrefix
                        + "' correctly encoded as '" + expectedCollectionPrefix + "'");
            } catch (AssertionError e) {
                System.out.println("[NGSIToMongo.configure]"
                        + "- FAIL - 'collection_prefix=" + collectionPrefix
                        + "' wrongly encoded as '" + expectedCollectionPrefix + "'");
                throw e;
            } // try catch // try catch
        } catch (Exception e) {
            System.out.println("[NGSIMySQLSink.buildCollectionName]"
                    + "- FAIL - There was some problem when building the DB name");
        }
    } // testConfigureCollectionPrefixIsEncodedNewEncoding

    /**
     * [NGSIMongoSink.configure] -------- Configured 'db_prefix' is encoded when having forbidden characters.
     */
    @Test
    public void testConfigureDBPrefixIsEncodedOldEncoding() {
        System.out.println("[NGSIToMongo.configure]"
                + "-------- Configured 'db_prefix' is encoded when having forbidden characters");

        runner.setProperty(NGSIToMongo.DB_PREFIX, "this\\is/a$prefix.with forbidden\"chars:-,");
        String dbPrefix = runner.getProcessContext().getProperty(NGSIToMongo.DB_PREFIX).getValue();
        runner.setProperty(NGSIToMongo.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMongo.ENABLE_ENCODING).asBoolean();

        String expectedDbPrefix = NGSICharsets.encodeSTHDB(dbPrefix);
        try{
            try {
                assertEquals(expectedDbPrefix, backend.buildDbName("", enableEncoding, dbPrefix));
                System.out.println("[NGSIToMongo.configure]"
                        + "-  OK  - 'db_prefix=" + dbPrefix + "' correctly encoded as '" + expectedDbPrefix + "'");
            } catch (AssertionError e) {
                System.out.println("[NGSIToMongo.configure]"
                        + "- FAIL - 'db_prefix=" + dbPrefix + "' wrongly encoded as '" + expectedDbPrefix + "'");
                throw e;
            } // try catch // try catch
        } catch (Exception e) {
            System.out.println("[NGSIMySQLSink.buildDBName]"
                    + "- FAIL - There was some problem when building the DB name");
        }
    } // testConfigureDBPrefixIsEncodedOldEncoding

    /**
     * [NGSIMongoSink.configure] -------- Configured 'db_prefix' is encoded when having forbidden characters.
     */
    @Test
    public void testConfigureDBPrefixIsEncodedNewEncoding() {
        System.out.println("[NGSIToMongo.configure]"
                + "-------- Configured 'db_prefix' is encoded when having forbidden characters");
        runner.setProperty(NGSIToMongo.DB_PREFIX, "this\\is/a$prefix.with forbidden\"chars:-,");
        String dbPrefix = runner.getProcessContext().getProperty(NGSIToMongo.DB_PREFIX).getValue();
        runner.setProperty(NGSIToMongo.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMongo.ENABLE_ENCODING).asBoolean();

        String expectedDbPrefix = NGSICharsets.encodeMongoDBDatabase(dbPrefix);
        try{
            try {
                assertEquals(expectedDbPrefix, backend.buildDbName("", enableEncoding, dbPrefix));
                System.out.println("[NGSIToMongo.configure]"
                        + "-  OK  - 'db_prefix=" + dbPrefix + "' correctly encoded as '" + expectedDbPrefix + "'");
            } catch (AssertionError e) {
                System.out.println("[NGSIToMongo.configure]"
                        + "- FAIL - 'db_prefix=" + dbPrefix + "' wrongly encoded as '" + expectedDbPrefix + "'");
                throw e;
            } // try catch // try catch
        } catch (Exception e) {
            System.out.println("[NGSIMySQLSink.buildDBName]"
                    + "- FAIL - There was some problem when building the DB name");
        }
    } // testConfigureDBPrefixIsEncodedNewEncoding

    /**
     * [NGSIToMongo.buildCollectionName] -------- When / service-path is notified/defaulted and
     * data_model=dm-by-service-path, the MongoDB collection name is the concatenation of \<prefix\> and
     * \<service-path\>.
     */
    @Test
    public void testBuildCollectionNameDMByServicePathRootServicePathOldEncoding() {
        System.out.println("[NGSIToMongo.buildCollectionName]"
                + "-------- When / service-path is notified/defaulted and data_model=dm-by-service-path, "
                + "the MongoDB collection name is the concatenation of <prefix> and <service-path>");

        runner.setProperty(NGSIToMongo.COLLECTION_PREFIX, "sth_");
        String collectionPrefix = runner.getProcessContext().getProperty(NGSIToMongo.COLLECTION_PREFIX).getValue();
        runner.setProperty(NGSIToMongo.DB_PREFIX, "sth_");
        String dbPrefix = runner.getProcessContext().getProperty(NGSIToMongo.DB_PREFIX).getValue();
        runner.setProperty(NGSIToMongo.DATA_MODEL, "db-by-service-path");
        backend.setDataModel(runner.getProcessContext().getProperty(NGSIToMongo.DATA_MODEL).getValue());
        runner.setProperty(NGSIToMongo.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMongo.ENABLE_ENCODING).asBoolean();

        String fiwareServicePath = "/";
        String entityId = "someId";
        String entityType = "someType";
        String attribute = "someName";

        try {
            String collectionName = backend.buildCollectionName(fiwareServicePath, entityId,entityType,attribute,enableEncoding,collectionPrefix);
            String expectedCollectionName = "sth_/";

            try {
                assertEquals(expectedCollectionName, collectionName);
                System.out.println("[NGSIToMongo.buildCollectionName]"
                        + "-  OK  - '" + collectionName + "' was created as collection name");
            } catch (AssertionError e) {
                System.out.println("[NGSIToMongo.buildCollectionName]"
                        + "- FAIL - '" + collectionName + "' was created as collection name instead of 'sth_/'");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToMongo.buildCollectionName]"
                    + "- FAIL - There was some problem when building the collection name");
            assertTrue(false);
        } // catch
    } // testBuildCollectionNameDMByServicePathRootServicePathOldEncoding

    /**
     * [NGSIToMongo.buildCollectionName] -------- When / service-path is notified/defaulted and
     * data_model=dm-by-service-path, the MongoDB collection name is the concatenation of \<prefix\> and
     * \<service-path\>.
     */
    @Test
    public void testBuildCollectionNameDMByServicePathRootServicePathNewEncoding() {
        System.out.println("[NGSIToMongo.buildCollectionName]"
                + "-------- When / service-path is notified/defaulted and data_model=dm-by-service-path, "
                + "the MongoDB collection name is the concatenation of <prefix> and <service-path>");

        runner.setProperty(NGSIToMongo.COLLECTION_PREFIX, "sth_");
        String collectionPrefix = runner.getProcessContext().getProperty(NGSIToMongo.COLLECTION_PREFIX).getValue();
        runner.setProperty(NGSIToMongo.DB_PREFIX, "sth_");
        String dbPrefix = runner.getProcessContext().getProperty(NGSIToMongo.DB_PREFIX).getValue();
        runner.setProperty(NGSIToMongo.DATA_MODEL, "db-by-service-path");
        backend.setDataModel(runner.getProcessContext().getProperty(NGSIToMongo.DATA_MODEL).getValue());
        runner.setProperty(NGSIToMongo.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMongo.ENABLE_ENCODING).asBoolean();

        String fiwareServicePath = "/";
        String entityId = "someId";
        String entityType = "someType";
        String attribute = "someName";

        try {
            String collectionName = backend.buildCollectionName(fiwareServicePath, entityId,entityType, attribute,enableEncoding,collectionPrefix);
            String expectedCollectionName = "sth_x002f";

            try {
                assertEquals(expectedCollectionName, collectionName);
                System.out.println("[NGSIToMongo.buildCollectionName]"
                        + "-  OK  - '" + collectionName + "' was created as collection name");
            } catch (AssertionError e) {
                System.out.println("[NGSIToMongo.buildCollectionName]"
                        + "- FAIL - '" + collectionName + "' was created as collection name instead of 'sth_/'");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToMongo.buildCollectionName]"
                    + "- FAIL - There was some problem when building the collection name");
            assertTrue(false);
        } // catch
    } // testBuildCollectionNameDMByServicePathRootServicePathNewEncoding

    /**
     * [NGSIToMongo.buildCollectionName] -------- When / service-path is notified/defaulted and
     * data_model=dm-by-entity, the MongoDB collections name is the concatenation of the \<prefix\>, \<service-path\>,
     * \<entityId\> and \<entityType\>.
     */
    @Test
    public void testBuildCollectionNameDMByEntityRootServicePathOldEncoding() {
        System.out.println("[NGSIToMongo.buildCollectionName]"
                + "-------- When / service-path is notified/defaulted and data_model=dm-by-entity, the MongoDB"
                + "collections name is the concatenation of the <prefix>, <service-path>, <entityId> and <entityType>");

        runner.setProperty(NGSIToMongo.COLLECTION_PREFIX, "sth_");
        String collectionPrefix = runner.getProcessContext().getProperty(NGSIToMongo.COLLECTION_PREFIX).getValue();
        runner.setProperty(NGSIToMongo.DB_PREFIX, "sth_");
        String dbPrefix = runner.getProcessContext().getProperty(NGSIToMongo.DB_PREFIX).getValue();
        runner.setProperty(NGSIToMongo.DATA_MODEL, "db-by-entity");
        backend.setDataModel(runner.getProcessContext().getProperty(NGSIToMongo.DATA_MODEL).getValue());
        runner.setProperty(NGSIToMongo.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMongo.ENABLE_ENCODING).asBoolean();

        String fiwareServicePath = "/";
        String entityId = "someId";
        String entityType = "someType";
        String attribute = "someName";

        try {
            String collectionName = backend.buildCollectionName(fiwareServicePath, entityId,entityType, attribute,enableEncoding,collectionPrefix);
            String expectedCollectionName = "sth_/_someId_someType";

            try {
                assertEquals(expectedCollectionName, collectionName);
                System.out.println("[NGSIToMongo.buildCollectionName]"
                        + "-  OK  - '" + collectionName + "' was created as collection name");
            } catch (AssertionError e) {
                System.out.println("[NGSIToMongo.buildCollectionName]"
                        + "- FAIL - '" + collectionName + "' was created as collection name instead of "
                        + "'sth_/_someId_someType'");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToMongo.buildCollectionName]"
                    + "- FAIL - There was some problem when building the collection name");
            assertTrue(false);
        } // catch
    } // testBuildCollectionNameDMByEntityRootServicePathOldEncoding

    /**
     * [NGSIToMongo.buildCollectionName] -------- When / service-path is notified/defaulted and
     * data_model=dm-by-entity, the MongoDB collections name is the concatenation of the \<prefix\>, \<service-path\>,
     * \<entityId\> and \<entityType\>.
     */
    @Test
    public void testBuildCollectionNameDMByEntityRootServicePathNewEncoding() {
        System.out.println("[NGSIToMongo.buildCollectionName]"
                + "-------- When / service-path is notified/defaulted and data_model=dm-by-entity, the MongoDB"
                + "collections name is the concatenation of the <prefix>, <service-path>, <entityId> and <entityType>");

        runner.setProperty(NGSIToMongo.COLLECTION_PREFIX, "sth_");
        String collectionPrefix = runner.getProcessContext().getProperty(NGSIToMongo.COLLECTION_PREFIX).getValue();
        runner.setProperty(NGSIToMongo.DB_PREFIX, "sth_");
        runner.setProperty(NGSIToMongo.DATA_MODEL, "db-by-entity");
        backend.setDataModel(runner.getProcessContext().getProperty(NGSIToMongo.DATA_MODEL).getValue());
        runner.setProperty(NGSIToMongo.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMongo.ENABLE_ENCODING).asBoolean();

        String fiwareServicePath = "/";
        String entityId = "someId";
        String entityType = "someType";
        String attribute = "someName";

        try {
            String collectionName = backend.buildCollectionName(fiwareServicePath, entityId,entityType, attribute,enableEncoding,collectionPrefix);
            String expectedCollectionName = "sth_x002fxffffsomeIdxffffsomeType";

            try {
                assertEquals(expectedCollectionName, collectionName);
                System.out.println("[NGSIToMongo.buildCollectionName]"
                        + "-  OK  - '" + collectionName + "' was created as collection name");
            } catch (AssertionError e) {
                System.out.println("[NGSIToMongo.buildCollectionName]"
                        + "- FAIL - '" + collectionName + "' was created as collection name instead of "
                        + "'sth_/_someId_someType'");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToMongo.buildCollectionName]"
                    + "- FAIL - There was some problem when building the collection name");
            assertTrue(false);
        } // catch
    } // testBuildCollectionNameDMByEntityRootServicePathNewEncoding

    /**
     * [NGSIToMongo.buildCollectionName] -------- When / service-path is notified/defaulted and
     * data_model=dm-by-attribute, the MongoDB collections name is the concatenation of \<prefix\>, \<service-path\>,
     * \<entityId\>, \<entityType\> and \<attrName\>.
     */
    @Test
    public void testBuildCollectionNameDMByAttributeRootServicePathOldEncoding() {
        System.out.println("[NGSIToMongo.buildCollectionName]"
                + "-------- When / service-path is notified/defaulted and data_model=dm-by-attribute, the "
                + "MongoDB collections name is the concatenation of <prefix>, <service-path>, <entityId>, <entityType> "
                + "and <attrName>");

        runner.setProperty(NGSIToMongo.COLLECTION_PREFIX, "sth_");
        String collectionPrefix = runner.getProcessContext().getProperty(NGSIToMongo.COLLECTION_PREFIX).getValue();
        runner.setProperty(NGSIToMongo.DB_PREFIX, "sth_");
        runner.setProperty(NGSIToMongo.DATA_MODEL, "db-by-attribute");
        backend.setDataModel(runner.getProcessContext().getProperty(NGSIToMongo.DATA_MODEL).getValue());
        runner.setProperty(NGSIToMongo.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMongo.ENABLE_ENCODING).asBoolean();

        String fiwareServicePath = "/";
        String entityId = "someId";
        String entityType = "someType";
        String attribute = "someName";

        try {
            String collectionName = backend.buildCollectionName(fiwareServicePath, entityId,entityType, attribute,enableEncoding,collectionPrefix);
            String expectedCollectionName = "sth_/_someId_someType_someName";

            try {
                assertEquals(expectedCollectionName, collectionName);
                System.out.println("[NGSIToMongo.buildCollectionName]"
                        + "-  OK  - '" + collectionName + "' was created as collection name");
            } catch (AssertionError e) {
                System.out.println("[NGSIToMongo.buildCollectionName]"
                        + "- FAIL - '" + collectionName + "' was created as collection name instead of "
                        + "'sth_/_someId_someType_someName_someType'");
                throw e;
            } // try catch
        } catch (Exception e) {
            fail("[NGSIToMongo.buildCollectionName]"
                    + "- FAIL - There was some problem when building the collection name");
        } // catch
    } // testBuildCollectionNameDMByEntityRootServicePathOldEncoding

    /**
     * [NGSIToMongo.buildCollectionName] -------- When / service-path is notified/defaulted and
     * data_model=dm-by-attribute, the MongoDB collections name is the concatenation of \<prefix\>, \<service-path\>,
     * \<entityId\>, \<entityType\> and \<attrName\>.
     */
    @Test
    public void testBuildCollectionNameDMByAttributeRootServicePathNewEncoding() {
        System.out.println("[NGSIToMongo.buildCollectionName]"
                + "-------- When / service-path is notified/defaulted and data_model=dm-by-attribute, the "
                + "MongoDB collections name is the concatenation of <prefix>, <service-path>, <entityId>, <entityType> "
                + "and <attrName>");
        runner.setProperty(NGSIToMongo.COLLECTION_PREFIX, "sth_");
        String collectionPrefix = runner.getProcessContext().getProperty(NGSIToMongo.COLLECTION_PREFIX).getValue();
        runner.setProperty(NGSIToMongo.DB_PREFIX, "sth_");
        runner.setProperty(NGSIToMongo.DATA_MODEL, "db-by-attribute");
        backend.setDataModel(runner.getProcessContext().getProperty(NGSIToMongo.DATA_MODEL).getValue());
        runner.setProperty(NGSIToMongo.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMongo.ENABLE_ENCODING).asBoolean();

        String fiwareServicePath = "/";
        String entityId = "someId";
        String entityType = "someType";
        String attribute = "someName";

        try {
            String collectionName = backend.buildCollectionName(fiwareServicePath, entityId,entityType, attribute,enableEncoding,collectionPrefix);
            String expectedCollectionName = "sth_x002fxffffsomeIdxffffsomeTypexffffsomeName";

            try {
                assertEquals(expectedCollectionName, collectionName);
                System.out.println("[NGSIToMongo.buildCollectionName]"
                        + "-  OK  - '" + collectionName + "' was created as collection name");
            } catch (AssertionError e) {
                System.out.println("[NGSIToMongo.buildCollectionName]"
                        + "- FAIL - '" + collectionName + "' was created as collection name instead of "
                        + "'sth_/_someId_someType_someName_someType'");
                throw e;
            } // try catch
        } catch (Exception e) {
            fail("[NGSIToMongo.buildCollectionName]"
                    + "- FAIL - There was some problem when building the collection name");
        } // catch
    } // testBuildCollectionNameDMByEntityRootServicePathNewEncoding

    /**
     * [NGSIToMongo.buildDbName] -------- A database name length greater than 113 characters is detected.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildDbNameLength() throws Exception {
        System.out.println("[NGSIToMongo.buildDbName]"
                + "-------- A database name length greater than 113 characters is detected");

        runner.setProperty(NGSIToMongo.DB_PREFIX, "sth_");
        String dbPrefix = runner.getProcessContext().getProperty(NGSIToMongo.DB_PREFIX).getValue();
        runner.setProperty(NGSIToMongo.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMongo.ENABLE_ENCODING).asBoolean();

        String service = "tooLooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
                + "oooooooooooooooooooongService";

        try {
            backend.buildDbName(service,enableEncoding,dbPrefix);
            fail("[NGSIToMongo.buildDbName]"
                    + "- FAIL - A database name length greater than 113 characters has not been detected");
        } catch (Exception e) {
            System.out.println("[NGSIToMongo.buildDbName]"
                    + "-  OK  - A database name length greater than 113 characters has been detected");
        } // try catch
    } // testBuildDbNameLength

    /**
     * [NGSIToMongo.buildCollectionName] -------- When data model is by service path, a collection name length
     * greater than 113 characters is detected.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildCollectionNameLengthDataModelByServicePath() throws Exception {
        System.out.println("[NGSIToMongo.buildCollectionName]"
                + "-------- When data model is by service path, a collection name length greater than 113 characters "
                + "is detected");

        runner.setProperty(NGSIToMongo.COLLECTION_PREFIX, "sth_");
        String collectionPrefix = runner.getProcessContext().getProperty(NGSIToMongo.COLLECTION_PREFIX).getValue();
        runner.setProperty(NGSIToMongo.DB_PREFIX, "sth_");
        runner.setProperty(NGSIToMongo.DATA_MODEL, "db-by-service-path");
        backend.setDataModel(runner.getProcessContext().getProperty(NGSIToMongo.DATA_MODEL).getValue());
        runner.setProperty(NGSIToMongo.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMongo.ENABLE_ENCODING).asBoolean();

        String servicePath = "/tooLooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
                + "oooooooooooooooooooooooooooongServicePath";
        String entityId = "someId";
        String entityType = "someType";
        String attribute = "someName";

        try {
            backend.buildCollectionName(servicePath, entityId,entityType, attribute,enableEncoding,collectionPrefix);
            fail("[NGSIToMongo.buildCollectionName]"
                    + "- FAIL - A collection name length greater than 113 characters has not been detected");
        } catch (Exception e) {
            System.out.println("[NGSIToMongo.buildCollectionName]"
                    + "-  OK  - A collection name length greater than 113 characters has been detected");
        } // try catch
    } // testBuildCollectionNameLengthDataModelByServicePath

    /**
     * [NGSIToMongo.buildCollectionName] -------- When data model is by entity, a collection name length greater
     * than 113 characters is detected.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildCollectionNameLengthDataModelByEntity() throws Exception {
        System.out.println("[NGSIToMongo.buildCollectionName]"
                + "-------- When data model is by entity, a collection name length greater than 113 characters is "
                + "detected");

        runner.setProperty(NGSIToMongo.COLLECTION_PREFIX, "sth_");
        String collectionPrefix = runner.getProcessContext().getProperty(NGSIToMongo.COLLECTION_PREFIX).getValue();
        runner.setProperty(NGSIToMongo.DB_PREFIX, "sth_");
        runner.setProperty(NGSIToMongo.DATA_MODEL, "db-by-entity");
        backend.setDataModel(runner.getProcessContext().getProperty(NGSIToMongo.DATA_MODEL).getValue());
        runner.setProperty(NGSIToMongo.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMongo.ENABLE_ENCODING).asBoolean();

        String servicePath = "/tooLooooooooooooooooooooooooooooooooooooooooooooooooooooongServicePath";
        String entityId = "tooLooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongEntity";
        String entityType ="toooooooooooooooooooooooooLooooooooooooooooooooooongEntityType";
        String attribute = null;

        try {
            backend.buildCollectionName(servicePath, entityId,entityType, attribute,enableEncoding,collectionPrefix);
            fail("[NGSIToMongo.buildCollectionName]"
                    + "- FAIL - A collection name length greater than 113 characters has not been detected");
        } catch (Exception e) {
            System.out.println("[NGSIToMongo.buildCollectionName]"
                    + "-  OK  - A collection name length greater than 113 characters has been detected");
        } // try catch
    } // testBuildCollectionNameLengthDataModelByEntity

    /**
     * [NGSIToMongo.buildCollectionName] -------- When data model is by attribute, a collection name length
     * greater than 113 characters is detected.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildCollectionNameLengthDataModelByAttribute() throws Exception {
        System.out.println("[NGSIToMongo.buildCollectionName]"
                + "-------- When data model is by atribute, a collection name length greater than 113 characters is "
                + "detected");

        runner.setProperty(NGSIToMongo.COLLECTION_PREFIX, "sth_");
        String collectionPrefix = runner.getProcessContext().getProperty(NGSIToMongo.COLLECTION_PREFIX).getValue();
        runner.setProperty(NGSIToMongo.DB_PREFIX, "sth_");
        runner.setProperty(NGSIToMongo.DATA_MODEL, "db-by-attribute");
        backend.setDataModel(runner.getProcessContext().getProperty(NGSIToMongo.DATA_MODEL).getValue());
        runner.setProperty(NGSIToMongo.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToMongo.ENABLE_ENCODING).asBoolean();

        String servicePath = "/tooLoooooooooooooooooooooooooooooooooooongServicePath";
        String entityId = "tooLoooooooooooooooooooooooooooooooooooooooooongEntity";
        String entityType ="toooooooooooooooooooooooooLooooooooooooooooooooooongEntityType";
        String attribute = "tooLoooooooooooooooooooooooooooooooooooooooongAttribute";

        try {
            backend.buildCollectionName(servicePath, entityId,entityType, attribute,enableEncoding,collectionPrefix);
            fail("[NGSIToMongo.buildCollectionName]"
                    + "- FAIL - A collection name length greater than 113 characters has not been detected");
        } catch (Exception e) {
            System.out.println("[NGSIToMongo.buildCollectionName]"
                    + "-  OK  - A collection name length greater than 113 characters has been detected");
        } // try catch
    } // testBuildCollectionNameLengthDataModelByAttribute


}
