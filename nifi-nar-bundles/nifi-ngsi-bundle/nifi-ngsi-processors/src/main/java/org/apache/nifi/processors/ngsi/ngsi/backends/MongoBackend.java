package org.apache.nifi.processors.ngsi.ngsi.backends;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.processors.ngsi.ngsi.utils.CommonConstants;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSICharsets;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSIConstants;
import org.bson.Document;


public class MongoBackend {

    /**
     * Available resolutions for aggregated data.
     */
    public enum Resolution { SECOND, MINUTE, HOUR, DAY, MONTH }

    private MongoClient client;
    private MongoClientURI mongoURI;
    private String dataModel;

    public MongoClient getClient() {
        return client;
    }

    public void setClient(MongoClient client) {
        this.client = client;
    }

    public MongoClientURI getMongoURI() {
        return mongoURI;
    }

    public String getDataModel() {
        return dataModel;
    }

    public void setDataModel(String dataModel) {
        this.dataModel=dataModel;
    }

    /**
     * Constructor.
     * @param mongoURI
     * @param dataModel
     */
    public MongoBackend(MongoClientURI mongoURI, String dataModel) {
        this.client = null;
        this.mongoURI = mongoURI;
        this.dataModel = dataModel;
    } // MongoBackendImpl

    /**
     * Creates a database, given its name, if not exists.
     * @param dbName
     * @throws Exception
     */
    public void createDatabase(String dbName){
        System.out.println("Creating Mongo database=" + dbName);
        getDatabase(dbName); // getting a non existent database automatically creates it
    } // createDatabase

    /**
     * Creates a collection for STH Comet, given its name, if not exists in the given database. Time-based limits are set,
     * if possible.
     * @param dbName
     * @param collectionName
     * @param dataExpiration
     * @throws Exception
     */
    public void createCollection(String dbName, String collectionName, long dataExpiration) {
        System.out.println("Creating Mongo collection=" + collectionName + " at database=" + dbName);
        MongoDatabase db = getDatabase(dbName);

        // create the collection
        try {
            db.createCollection(collectionName);
        } catch (Exception e) {
            if (e.getMessage().contains("\"code\" : 48")) {
                System.out.println("Collection already exists, nothing to create");
            } else {
                throw e;
            } // if else
        } // try catch

        // ensure the _id.origin index, if possible
        try {
            if (dataExpiration != 0) {
                BasicDBObject keys = new BasicDBObject().append("_id.origin", 1);
                IndexOptions options = new IndexOptions().expireAfter(dataExpiration, TimeUnit.SECONDS);
                db.getCollection(collectionName).createIndex(keys, options);
            } // if
        } catch (Exception e) {
            throw e;
        } // try catch
    } // createCollection

    /**
     * Creates a collection for plain MongoDB, given its name, if not exists in the given database. Size-based limits
     * are set, if possible. Time-based limits are also set, if possible.
     * @param dbName
     * @param collectionName
     * @param collectionsSize
     * @param maxDocuments
     * @throws Exception
     */
    public void createCollection(String dbName, String collectionName, long collectionsSize, long maxDocuments,
                                 long dataExpiration) throws Exception {
        MongoDatabase db = getDatabase(dbName);

        // create the collection, with size-based limits if possible
        try {
            if (collectionsSize != 0 && maxDocuments != 0) {
                CreateCollectionOptions options = new CreateCollectionOptions()
                        .capped(true)
                        .sizeInBytes(collectionsSize)
                        .maxDocuments(maxDocuments);
                System.out.println("Creating Mongo collection=" + collectionName + " at database=" + dbName + " with "
                        + "collections_size=" + collectionsSize + " and max_documents=" + maxDocuments + " options");
                db.createCollection(collectionName, options);
            } else {
                System.out.println("Creating Mongo collection=" + collectionName + " at database=" + dbName);
                db.createCollection(collectionName);
            } // if else
        } catch (Exception e) {
            if (e.getMessage().contains("\"code\" : 48")) {
                System.out.println("Collection already exists, nothing to create");
            } else {
                throw e;
            } // if else
        } // try catch

        // ensure the recvTime index, if possible
        try {
            if (dataExpiration != 0) {
                BasicDBObject keys = new BasicDBObject().append("recvTime", 1);
                IndexOptions options = new IndexOptions().expireAfter(dataExpiration, TimeUnit.SECONDS);
                db.getCollection(collectionName).createIndex(keys, options);
            } // if
        } catch (Exception e) {
            throw e;
        } // try catch
    } // createCollection

    /**
     * Inserts a new document in the given raw collection within the given database (row-like mode).
     * @param dbName
     * @param collectionName
     * @param aggregation
     * @throws Exception
     */
    public void insertContextDataRaw(String dbName, String collectionName, ArrayList<Document> aggregation)
             {
        MongoDatabase db = getDatabase(dbName);
        MongoCollection collection = db.getCollection(collectionName);
        collection.insertMany(aggregation);
    } // insertContextDataRaw


    public void insertContextDataAggregated(String dbName, String collectionName, long recvTimeTs, String entityId,
                                            String entityType, String attrName, String attrType, double max, double min, double sum, double sum2,
                                            int numSamples, boolean[] resolutions) throws Exception {
        // Preprocess some values
        GregorianCalendar calendar = new GregorianCalendar();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.setTimeInMillis(recvTimeTs);

        // Insert the data in an aggregated fashion for each resolution type
        if (resolutions[0]) {
            insertContextDataAggregatedForResoultion(dbName, collectionName, calendar, entityId, entityType,
                    attrName, attrType, max, min, sum, sum2, numSamples, Resolution.SECOND);
        } // if

        if (resolutions[1]) {
            insertContextDataAggregatedForResoultion(dbName, collectionName, calendar, entityId, entityType,
                    attrName, attrType, max, min, sum, sum2, numSamples, Resolution.MINUTE);
        } // if

        if (resolutions[2]) {
            insertContextDataAggregatedForResoultion(dbName, collectionName, calendar, entityId, entityType,
                    attrName, attrType, max, min, sum, sum2, numSamples, Resolution.HOUR);
        } // if

        if (resolutions[3]) {
            insertContextDataAggregatedForResoultion(dbName, collectionName, calendar, entityId, entityType,
                    attrName, attrType, max, min, sum, sum2, numSamples, Resolution.DAY);
        } // if

        if (resolutions[4]) {
            insertContextDataAggregatedForResoultion(dbName, collectionName, calendar, entityId, entityType,
                    attrName, attrType, max, min, sum, sum2, numSamples, Resolution.MONTH);
        } // if
    } // insertContextDataAggregated

    public void insertContextDataAggregated(String dbName, String collectionName, long recvTimeTs, String entityId,
                                            String entityType, String attrName, String attrType, HashMap<String, Integer> counts,
                                            boolean[] resolutions) throws Exception {
        // Preprocess some values
        GregorianCalendar calendar = new GregorianCalendar();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.setTimeInMillis(recvTimeTs);

        // Insert the data in an aggregated fashion for each resolution type
        if (resolutions[0]) {
            insertContextDataAggregatedForResoultion(dbName, collectionName, calendar, entityId, entityType,
                    attrName, attrType, counts, Resolution.SECOND);
        } // if

        if (resolutions[1]) {
            insertContextDataAggregatedForResoultion(dbName, collectionName, calendar, entityId, entityType,
                    attrName, attrType, counts, Resolution.MINUTE);
        } // if

        if (resolutions[2]) {
            insertContextDataAggregatedForResoultion(dbName, collectionName, calendar, entityId, entityType,
                    attrName, attrType, counts, Resolution.HOUR);
        } // if

        if (resolutions[3]) {
            insertContextDataAggregatedForResoultion(dbName, collectionName, calendar, entityId, entityType,
                    attrName, attrType, counts, Resolution.DAY);
        } // if

        if (resolutions[4]) {
            insertContextDataAggregatedForResoultion(dbName, collectionName, calendar, entityId, entityType,
                    attrName, attrType, counts, Resolution.MONTH);
        } // if
    } // insertContextDataAggregated

    private void insertContextDataAggregatedForResoultion(String dbName, String collectionName,
                                                          GregorianCalendar calendar, String entityId, String entityType, String attrName, String attrType,
                                                          double max, double min, double sum, double sum2, int numSamples, Resolution resolution) {
        // Get database and collection
        MongoDatabase db = getDatabase(dbName);
        MongoCollection collection = db.getCollection(collectionName);

        // Build the query
        BasicDBObject query = buildQueryForInsertAggregated(calendar, entityId, entityType, attrName, resolution);

        // Prepopulate if needed
        BasicDBObject insert = buildInsertForPrepopulate(attrType, resolution, true);
        UpdateResult res = collection.updateOne(query, insert, new UpdateOptions().upsert(true));

        if (res.getMatchedCount() == 0) {
            System.out.println("Prepopulating data, database=" + dbName + ", collection=" + collectionName + ", query="
                    + query.toString() + ", insert=" + insert.toString());
        } // if

        // Do the update
        BasicDBObject update = buildUpdateForUpdate(attrType, calendar, max, min, sum, sum2, numSamples);
        System.out.println("Updating data, database=" + dbName + ", collection=" + collectionName + ", query="
                + query.toString() + ", update=" + update.toString());
        collection.updateOne(query, update);
    } // insertContextDataAggregated

    private void insertContextDataAggregatedForResoultion(String dbName, String collectionName,
                                                          GregorianCalendar calendar, String entityId, String entityType, String attrName, String attrType,
                                                          HashMap<String, Integer> counts, Resolution resolution) {
        // Get database and collection
        MongoDatabase db = getDatabase(dbName);
        MongoCollection collection = db.getCollection(collectionName);

        // Build the query
        BasicDBObject query = buildQueryForInsertAggregated(calendar, entityId, entityType, attrName, resolution);

        // Prepopulate if needed
        BasicDBObject insert = buildInsertForPrepopulate(attrType, resolution, false);
        UpdateResult res = collection.updateOne(query, insert, new UpdateOptions().upsert(true));

        if (res.getMatchedCount() == 0) {
            System.out.println("Prepopulating data, database=" + dbName + ", collection=" + collectionName + ", query="
                    + query.toString() + ", insert=" + insert.toString());
        } // if

        // Do the update
        for (String key : counts.keySet()) {
            int count = counts.get(key);
            BasicDBObject update = buildUpdateForUpdate(attrType, resolution, calendar, key, count);
            System.out.println("Updating data, database=" + dbName + ", collection=" + collectionName + ", query="
                    + query.toString() + ", update=" + update.toString());
            collection.updateOne(query, update);
        } // for
    } // insertContextDataAggregated

    /**
     * Builds the Json query used both to prepopulate and update an aggregated collection. It is protected for testing
     * purposes.
     * @param calendar
     * @param entityId
     * @param entityType
     * @param attrName
     * @param resolution
     * @return
     */
    protected BasicDBObject buildQueryForInsertAggregated(GregorianCalendar calendar, String entityId, String entityType,
                                                          String attrName, Resolution resolution) {
        int offset = getOffset(calendar, resolution);
        BasicDBObject query = new BasicDBObject();

        switch (dataModel) {
            case "db-by-service-path":
                query.append("_id", new BasicDBObject("entityId", entityId)
                        .append("entityType", entityType)
                        .append("attrName", attrName)
                        .append("origin", getOrigin(calendar, resolution))
                        .append("resolution", resolution.toString().toLowerCase())
                        .append("range", getRange(resolution)))
                        .append("points.offset", offset);
                break;
            case "db-by-entity":
                query.append("_id", new BasicDBObject("attrName", attrName)
                        .append("origin", getOrigin(calendar, resolution))
                        .append("resolution", resolution.toString().toLowerCase())
                        .append("range", getRange(resolution)))
                        .append("points.offset", offset);
                break;
            case "db-by-attribute":
                query.append("_id", new BasicDBObject("origin", getOrigin(calendar, resolution))
                        .append("resolution", resolution.toString().toLowerCase())
                        .append("range", getRange(resolution)))
                        .append("points.offset", offset);
                break;
            default:
                break;
                // this will never be reached
        } // switch

        return query;
    } // buildQueryForInsertAggregated

    /**
     * Builds the Json to be inserted as prepopulation. It is protected for testing purposes.
     * @param attrType
     * @param resolution
     * @param isANumber
     * @return
     */
    protected BasicDBObject buildInsertForPrepopulate(String attrType, Resolution resolution, boolean isANumber) {
        BasicDBObject update = new BasicDBObject();
        update.append("$setOnInsert", new BasicDBObject("attrType", attrType)
                .append("points", buildPrepopulatedPoints(resolution, isANumber)));
        return update;
    } // buildInsertForPrepopulate

    /**
     * Builds the points part for the Json used to prepopulate.
     * @param resolution
     */
    private BasicDBList buildPrepopulatedPoints(Resolution resolution, boolean isANumber) {
        BasicDBList prepopulatedData = new BasicDBList();
        int offsetOrigin = 0;
        int numValues = 0;

        switch (resolution) {
            case SECOND:
                numValues = 60;
                break;
            case MINUTE:
                numValues = 60;
                break;
            case HOUR:
                numValues = 24;
                break;
            case DAY:
                numValues = 32;
                offsetOrigin = 1;
                break;
            case MONTH:
                numValues = 13;
                offsetOrigin = 1;
                break;
            default:
                // should never be reached
        } // switch

        if (isANumber) {
            for (int i = offsetOrigin; i < numValues; i++) {
                prepopulatedData.add(new BasicDBObject("offset", i)
                        .append("samples", 0)
                        .append("sum", 0)
                        .append("sum2", 0)
                        .append("min", Double.POSITIVE_INFINITY)
                        .append("max", Double.NEGATIVE_INFINITY));
            } // for
        } else {
            for (int i = offsetOrigin; i < numValues; i++) {
                prepopulatedData.add(new BasicDBObject("offset", i)
                        .append("samples", 0)
                        .append("occur", new BasicDBObject()));
            } // for
        } // if else

        return prepopulatedData;
    } // buildPrepopulatedPoints

    protected BasicDBObject buildUpdateForUpdate(String attrType, GregorianCalendar calendar,
                                                 double max, double min, double sum, double sum2, int numSamples) {
        BasicDBObject update = new BasicDBObject();
        return update.append("$set", new BasicDBObject("attrType", attrType))
                .append("$inc", new BasicDBObject("points.$.samples", numSamples)
                        .append("points.$.sum", sum)
                        .append("points.$.sum2", sum2))
                .append("$min", new BasicDBObject("points.$.min", min))
                .append("$max", new BasicDBObject("points.$.max", max));
    } // buildUpdateForUpdate

    protected BasicDBObject buildUpdateForUpdate(String attrType, Resolution resolution, GregorianCalendar calendar,
                                                 String value, int numSamples) {
        BasicDBObject update = new BasicDBObject();
        int offset = getOffset(calendar, resolution);
        int modifiedOffset = offset - (resolution == Resolution.DAY || resolution == Resolution.MONTH ? 1 : 0);
        update.append("$set", new BasicDBObject("attrType", attrType))
                .append("$inc", new BasicDBObject("points." + modifiedOffset + ".samples", numSamples)
                        .append("points." + modifiedOffset + ".occur." + value, numSamples));
        return update;
    } // buildUpdateForUpdate

    /**
     * Gets a Mongo database.
     * @param dbName
     * @return
     */
    public MongoDatabase getDatabase(String dbName) {
        // create a Mongo client
        if (client == null) {
            client = new MongoClient(mongoURI);
        }
        // get the database
        return client.getDatabase(dbName);
    } // getDatabase

    /**
     * Given a resolution, gets the range. It is protected for testing purposes.
     * @param resolution
     * @return
     */
    protected String getRange(Resolution resolution) {
        switch(resolution) {
            case SECOND:
                return "minute";
            case MINUTE:
                return "hour";
            case HOUR:
                return "day";
            case DAY:
                return "month";
            case MONTH:
                return "year";
            default:
                return null; // this should never be returned
        } // switch
    } // getDataModel

    /**
     * Given a calendar and a resolution, gets the origin. It is protected for testing purposes.
     * @param calendar
     * @param resolution
     * @return
     */
    protected Date getOrigin(GregorianCalendar calendar, Resolution resolution) {
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE);
        int second;

        switch (resolution) {
            case MONTH:
                month = 0;
                // falls through
            case DAY:
                day = 1;
                // falls through
            case HOUR:
                hour = 0;
                // falls through
            case MINUTE:
                minute = 0;
                // falls through
            case SECOND:
                second = 0;
                break;
            default:
                // should never be reached
                return null;

        } // switch

        GregorianCalendar gc = new GregorianCalendar(year, month, day, hour, minute, second);
        gc.setTimeZone(TimeZone.getTimeZone("UTC"));
        return new Date(gc.getTimeInMillis());
    } // getOrigin

    /**
     * Given a calendar and a resolution, gets the offset. It is protected for testing purposes.
     * @param calendar
     * @param resolution
     * @return
     */
    protected int getOffset(GregorianCalendar calendar, Resolution resolution) {
        int offset;

        switch (resolution) {
            case SECOND:
                offset = calendar.get(Calendar.SECOND);
                break;
            case MINUTE:
                offset = calendar.get(Calendar.MINUTE);
                break;
            case HOUR:
                offset = calendar.get(Calendar.HOUR_OF_DAY);
                break;
            case DAY:
                offset = calendar.get(Calendar.DAY_OF_MONTH);
                break;
            case MONTH:
                // This offset has to be modified since Java data classes enum months starting by 0 (January)
                offset = calendar.get(Calendar.MONTH) + 1;
                break;
            default:
                // should never be reached
                offset = 0;
                break;
        } // switch

        return offset;
    } // getOffset

    /**
     * Builds a database name given a fiwareService. It throws an exception if the naming conventions are violated.
     * @param fiwareService
     * @return
     */
    public String buildDbName(String fiwareService,boolean enableEncoding,String dbPrefix) throws MongoException {
        String dbName;

        if (enableEncoding) {
            dbName = NGSICharsets.encodeMongoDBDatabase(dbPrefix) + NGSICharsets.encodeMongoDBDatabase(fiwareService);
        } else {
            dbName = NGSICharsets.encodeSTHDB(dbPrefix) + NGSICharsets.encodeSTHDB(fiwareService);
        } // if else

        if (dbName.length() > NGSIConstants.MONGO_DB_MAX_NAMESPACE_SIZE_IN_BYTES) {
            throw new MongoException ("Building database name '" + dbName + "' and its length is greater "
                    + "than " + NGSIConstants.MONGO_DB_MAX_NAMESPACE_SIZE_IN_BYTES);
        } // if

        return dbName;
    } // buildDbName

    /**
     * Builds a collection name given a fiwareServicePath and a destination. It throws an exception if the naming
     * conventions are violated.
     * @param fiwareServicePath
     * @param entity
     * @param attribute
     * @return
     */
    public String buildCollectionName(String fiwareServicePath, String entity, String entityType, String attribute,boolean enableEncoding,String collectionPrefix) throws Exception
    {
        String collectionName="";

        if (enableEncoding) {
            switch (dataModel) {
                case "db-by-service-path":
                    collectionName = NGSICharsets.encodeMongoDBCollection(collectionPrefix) +NGSICharsets.encodeMongoDBCollection(fiwareServicePath);
                    break;
                case "db-by-entity":
                    collectionName = NGSICharsets.encodeMongoDBCollection(collectionPrefix) +NGSICharsets.encodeMongoDBCollection(fiwareServicePath)
                            + CommonConstants.CONCATENATOR
                            + NGSICharsets.encodeMongoDBCollection(entity)
                            + CommonConstants.CONCATENATOR
                            + NGSICharsets.encodeMongoDBCollection(entityType);
                    break;
                case "db-by-attribute":
                    collectionName = NGSICharsets.encodeMongoDBCollection(collectionPrefix) +NGSICharsets.encodeMongoDBCollection(fiwareServicePath)
                            + CommonConstants.CONCATENATOR
                            + NGSICharsets.encodeMongoDBCollection(entity)
                            + CommonConstants.CONCATENATOR
                            + NGSICharsets.encodeMongoDBCollection(entityType)
                            + CommonConstants.CONCATENATOR
                            + NGSICharsets.encodeMongoDBCollection(attribute);
                    break;
                default:
                    System.out.println("Unknown data model '" + dataModel
                            + "'. Please, use dm-by-service-path, dm-by-entity or dm-by-attribute");
            } // switch
        } else {
            switch (dataModel) {
                case "db-by-service-path":
                    collectionName = NGSICharsets.encodeSTHCollection(collectionPrefix) +NGSICharsets.encodeSTHCollection(fiwareServicePath);
                    break;
                case "db-by-entity":
                    collectionName = NGSICharsets.encodeSTHCollection(collectionPrefix) +NGSICharsets.encodeSTHCollection(fiwareServicePath) + "_"
                            + NGSICharsets.encodeSTHCollection(entity)+"_"
                            + NGSICharsets.encodeSTHCollection(entityType);
                    break;
                case "db-by-attribute":
                    collectionName = NGSICharsets.encodeSTHCollection(collectionPrefix) +NGSICharsets.encodeSTHCollection(fiwareServicePath)
                            + "_" + NGSICharsets.encodeSTHCollection(entity)
                            + "_" + NGSICharsets.encodeSTHCollection(entityType)
                            + "_" + NGSICharsets.encodeSTHCollection(attribute);
                    break;
                default:
                    // this should never be reached
                    collectionName = null;
                    break;
            } // switch
        } // else

        if (collectionName.getBytes().length > NGSIConstants.MONGO_DB_MAX_NAMESPACE_SIZE_IN_BYTES) {
            throw new MongoException("Building collection name '" + collectionName + "' and its length is "
                    + "greater than " + NGSIConstants.MONGO_DB_MAX_NAMESPACE_SIZE_IN_BYTES);
        } // if

        return collectionName;
    } // buildCollectionName

    /*#####################################################################*/

    private Document createDocWithMetadata(Long recvTimeTs, String entityId, String entityType, String attrName,
                                           String attrType, String attrValue, String attrMetadata) {
        Document doc = new Document("recvTime", new Date(recvTimeTs));

        switch (dataModel) {
            case "db-by-service-path":
                doc.append("entityId", entityId)
                        .append("entityType", entityType)
                        .append("attrName", attrName)
                        .append("attrType", attrType)
                        .append("attrValue", attrValue)
                        .append("attrMetadata", BasicDBObject.parse(attrMetadata));
                break;
            case "db-by-entity":
                doc.append("attrName", attrName)
                        .append("attrType", attrType)
                        .append("attrValue", attrValue)
                        .append("attrMetadata", BasicDBObject.parse(attrMetadata));
                break;
            case "db-by-attribute":
                doc.append("attrType", attrType)
                        .append("attrValue", attrValue)
                        .append("attrMetadata", BasicDBObject.parse(attrMetadata));
                break;
            default:
                return null; // this will never be reached
        } // switch

        return doc;
    } // createDocWithMetadata

    public void close(){
        client.close();
    }

    private Document createDoc(long recvTimeTs, String entityId, String entityType, String attrName,
                               String attrType, String attrValue) {
        Document doc = new Document("recvTime", new Date(recvTimeTs));

        switch (dataModel) {
            case "db-by-service-path":
                doc.append("entityId", entityId)
                        .append("entityType", entityType)
                        .append("attrName", attrName)
                        .append("attrType", attrType)
                        .append("attrValue", attrValue);
                break;
            case "db-by-entity":
                doc.append("attrName", attrName)
                        .append("attrType", attrType)
                        .append("attrValue", attrValue);
                break;
            case "db-by-attribute":
                doc.append("attrType", attrType)
                        .append("attrValue", attrValue);
                break;
            default:
                return null; // this will never be reached
        } // switch

        return doc;
    } // createDoc
} // MongoBackendImpl