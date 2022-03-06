package org.apache.nifi.processors.ngsi.ngsi.aggregators;

import com.mongodb.BasicDBObject;
import java.util.ArrayList;
import java.util.Date;
import org.apache.nifi.processors.ngsi.ngsi.backends.MongoBackend;
import org.apache.nifi.processors.ngsi.ngsi.utils.Attributes;
import org.apache.nifi.processors.ngsi.ngsi.utils.Entity;
import org.bson.Document;


public abstract class MongoAggregator {


        // string containing the data fieldValues
        protected ArrayList<Document> aggregation;
        protected Entity entity;
        protected String dataModel;
        protected long creationTime;

        public MongoAggregator() {
            aggregation = new ArrayList<>();
        } // MongoDBAggregator

        public ArrayList<Document> getAggregation() {
            return aggregation;
        } // getAggregation

        public void initialize(Entity entity, long creationTime,String dataModel) {
            this.entity=entity;
            this.creationTime=creationTime;
            this.dataModel=dataModel;
        } // initialize

        public abstract void aggregate(Entity entity, long creationTime,String dataModel);

    // MongoDBAggregator

    /**
     * Class for aggregating batches in row mode.
     */
    public class RowAggregator extends MongoAggregator {

        @Override
        public void initialize(Entity entity, long creationTime,String dataModel) {
            super.initialize(entity,creationTime,dataModel);
        } // initialize

        @Override
        public void aggregate(Entity entity, long creationTime,String dataModel) {
            // get the event headers
            long notifiedRecvTimeTs = creationTime;
            // get the event body
                String entityId = entity.getEntityId();
                String entityType = entity.getEntityType();
                // iterate on all this context element attributes, if there are attributes
                ArrayList<Attributes> contextAttributes = entity.getEntityAttrs();

                for (Attributes contextAttribute : contextAttributes) {
                    String attrName = contextAttribute.getAttrName();
                    String attrType = contextAttribute.getAttrType();
                    String attrValue = contextAttribute.getAttrValue();
                    String attrMetadata = (contextAttribute.getAttrMetadata() != null) ? contextAttribute.getMetadataString() : "{}";

                    // check if the metadata contains a TimeInstant value; use the notified reception time instead
                    Long recvTimeTs;
                    Long timeInstant = null;

                    if (timeInstant != null) {
                        recvTimeTs = timeInstant;
                    } else {
                        recvTimeTs = notifiedRecvTimeTs;
                    } // if else

                    Document doc;
                    doc = createDocWithMetadata(recvTimeTs, entityId, entityType, attrName, attrType, attrValue, attrMetadata, dataModel);

                    aggregation.add(doc);
                } // for
        } // aggregate

        private Document createDocWithMetadata(Long recvTimeTs, String entityId, String entityType, String attrName,
                                               String attrType, String attrValue, String attrMetadata, String dataModel) {
            Document doc = new Document("recvTime", new Date(recvTimeTs));

            switch (dataModel) {
                case "db-by-service-path":
                    doc.append("entityId", entityId)
                            .append("entityType", entityType)
                            .append("attrName", attrName)
                            .append("attrType", attrType)
                            .append("attrValue", attrValue)
                            .append("attrMetadata",  BasicDBObject.parse(attrMetadata));
                    break;
                case "db-by-entity":
                    doc.append("attrName", attrName)
                            .append("attrType", attrType)
                            .append("attrValue", attrValue)
                            .append("attrMetadata",  BasicDBObject.parse(attrMetadata));
                    break;
                case "db-by-attribute":
                    doc.append("attrType", attrType)
                            .append("attrValue", attrValue)
                            .append("attrMetadata",  BasicDBObject.parse(attrMetadata));
                    break;
                default:
                    return null; // this will never be reached
            } // switch

            return doc;
        } // createDocWithMetadata

        private Document createDoc(long recvTimeTs, String entityId, String entityType, String attrName,
                                   String attrType, String attrValue,String dataModel) {
            Document doc = new Document("recvTime", new Date(recvTimeTs));

            switch (dataModel) {
                case "db-by-service-path":
                    doc.append("entityId", entityId)
                            .append("entityType", entityType)
                            .append("attrName", attrName)
                            .append("attrType", attrType)
                            .append("attrValue", attrValue);
                    System.out.println(doc);
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

    } // RowAggregator

    /**
     * Class for aggregating batches in column mode.
     */
    public class ColumnAggregator extends MongoAggregator {

        @Override
        public void initialize(Entity entity, long creationTime,String dataModel) {
            super.initialize(entity,creationTime,dataModel);
        } // initialize

        @Override
        public void aggregate(Entity entity, long creationTime,String dataModel) {
            long recvTimeTs = creationTime;

            // get the event body
                String entityId = entity.getEntityId();
                String entityType = entity.getEntityType();
            // iterate on all this context element attributes, if there are attributes
                ArrayList<Attributes> contextAttributes = entity.getEntityAttrs();
                Document doc = createDoc(recvTimeTs, entityId, entityType, dataModel);

            for (Attributes contextAttribute : contextAttributes) {
                    String attrName = contextAttribute.getAttrName();
                    String attrType = contextAttribute.getAttrType();
                    String attrValue = contextAttribute.getAttrValue();
                    doc.append( attrName,attrValue);
                } // for
                aggregation.add(doc);
        } // aggregate

        private Document createDoc(long recvTimeTs, String entityId, String entityType,String dataModel) {
            Document doc = new Document("recvTime", new Date(recvTimeTs));
            doc.append("recvTimeTs", recvTimeTs);

            switch (dataModel) {
                case "db-by-service-path":
                    doc.append("entityId", entityId).append("entityType", entityType);
                    break;
                case "db-by-entity":
                    break;
                case "db-by-attribute":
                    return null; // this will never be reached
                default:
                    return null; // this will never be reached
            } // switch

            return doc;
        } // createDoc

    } // ColumnAggregator

    public MongoAggregator getAggregator(boolean rowAttrPersistence) {
        if (rowAttrPersistence) {
            return new RowAggregator();
        } else {
            return new ColumnAggregator();
        } // if else
    } // getAggregator

    public void persistAggregation(MongoAggregator aggregator, String db, String collection, boolean enableLowercase, MongoBackend backend, long collectionsSize, long maxDocuments, long dataExpiration) {
        ArrayList<Document> aggregation = aggregator.getAggregation();

        String dbName = (enableLowercase)? db.toLowerCase():db;
        String collectionName = (enableLowercase)?collection.toLowerCase():collection;

        try {
            backend.createDatabase(dbName);
            if (backend.getDatabase(dbName).getCollection(collectionName)==null) {
                backend.createCollection(dbName, collectionName, collectionsSize, maxDocuments, dataExpiration);
            }
            backend.insertContextDataRaw(dbName, collectionName, aggregation);
        } catch (Exception e) {
            System.out.println("-, " + e.getMessage());
        } // try catch
    } // persistAggregation
}
