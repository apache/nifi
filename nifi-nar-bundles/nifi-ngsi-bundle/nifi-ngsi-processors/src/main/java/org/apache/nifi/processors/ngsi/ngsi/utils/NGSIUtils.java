package org.apache.nifi.processors.ngsi.ngsi.utils;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;


public class NGSIUtils {

    public NGSIEvent getEventFromFlowFile(FlowFile flowFile, final ProcessSession session, String version){

        final byte[] buffer = new byte[(int) flowFile.getSize()];

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, buffer);
            }
        });
        // Create the PreparedStatement to use for this FlowFile.
        Map flowFileAttributes = flowFile.getAttributes();
        Map <String,String> newFlowFileAttributes = new CaseInsensitiveMap(flowFileAttributes);
        final String flowFileContent = new String(buffer, StandardCharsets.UTF_8);
        String fiwareService = (newFlowFileAttributes.get("fiware-service") == null) ? "nd":newFlowFileAttributes.get("fiware-service");
        String fiwareServicePath = (newFlowFileAttributes.get("fiware-servicepath")==null) ? "/nd":newFlowFileAttributes.get("fiware-servicepath");
        System.out.println(fiwareServicePath);
        long creationTime=flowFile.getEntryDate();
        JSONObject content = new JSONObject(flowFileContent);
        JSONArray data;
        String entityType;
        String entityId;
        ArrayList<Entity> entities = new ArrayList<>();
        NGSIEvent event= null;

        if("v2".compareToIgnoreCase(version)==0){
            data = (JSONArray) content.get("data");
            for (int i = 0; i < data.length(); i++) {
                JSONObject lData = data.getJSONObject(i);
                entityId = lData.getString("id");
                entityType = lData.getString("type");
                ArrayList<Attributes> attrs  = new ArrayList<>();
                Iterator<String> keys = lData.keys();
                while (keys.hasNext()) {
                    String key = keys.next();
                    if (!"id".equals(key) && !"type".equals(key)){
                        JSONObject value = lData.getJSONObject(key);
                        JSONObject mtdo = (JSONObject) value.get("metadata");
                        Iterator<String> keysOneLevel=mtdo.keys();
                        String metadataString = value.get("metadata").toString();
                        ArrayList<Metadata>  mtd = new ArrayList<>();
                        while (keysOneLevel.hasNext()) {
                            String keyOne = keysOneLevel.next();
                            JSONObject value2 = mtdo.getJSONObject(keyOne);
                            mtd.add(new Metadata(keyOne,value2.getString("type"),value2.get("value").toString()));
                        }
                        if(mtdo.length()<=0){
                            attrs.add(new Attributes(key,value.getString("type"),value.get("value").toString(),null,""));
                        }else{
                            attrs.add(new Attributes(key,value.getString("type"),value.get("value").toString(),mtd,metadataString));
                        }
                    }
                }
                entities.add(new Entity(entityId,entityType,attrs));
            }
            event = new NGSIEvent(creationTime,fiwareService,fiwareServicePath,entities);
        }else if ("ld".compareToIgnoreCase(version)==0){
            System.out.println("NGSI-LD Notification");
            boolean hasSubAttrs= false;
            data = (JSONArray) content.get("data");
            for (int i = 0; i < data.length(); i++) {
                JSONObject lData = data.getJSONObject(i);
                entityId = lData.getString("id");
                entityType = lData.getString("type");
                ArrayList<AttributesLD> attributes  = new ArrayList<>();
                Iterator<String> keys = lData.keys();
                String attrType="";
                String attrValue="";
                String subAttrName="";
                String subAttrType="";
                String subAttrValue="";
                ArrayList<AttributesLD> subAttributes=new ArrayList<>();

                while (keys.hasNext()) {
                    String key = keys.next();
                    if (!"id".equals(key) && !"type".equals(key) && !"@context".equals(key)){
                        JSONObject value = lData.getJSONObject(key);
                        attrType = value.getString("type");
                        if ("Relationship".contentEquals(attrType)){
                            attrValue = value.get("object").toString();
                        }else if ("Property".contentEquals(attrType)){
                            attrValue = value.get("value").toString();
                            Iterator<String> keysOneLevel = value.keys();
                            while (keysOneLevel.hasNext()) {
                                String keyOne = keysOneLevel.next();
                                if ("type".equals(keyOne)){
                                    // Do Nothing
                                } else if ("observedAt".equals(keyOne) || "unitCode".equals(keyOne)){
                                    // TBD Do Something for unitCode and observedAt
                                    String value2 = value.getString(keyOne);
                                    subAttrName = keyOne;
                                    subAttrValue = value2;
                                    hasSubAttrs = true;
                                    subAttributes.add(new AttributesLD(subAttrName,subAttrValue,subAttrValue,false,null));
                                } else if (!"value".equals(keyOne)){
                                    JSONObject value2 = value.getJSONObject(keyOne);
                                    subAttrName=keyOne;
                                    subAttrType=value2.get("type").toString();
                                    if ("Relationship".contentEquals(subAttrType)){
                                        subAttrValue = value2.get("object").toString();
                                    }else if ("Property".contentEquals(subAttrType)){
                                        subAttrValue = value2.get("value").toString();
                                    }else if ("GeoProperty".contentEquals(subAttrType)){
                                        subAttrValue = value2.get("value").toString();
                                    }
                                    hasSubAttrs= true;
                                    subAttributes.add(new AttributesLD(subAttrName,subAttrType,subAttrValue,false,null));
                                }
                            }
                        }else if ("GeoProperty".contentEquals(attrType)){
                            attrValue = value.get("value").toString();
                        }
                        attributes.add(new AttributesLD(key,attrType,attrValue, hasSubAttrs,subAttributes));
                        subAttributes=new ArrayList<>();
                        hasSubAttrs= false;
                    }
                }
                entities.add(new Entity(entityId,entityType,attributes,true));
            }
            event = new NGSIEvent(creationTime,fiwareService,entities);
        }
        return event;
    }
}