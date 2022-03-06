package org.apache.nifi.processors.ngsi.ngsi.utils;

import java.util.ArrayList;

public class NGSIEvent {
    public long creationTime;
    public String fiwareService;
    public String fiwareServicePath;
    public ArrayList <Entity> entities;
    public ArrayList <Entity> entitiesLD;


    public NGSIEvent(long creationTime, String fiwareService, String fiwareServicePath, ArrayList<Entity> entities) {
        this.creationTime = creationTime;
        this.fiwareService = fiwareService;
        this.fiwareServicePath = fiwareServicePath;
        this.entities = entities;
    }

    public NGSIEvent(long creationTime, String fiwareService, ArrayList<Entity> entitiesLD ){
        this.creationTime = creationTime;
        this.fiwareService = fiwareService;
        this.entitiesLD = entitiesLD;
    }

    public ArrayList<Entity> getEntitiesLD() {
        return entitiesLD;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public String getFiwareService() {
        return fiwareService;
    }

    public void setFiwareService(String fiwareService) {
        this.fiwareService = fiwareService;
    }

    public String getFiwareServicePath() {
        return fiwareServicePath;
    }

    public void setFiwareServicePath(String fiwareServicePath) {
        this.fiwareServicePath = fiwareServicePath;
    }

    public ArrayList<Entity> getEntities() {
        return entities;
    }

    public void setEntities(ArrayList<Entity> entities) {
        this.entities = entities;
    }

    public long getRecvTimeTs() {
        return new Long(this.creationTime);
    } // getRecvTimeTs

}
