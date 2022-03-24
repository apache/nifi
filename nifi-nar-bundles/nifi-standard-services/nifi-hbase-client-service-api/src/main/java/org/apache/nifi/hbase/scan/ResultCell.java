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
package org.apache.nifi.hbase.scan;

public class ResultCell {

    byte[] rowArray;
    int rowOffset;
    short rowLength;

    byte[] familyArray;
    int familyOffset;
    byte familyLength;

    byte[] qualifierArray;
    int qualifierOffset;
    int qualifierLength;

    long timestamp;
    byte typeByte;
    long sequenceId;

    byte[] valueArray;
    int valueOffset;
    int valueLength;

    byte[] tagsArray;
    int tagsOffset;
    int tagsLength;

    public byte[] getRowArray() {
        return rowArray;
    }

    public void setRowArray(byte[] rowArray) {
        this.rowArray = rowArray;
    }

    public int getRowOffset() {
        return rowOffset;
    }

    public void setRowOffset(int rowOffset) {
        this.rowOffset = rowOffset;
    }

    public short getRowLength() {
        return rowLength;
    }

    public void setRowLength(short rowLength) {
        this.rowLength = rowLength;
    }

    public byte[] getFamilyArray() {
        return familyArray;
    }

    public void setFamilyArray(byte[] familyArray) {
        this.familyArray = familyArray;
    }

    public int getFamilyOffset() {
        return familyOffset;
    }

    public void setFamilyOffset(int familyOffset) {
        this.familyOffset = familyOffset;
    }

    public byte getFamilyLength() {
        return familyLength;
    }

    public void setFamilyLength(byte familyLength) {
        this.familyLength = familyLength;
    }

    public byte[] getQualifierArray() {
        return qualifierArray;
    }

    public void setQualifierArray(byte[] qualifierArray) {
        this.qualifierArray = qualifierArray;
    }

    public int getQualifierOffset() {
        return qualifierOffset;
    }

    public void setQualifierOffset(int qualifierOffset) {
        this.qualifierOffset = qualifierOffset;
    }

    public int getQualifierLength() {
        return qualifierLength;
    }

    public void setQualifierLength(int qualifierLength) {
        this.qualifierLength = qualifierLength;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public byte getTypeByte() {
        return typeByte;
    }

    public void setTypeByte(byte typeByte) {
        this.typeByte = typeByte;
    }

    public long getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(long sequenceId) {
        this.sequenceId = sequenceId;
    }

    public byte[] getValueArray() {
        return valueArray;
    }

    public void setValueArray(byte[] valueArray) {
        this.valueArray = valueArray;
    }

    public int getValueOffset() {
        return valueOffset;
    }

    public void setValueOffset(int valueOffset) {
        this.valueOffset = valueOffset;
    }

    public int getValueLength() {
        return valueLength;
    }

    public void setValueLength(int valueLength) {
        this.valueLength = valueLength;
    }

    public byte[] getTagsArray() {
        return tagsArray;
    }

    public void setTagsArray(byte[] tagsArray) {
        this.tagsArray = tagsArray;
    }

    public int getTagsOffset() {
        return tagsOffset;
    }

    public void setTagsOffset(int tagsOffset) {
        this.tagsOffset = tagsOffset;
    }

    public int getTagsLength() {
        return tagsLength;
    }

    public void setTagsLength(int tagsLength) {
        this.tagsLength = tagsLength;
    }
}
