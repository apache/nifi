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
package org.apache.nifi.wrapping.wrapper;

import org.apache.nifi.wrapping.wrapper.WrapperFactory.CheckSum;
import org.apache.nifi.wrapping.wrapper.WrapperFactory.MaskLength;
import org.apache.nifi.wrapping.wrapper.WrapperFactory.TechniqueType;

/**
 * Class holding the individual properties required to perform the Cloaked Dagger wrapping.
 */
public class WrapperProperties {

    /**
     * Individual Properties.
     */
    private MaskLength finalMaskLength = null;
    private TechniqueType finalTechType = null;
    private CheckSum finalBodyCs = null;
    private CheckSum finalHeaderCs = null;
    private boolean noBodyCs = false;
    private boolean noHeaderCs = false;

    /*
     * Constructor 1. No Parameters.
     */
    public WrapperProperties() {
        super();
    }

    /**
     * Constructor 2. With all parameters.
     *
     * @param finalMaskLength
     *            - new value for parameter.
     * @param finalTechType
     *            - new value for parameter.
     * @param finalBodyCs
     *            - new value for parameter.
     * @param finalHeaderCs
     *            - new value for parameter.
     * @param noBodyCs
     *            - new value for parameter.
     * @param noHeaderCs
     *            - new value for parameter.
     */
    public WrapperProperties(final MaskLength finalMaskLength, final TechniqueType finalTechType,
                    final CheckSum finalBodyCs, final CheckSum finalHeaderCs, final boolean noBodyCs,
                    final boolean noHeaderCs) {
        this();
        this.finalMaskLength = finalMaskLength;
        this.finalTechType = finalTechType;
        this.finalBodyCs = finalBodyCs;
        this.finalHeaderCs = finalHeaderCs;
        this.noBodyCs = noBodyCs;
        this.noHeaderCs = noHeaderCs;
    }

    /**
     * Get Method.
     *
     * @return the finalMaskLength
     */
    public MaskLength getFinalMaskLength() {
        return finalMaskLength;
    }

    /**
     * Set Method.
     *
     * @param finalMaskLength
     *            the finalMaskLength to set
     */
    public void setFinalMaskLength(MaskLength finalMaskLength) {
        this.finalMaskLength = finalMaskLength;
    }

    /**
     * Get Method.
     *
     * @return the finalTechType
     */
    public TechniqueType getFinalTechType() {
        return finalTechType;
    }

    /**
     * Set Method.
     *
     * @param finalTechType
     *            the finalTechType to set
     */
    public void setFinalTechType(TechniqueType finalTechType) {
        this.finalTechType = finalTechType;
    }

    /**
     * Get Method.
     *
     * @return the finalBodyCs
     */
    public CheckSum getFinalBodyCs() {
        return finalBodyCs;
    }

    /**
     * Set Method.
     *
     * @param finalBodyCs
     *            the finalBodyCs to set
     */
    public void setFinalBodyCs(CheckSum finalBodyCs) {
        this.finalBodyCs = finalBodyCs;
    }

    /**
     * Get Method.
     *
     * @return the finalHeaderCs
     */
    public CheckSum getFinalHeaderCs() {
        return finalHeaderCs;
    }

    /**
     * Set Method.
     *
     * @param finalHeaderCs
     *            the finalHeaderCs to set
     */
    public void setFinalHeaderCs(CheckSum finalHeaderCs) {
        this.finalHeaderCs = finalHeaderCs;
    }

    /**
     * Get Method.
     *
     * @return the noBodyCs
     */
    public boolean isNoBodyCs() {
        return noBodyCs;
    }

    /**
     * Set Method.
     *
     * @param noBodyCs
     *            the noBodyCs to set
     */
    public void setNoBodyCs(boolean noBodyCs) {
        this.noBodyCs = noBodyCs;
    }

    /**
     * Get Method.
     *
     * @return the noHeaderCs
     */
    public boolean isNoHeaderCs() {
        return noHeaderCs;
    }

    /**
     * Set Method.
     *
     * @param noHeaderCs
     *            the noHeaderCs to set
     */
    public void setNoHeaderCs(boolean noHeaderCs) {
        this.noHeaderCs = noHeaderCs;
    }

}
