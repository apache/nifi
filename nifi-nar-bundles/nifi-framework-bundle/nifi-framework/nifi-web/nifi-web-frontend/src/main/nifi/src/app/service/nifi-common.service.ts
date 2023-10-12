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

import { Injectable } from '@angular/core';

@Injectable({
    providedIn: 'root'
})
export class NiFiCommon {
    constructor() {}

    /**
     * Extracts the contents of the specified str before the strToFind. If the
     * strToFind is not found or the first path of the str, an empty string is
     * returned.
     *
     * @argument {string} str       The full string
     * @argument {string} strToFind The substring to find
     */
    public substringBeforeFirst(str: string, strToFind: string) {
        let result = '';
        const indexOfStrToFind = str.indexOf(strToFind);
        if (indexOfStrToFind >= 0) {
            result = str.substring(0, indexOfStrToFind);
        }
        return result;
    }

    /**
     * Extracts the contents of the specified str after the strToFind. If the
     * strToFind is not found or the last part of the str, an empty string is
     * returned.
     *
     * @argument {string} str       The full string
     * @argument {string} strToFind The substring to find
     */
    public substringAfterFirst(str: string, strToFind: string) {
        var result = '';
        var indexOfStrToFind = str.indexOf(strToFind);
        if (indexOfStrToFind >= 0) {
            var indexAfterStrToFind = indexOfStrToFind + strToFind.length;
            if (indexAfterStrToFind < str.length) {
                result = str.substring(indexAfterStrToFind);
            }
        }
        return result;
    }

    /**
     * Extracts the contents of the specified str after the last strToFind. If the
     * strToFind is not found or the last part of the str, an empty string is
     * returned.
     *
     * @argument {string} str       The full string
     * @argument {string} strToFind The substring to find
     */
    public substringAfterLast(str: string, strToFind: string): string {
        let result = '';
        const indexOfStrToFind = str.lastIndexOf(strToFind);
        if (indexOfStrToFind >= 0) {
            const indexAfterStrToFind = indexOfStrToFind + strToFind.length;
            if (indexAfterStrToFind < str.length) {
                result = str.substring(indexAfterStrToFind);
            }
        }
        return result;
    }

    /**
     * Determines whether the specified string is blank (or null or undefined).
     *
     * @argument {string} str   The string to test
     */
    public isBlank(str: string) {
        if (str) {
            return str.trim().length === 0;
        }

        return true;
    }

    /**
     * Determines if the specified array is empty. If the specified arg is not an
     * array, then true is returned.
     *
     * @argument {array} arr    The array to test
     */
    public isEmpty(arr: any) {
        return Array.isArray(arr) ? arr.length === 0 : true;
    }

    /**
     * Formats the class name of this component.
     *
     * @param dataContext component datum
     */
    public formatClassName(dataContext: any): string {
        return this.substringAfterLast(dataContext.type, '.');
    }

    /**
     * Formats the type of this component.
     *
     * @param dataContext component datum
     */
    public formatType(dataContext: any): string {
        let typeString: string = this.formatClassName(dataContext);
        if (dataContext.bundle.version !== 'unversioned') {
            typeString += ' ' + dataContext.bundle.version;
        }
        return typeString;
    }

    /**
     * Formats the bundle label.
     *
     * @param bundle
     */
    public formatBundle(bundle: any): string {
        let groupString: string = '';
        if (bundle.group !== 'default') {
            groupString = bundle.group + ' - ';
        }
        return groupString + bundle.artifact;
    }

    /**
     * Constant regex for leading and/or trailing whitespace.
     */
    private static readonly LEAD_TRAIL_WHITE_SPACE_REGEX: RegExp = /^[ \s]+|[ \s]+$/;

    /**
     * Checks the specified value for leading and/or trailing whitespace only.
     *
     * @argument {string} value     The value to check
     */
    public hasLeadTrailWhitespace(value: string): boolean {
        if (this.isBlank(value)) {
            return false;
        }
        return NiFiCommon.LEAD_TRAIL_WHITE_SPACE_REGEX.test(value);
    }
}
