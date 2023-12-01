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
    private static readonly MILLIS_PER_DAY: number = 86400000;
    private static readonly MILLIS_PER_HOUR: number = 3600000;
    private static readonly MILLIS_PER_MINUTE: number = 60000;
    private static readonly MILLIS_PER_SECOND: number = 1000;

    constructor() {}

    /**
     * Extracts the contents of the specified str before the last strToFind. If the
     * strToFind is not found or the first part of the str, an empty string is
     * returned.
     *
     * @argument {string} str       The full string
     * @argument {string} strToFind The substring to find
     */
    public substringBeforeLast(str: string, strToFind: string): string {
        let result = '';
        const indexOfStrToFind = str.lastIndexOf(strToFind);
        if (indexOfStrToFind >= 0) {
            result = str.substring(0, indexOfStrToFind);
        }
        return result;
    }

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
    public isBlank(str: string | null | undefined) {
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

    /**
     * Pads the specified value to the specified width with the specified character.
     * If the specified value is already wider than the specified width, the original
     * value is returned.
     *
     * @param {integer} value
     * @param {integer} width
     * @param {string} character
     * @returns {string}
     */
    pad(value: number, width: number, character: string): string {
        let s: string = value + '';

        // pad until wide enough
        while (s.length < width) {
            s = character + s;
        }

        return s;
    }

    /**
     * Formats the specified DateTime.
     *
     * @param {Date} date
     * @returns {String}
     */
    formatDateTime(date: Date): string {
        return (
            this.pad(date.getMonth() + 1, 2, '0') +
            '/' +
            this.pad(date.getDate(), 2, '0') +
            '/' +
            this.pad(date.getFullYear(), 2, '0') +
            ' ' +
            this.pad(date.getHours(), 2, '0') +
            ':' +
            this.pad(date.getMinutes(), 2, '0') +
            ':' +
            this.pad(date.getSeconds(), 2, '0') +
            '.' +
            this.pad(date.getMilliseconds(), 3, '0')
        );
    }

    /**
     * Parses the specified date time into a Date object. The resulting
     * object does not account for timezone and should only be used for
     * performing relative comparisons.
     *
     * @param {string} rawDateTime
     * @returns {Date}
     */
    parseDateTime(rawDateTime: string): Date {
        // handle non date values
        if (!rawDateTime) {
            return new Date();
        }

        // parse the date time
        const dateTime: string[] = rawDateTime.split(/ /);

        // ensure the correct number of tokens
        if (dateTime.length !== 3) {
            return new Date();
        }

        // get the date and time
        const date: string[] = dateTime[0].split(/\//);
        const time: string[] = dateTime[1].split(/:/);

        // ensure the correct number of tokens
        if (date.length !== 3 || time.length !== 3) {
            return new Date();
        }
        const year: number = parseInt(date[2], 10);
        const month: number = parseInt(date[0], 10) - 1; // new Date() accepts months 0 - 11
        const day: number = parseInt(date[1], 10);
        const hours: number = parseInt(time[0], 10);
        const minutes: number = parseInt(time[1], 10);

        // detect if there is millis
        const secondsSpec: string[] = time[2].split(/\./);
        const seconds: number = parseInt(secondsSpec[0], 10);
        let milliseconds: number = 0;
        if (secondsSpec.length === 2) {
            milliseconds = parseInt(secondsSpec[1], 10);
        }
        return new Date(year, month, day, hours, minutes, seconds, milliseconds);
    }

    /**
     * Formats the specified duration.
     *
     * @param {number} millis in millis
     */
    formatDuration(millis: number): string {
        // don't support sub millisecond resolution
        let duration: number = millis < 1 ? 0 : millis;

        // determine the number of days in the specified duration
        let days: number = duration / NiFiCommon.MILLIS_PER_DAY;
        days = days >= 1 ? Math.trunc(days) : 0;
        duration %= NiFiCommon.MILLIS_PER_DAY;

        // remaining duration should be less than 1 day, get number of hours
        let hours: number = duration / NiFiCommon.MILLIS_PER_HOUR;
        hours = hours >= 1 ? Math.trunc(hours) : 0;
        duration %= NiFiCommon.MILLIS_PER_HOUR;

        // remaining duration should be less than 1 hour, get number of minutes
        let minutes: number = duration / NiFiCommon.MILLIS_PER_MINUTE;
        minutes = minutes >= 1 ? Math.trunc(minutes) : 0;
        duration %= NiFiCommon.MILLIS_PER_MINUTE;

        // remaining duration should be less than 1 minute, get number of seconds
        let seconds: number = duration / NiFiCommon.MILLIS_PER_SECOND;
        seconds = seconds >= 1 ? Math.trunc(seconds) : 0;

        // remaining duration is the number millis (don't support sub millisecond resolution)
        duration = Math.floor(duration % NiFiCommon.MILLIS_PER_SECOND);

        // format the time
        const time =
            this.pad(hours, 2, '0') +
            ':' +
            this.pad(minutes, 2, '0') +
            ':' +
            this.pad(seconds, 2, '0') +
            '.' +
            this.pad(duration, 3, '0');

        // only include days if appropriate
        if (days > 0) {
            return days + ' days and ' + time;
        } else {
            return time;
        }
    }
}
