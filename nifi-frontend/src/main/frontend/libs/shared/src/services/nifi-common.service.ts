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
import { SelectOption } from '../types';

@Injectable({
    providedIn: 'root'
})
export class NiFiCommon {
    public static readonly EDIT_PARAMETER_CONTEXT_DIALOG_ID: string = 'edit-parameter-context-selected-index';

    /**
     * Constants for time duration formatting.
     */
    public static readonly MILLIS_PER_DAY: number = 86400000;
    public static readonly MILLIS_PER_HOUR: number = 3600000;
    public static readonly MILLIS_PER_MINUTE: number = 60000;
    public static readonly MILLIS_PER_SECOND: number = 1000;

    /**
     * Constants for formatting data size.
     */
    public static readonly BYTES_IN_KILOBYTE: number = 1024;
    public static readonly BYTES_IN_MEGABYTE: number = 1048576;
    public static readonly BYTES_IN_GIGABYTE: number = 1073741824;
    public static readonly BYTES_IN_TERABYTE: number = 1099511627776;

    public static readonly PACKAGE_SEPARATOR: string = '.';

    public static readonly TOOLTIP_DELAY_CLOSE_MILLIS: number = 400;
    public static readonly TOOLTIP_DELAY_OPEN_MILLIS: number = 500;

    private policyTypeListing: SelectOption[] = [
        {
            text: 'view the user interface',
            value: 'flow',
            description: 'Allows users to view the user interface'
        },
        {
            text: 'access the controller',
            value: 'controller',
            description:
                'Allows users to view/modify the controller including Management Controller Services, Reporting Tasks, Registry Clients, Parameter Providers and nodes in the cluster'
        },
        {
            text: 'access parameter contexts',
            value: 'parameter-contexts',
            description: 'Allows users to view/modify Parameter Contexts'
        },
        {
            text: 'query provenance',
            value: 'provenance',
            description: 'Allows users to submit a Provenance Search and request Event Lineage'
        },
        {
            text: 'access restricted components',
            value: 'restricted-components',
            description: 'Allows users to create/modify restricted components assuming other permissions are sufficient'
        },
        {
            text: 'access all policies',
            value: 'policies',
            description: 'Allows users to view/modify the policies for all components'
        },
        {
            text: 'access users/user groups',
            value: 'tenants',
            description: 'Allows users to view/modify the users and user groups'
        },
        {
            text: 'retrieve site-to-site details',
            value: 'site-to-site',
            description: 'Allows other NiFi instances to retrieve Site-To-Site details of this NiFi'
        },
        {
            text: 'view system diagnostics',
            value: 'system',
            description: 'Allows users to view System Diagnostics'
        },
        {
            text: 'proxy user requests',
            value: 'proxy',
            description: 'Allows proxy machines to send requests on the behalf of others'
        },
        {
            text: 'access counters',
            value: 'counters',
            description: 'Allows users to view/modify Counters'
        }
    ];

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
        let result = '';
        const indexOfStrToFind = str.indexOf(strToFind);
        if (indexOfStrToFind >= 0) {
            const indexAfterStrToFind = indexOfStrToFind + strToFind.length;
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
     * Get the label for a Component Type using the simple name from a qualified class
     *
     * @argument {string} componentType Qualified class name or simple name
     */
    public getComponentTypeLabel(componentType: string): string {
        let label = componentType;

        const separatorIndex = componentType.indexOf(NiFiCommon.PACKAGE_SEPARATOR);
        if (separatorIndex >= 0) {
            label = this.substringAfterLast(componentType, NiFiCommon.PACKAGE_SEPARATOR);
        }

        return label;
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
     * Determines if the specified object is defined and not null.
     *
     * @argument {object} obj   The object to test
     */
    public isDefinedAndNotNull(obj: any) {
        return !this.isUndefined(obj) && !this.isNull(obj);
    }

    /**
     * Determines if the specified object is undefined.
     *
     * @argument {object} obj   The object to test
     */
    public isUndefined(obj: any) {
        return typeof obj === 'undefined';
    }

    /**
     * Determines if the specified object is null.
     *
     * @argument {object} obj   The object to test
     */
    public isNull(obj: any) {
        return obj === null;
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

    public isNumber(obj: any) {
        if (!obj) {
            return false;
        }
        if (typeof obj === 'number') {
            return true;
        }
        if (obj instanceof Number) {
            return true;
        }
        return typeof obj === 'string' && !isNaN(parseInt(obj, 10));
    }

    /**
     * Determines if a string contains another, optionally looking case insensitively.
     *
     * @param stringToSearch
     * @param stringToFind
     * @param caseInsensitive
     */
    public stringContains(
        stringToSearch: string | null | undefined,
        stringToFind: string | null | undefined,
        caseInsensitive = false
    ): boolean {
        if (this.isBlank(stringToSearch)) {
            return false;
        }
        if (this.isBlank(stringToFind)) {
            return true;
        }
        if (caseInsensitive) {
            // @ts-ignore
            return stringToSearch.toLowerCase().indexOf(stringToFind.toLowerCase()) >= 0;
        }
        // @ts-ignore
        return stringToSearch.indexOf(stringToFind) >= 0;
    }

    /**
     * Formats the class name of this component.
     *
     * @param dataContext component datum
     */
    public formatClassName(dataContext: any): string {
        return this.getComponentTypeLabel(dataContext.type);
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
        let groupString = '';
        if (bundle.group !== 'default') {
            groupString = bundle.group + ' - ';
        }
        return groupString + bundle.artifact;
    }

    /**
     * Compares two strings.
     *
     * @param a
     * @param b
     */
    public compareString(a: string | null | undefined, b: string | null | undefined): number {
        if (a == b) {
            return 0;
        }
        return (a || '').localeCompare(b || '');
    }

    /**
     * Compares two numbers.
     *
     * @param a
     * @param b
     */
    public compareNumber(a: number | null | undefined, b: number | null | undefined): number {
        // nulls last
        return (
            (this.isDefinedAndNotNull(a) ? a || 0 : Number.MIN_VALUE) -
            (this.isDefinedAndNotNull(b) ? b || 0 : Number.MIN_VALUE)
        );
    }

    public compareVersion(aRawVersion: string, bRawVersion: string): number {
        if (aRawVersion === bRawVersion) {
            return 0;
        }

        // attempt to parse the raw strings
        const aTokens = aRawVersion.split(/-/);
        const bTokens = bRawVersion.split(/-/);

        // ensure there is at least one token
        if (aTokens.length >= 1 && bTokens.length >= 1) {
            const aVersionTokens = aTokens[0].split(/\./);
            const bVersionTokens = bTokens[0].split(/\./);

            // ensure both versions have at least one token
            if (aVersionTokens.length >= 1 && bVersionTokens.length >= 1) {
                // find the number of tokens a and b have in common
                const commonTokenLength = Math.min(aVersionTokens.length, bVersionTokens.length);

                // consider all tokens in common
                for (let i = 0; i < commonTokenLength; i++) {
                    const aVersionSegment = parseInt(aVersionTokens[i], 10);
                    const bVersionSegment = parseInt(bVersionTokens[i], 10);

                    // if both are non-numeric, consider the next token
                    if (isNaN(aVersionSegment) && isNaN(bVersionSegment)) {
                        continue;
                    } else if (isNaN(aVersionSegment)) {
                        // NaN is considered less
                        return -1;
                    } else if (isNaN(bVersionSegment)) {
                        // NaN is considered less
                        return 1;
                    }

                    // if a version at any point does not match
                    if (aVersionSegment !== bVersionSegment) {
                        return aVersionSegment - bVersionSegment;
                    }
                }

                if (aVersionTokens.length === bVersionTokens.length) {
                    if (aTokens.length === bTokens.length) {
                        // same version for all tokens so consider the trailing bits (1.1-RC vs 1.1-SNAPSHOT)
                        const aExtraBits = this.substringAfterFirst(aRawVersion, aTokens[0]);
                        const bExtraBits = this.substringAfterFirst(bRawVersion, bTokens[0]);
                        return aExtraBits === bExtraBits ? 0 : aExtraBits > bExtraBits ? 1 : -1;
                    } else {
                        // in this case, extra bits means it's consider less than no extra bits (1.1 vs 1.1-SNAPSHOT)
                        return bTokens.length - aTokens.length;
                    }
                } else {
                    // same version for all tokens in common (ie 1.1 vs 1.1.1)
                    return aVersionTokens.length - bVersionTokens.length;
                }
            } else if (aVersionTokens.length >= 1) {
                // presence of version tokens is considered greater
                return 1;
            } else if (bVersionTokens.length >= 1) {
                // presence of version tokens is considered greater
                return -1;
            } else {
                return 0;
            }
        } else if (aTokens.length >= 1) {
            // presence of tokens is considered greater
            return 1;
        } else if (bTokens.length >= 1) {
            // presence of tokens is considered greater
            return -1;
        } else {
            return 0;
        }
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
     * @param {number} value
     * @param {number} width
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
    parseDateTime(rawDateTime: string | null | undefined): Date {
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
        let milliseconds = 0;
        if (secondsSpec.length === 2) {
            milliseconds = parseInt(secondsSpec[1], 10);
        }
        return new Date(year, month, day, hours, minutes, seconds, milliseconds);
    }

    /**
     * Parses the specified duration and returns the total number of millis.
     *
     * @param {string} rawDuration
     * @returns {number}        The number of millis
     */
    parseDuration(rawDuration: string) {
        const duration = rawDuration.split(/:/);

        // ensure the appropriate number of tokens
        if (duration.length !== 3) {
            return 0;
        }

        // detect if there is millis
        const seconds = duration[2].split(/\./);
        if (seconds.length === 2) {
            return new Date(
                1970,
                0,
                1,
                parseInt(duration[0], 10),
                parseInt(duration[1], 10),
                parseInt(seconds[0], 10),
                parseInt(seconds[1], 10)
            ).getTime();
        } else {
            return new Date(
                1970,
                0,
                1,
                parseInt(duration[0], 10),
                parseInt(duration[1], 10),
                parseInt(duration[2], 10),
                0
            ).getTime();
        }
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

    /**
     * Formats the specified number of bytes into a human readable string.
     *
     * @param {number} dataSize
     * @returns {string}
     */
    public formatDataSize(dataSize: number): string {
        let dataSizeToFormat: number = parseFloat(`${dataSize / NiFiCommon.BYTES_IN_TERABYTE}`);
        if (dataSizeToFormat > 1) {
            return dataSizeToFormat.toFixed(2) + ' TB';
        }

        // check gigabytes
        dataSizeToFormat = parseFloat(`${dataSize / NiFiCommon.BYTES_IN_GIGABYTE}`);
        if (dataSizeToFormat > 1) {
            return dataSizeToFormat.toFixed(2) + ' GB';
        }

        // check megabytes
        dataSizeToFormat = parseFloat(`${dataSize / NiFiCommon.BYTES_IN_MEGABYTE}`);
        if (dataSizeToFormat > 1) {
            return dataSizeToFormat.toFixed(2) + ' MB';
        }

        // check kilobytes
        dataSizeToFormat = parseFloat(`${dataSize / NiFiCommon.BYTES_IN_KILOBYTE}`);
        if (dataSizeToFormat > 1) {
            return dataSizeToFormat.toFixed(2) + ' KB';
        }

        // default to bytes
        return parseFloat(`${dataSize}`).toFixed(2) + ' bytes';
    }

    /**
     * Formats the specified integer as a string (adding commas). At this
     * point this does not take into account any locales.
     *
     * @param {integer} integer
     * @param {boolean} useCompactNotation - Whether to use compact notation (100K, 1M, etc.) for large numbers
     */
    public formatInteger(integer: number, useCompactNotation: boolean = false): string {
        const locale: string = (navigator && navigator.language) || 'en';

        // For values >= 100,000 and when compact notation is requested, use compact notation (100K, 1M, 2.5B, etc.)
        if (useCompactNotation && Math.abs(integer) >= 100000) {
            return new Intl.NumberFormat(locale, {
                notation: 'compact',
                maximumFractionDigits: 2
            }).format(integer);
        }

        // For all other cases, use the traditional format with commas
        return integer.toLocaleString(locale, { maximumFractionDigits: 0 });
    }

    /**
     * Formats the specified float using two decimal places.
     *
     * @param {float} f
     */
    public formatFloat(f: number): string {
        if (!f) {
            return '0.0';
        }
        const locale: string = (navigator && navigator.language) || 'en';
        return f.toLocaleString(locale, { maximumFractionDigits: 2, minimumFractionDigits: 2 });
    }

    /**
     * Gets the policy type for the specified resource.
     *
     * @param value
     */
    public getPolicyTypeListing(value: string): SelectOption | undefined {
        return this.policyTypeListing.find((policy: SelectOption) => value === policy.value);
    }

    /**
     * Gets all policy types for every global resource.
     */
    public getAllPolicyTypeListing(): SelectOption[] {
        return this.policyTypeListing;
    }

    /**
     * The NiFi model contain the url for each component. That URL is an absolute URL. Angular CSRF handling
     * does not work on absolute URLs, so we need to strip off the proto for the request header to be added.
     *
     * https://stackoverflow.com/a/59586462
     *
     * @param url
     * @private
     */
    public stripProtocol(url: string): string {
        return this.substringAfterFirst(url, ':');
    }
}
