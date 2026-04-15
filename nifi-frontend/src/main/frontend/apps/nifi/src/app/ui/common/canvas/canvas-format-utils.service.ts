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
export class CanvasFormatUtils {
    // Constants for byte conversion (matches NiFiCommon)
    private static readonly BYTES_IN_KILOBYTE = 1024;
    private static readonly BYTES_IN_MEGABYTE = 1048576;
    private static readonly BYTES_IN_GIGABYTE = 1073741824;
    private static readonly BYTES_IN_TERABYTE = 1099511627776;

    public substringBeforeFirst(str: string, delimiter: string): string {
        if (!str) return '';
        const index = str.indexOf(delimiter);
        return index >= 0 ? str.substring(0, index) : str;
    }

    public substringAfterFirst(str: string, delimiter: string): string {
        if (!str) return '';
        const index = str.indexOf(delimiter);
        return index >= 0 ? str.substring(index + delimiter.length) : '';
    }

    public formatInteger(value: number): string {
        if (isNaN(value)) {
            return '0';
        }

        const isNegative = value < 0;
        const absoluteValue = Math.abs(value);

        // Convert to string and add commas
        const parts = absoluteValue.toString().split('.');
        parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ',');

        return (isNegative ? '-' : '') + parts.join('.');
    }

    public formatDataSize(bytes: number): string {
        if (bytes === 0) {
            return '0 bytes';
        }

        let formatted = parseFloat(`${bytes / CanvasFormatUtils.BYTES_IN_TERABYTE}`);
        if (formatted > 1) {
            return formatted.toFixed(2) + ' TB';
        }

        formatted = parseFloat(`${bytes / CanvasFormatUtils.BYTES_IN_GIGABYTE}`);
        if (formatted > 1) {
            return formatted.toFixed(2) + ' GB';
        }

        formatted = parseFloat(`${bytes / CanvasFormatUtils.BYTES_IN_MEGABYTE}`);
        if (formatted > 1) {
            return formatted.toFixed(2) + ' MB';
        }

        formatted = parseFloat(`${bytes / CanvasFormatUtils.BYTES_IN_KILOBYTE}`);
        if (formatted > 1) {
            return formatted.toFixed(2) + ' KB';
        }

        // Default to bytes
        return parseFloat(`${bytes}`).toFixed(2) + ' bytes';
    }

    public formatSizeString(sizeStr: string): string {
        // Parse size string (e.g., "5678901 bytes")
        const match = sizeStr.match(/^([\d.]+)\s*(\w+)$/);
        if (!match) {
            return sizeStr; // Return as-is if we can't parse
        }

        const value = parseFloat(match[1]);
        const unit = match[2].toLowerCase();

        // If already formatted with units other than bytes, return as-is
        if (unit !== 'bytes' && unit !== 'byte') {
            return sizeStr;
        }

        // Format bytes to appropriate unit
        return this.formatDataSize(value);
    }

    public formatQueuedStats(queuedValue: string): { count: string; size: string } {
        // Parse the queued value: "count (size)"
        const match = queuedValue.match(/^(\d+)\s*\(([^)]+)\)$/);

        if (!match) {
            // Fallback for unexpected format - use substring methods
            const count = this.substringBeforeFirst(queuedValue, ' ');
            const size = this.substringAfterFirst(queuedValue, ' ');
            return { count, size };
        }

        const count = parseInt(match[1], 10);
        const sizeStr = match[2].trim();

        // Format count with commas
        const formattedCount = this.formatInteger(count);

        // Parse and format size
        const formattedSize = this.formatSizeString(sizeStr);

        return {
            count: formattedCount,
            size: ` (${formattedSize})`
        };
    }
}
