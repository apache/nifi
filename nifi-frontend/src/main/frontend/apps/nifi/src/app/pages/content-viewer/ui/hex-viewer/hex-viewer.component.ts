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

import { Component } from '@angular/core';
import { ContentViewerService } from '../../service/content-viewer.service';
import { HttpErrorResponse } from '@angular/common/http';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { isDefinedAndNotNull, NiFiCommon } from '@nifi/shared';
import { NiFiState } from '../../../../state';
import { Store } from '@ngrx/store';
import { selectRef } from '../../state/content/content.selectors';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Component({
    selector: 'hex-viewer',
    templateUrl: './hex-viewer.component.html',
    imports: [],
    styleUrls: ['./hex-viewer.component.scss']
})
export class HexViewer {
    private static readonly KB_COUNT = 10;

    private static readonly MAX_CONTENT_BYTES = HexViewer.KB_COUNT * NiFiCommon.BYTES_IN_KILOBYTE;
    private static readonly OFFSET_LENGTH: number = 8;
    private static readonly HEX_LENGTH: number = 48;
    private static readonly ASCII_LENGTH: number = 16;

    hexdump: string = '';
    error: string | null = null;

    constructor(
        private store: Store<NiFiState>,
        private contentViewerService: ContentViewerService,
        private errorHelper: ErrorHelper
    ) {
        this.store
            .select(selectRef)
            .pipe(isDefinedAndNotNull(), takeUntilDestroyed())
            .subscribe((ref) => {
                this.contentViewerService.getBlob(ref, 0, HexViewer.MAX_CONTENT_BYTES).subscribe({
                    next: (blob: Blob) => {
                        blob.arrayBuffer().then((buffer) => {
                            const data = new Uint8Array(buffer);
                            const lines: string[] = [];

                            for (let i = 0; i < data.length; i += 16) {
                                const block = data.slice(i, i + 16);

                                const hex: string[] = [];
                                const ascii: string[] = [];

                                // go through each value in this block
                                block.forEach((value) => {
                                    // convert the value to hex and pad if necessary
                                    hex.push(value.toString(16).padStart(2, '0'));

                                    // get the character code if the value is printable, '.' otherwise
                                    if (value >= 0x20 && value < 0x7f) {
                                        ascii.push(String.fromCharCode(value));
                                    } else {
                                        ascii.push('.');
                                    }
                                });

                                // format the offset string
                                const offsetString = i.toString(16).padStart(HexViewer.OFFSET_LENGTH, '0');

                                // format the hex string
                                let hexString;
                                if (hex.length > 8) {
                                    hexString = hex.slice(0, 8).join(' ') + '  ' + hex.slice(8).join(' ');
                                } else {
                                    hexString = hex.join(' ');
                                }
                                hexString = hexString.padEnd(HexViewer.HEX_LENGTH);

                                // format the ascii string
                                const asciiString = ascii.join('').padEnd(HexViewer.ASCII_LENGTH);

                                // build and record the line
                                const line = `${offsetString}  ${hexString}  |${asciiString}|`;
                                lines.push(line);
                            }

                            this.hexdump = lines.join('\n');
                        });
                    },
                    error: (errorResponse: HttpErrorResponse) => {
                        if (errorResponse.error instanceof Blob) {
                            errorResponse.error.text().then((text) => {
                                this.error = text;
                            });
                        } else {
                            this.error = this.errorHelper.getErrorString(errorResponse);
                        }
                    }
                });
            });
    }

    getMaxKBCount(): number {
        return HexViewer.KB_COUNT;
    }
}
