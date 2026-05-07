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

import { Directive, ElementRef, EventEmitter, HostBinding, HostListener, Input, Output, inject } from '@angular/core';

@Directive({
    selector: '[dragAndDrop]',
    standalone: true
})
export class DragAndDropDirective {
    private element = inject<ElementRef<HTMLElement>>(ElementRef);

    @Input() canDragAndDrop: (() => boolean) | null = null;
    @Input() allowedFileTypes: string[] = [];
    @Input() allowMultipleFiles = true;
    @Input() maxFileSize: number | null = null;

    @Output() filesDropped = new EventEmitter<FileList>();
    @Output() invalidDrop = new EventEmitter<string>();

    @HostBinding('class.drop-allowed') dropAllowed = false;
    @HostBinding('class.drop-invalid') dropInvalid = false;

    private dataTransferAllowedByFiles(fileList: FileList): boolean {
        const files: File[] = Array.from(fileList);
        if (files.length > 0) {
            const fileCountAllowed = files.length === 1 || this.allowMultipleFiles;

            let fileTypesAllowed = true;
            if (this.allowedFileTypes.length > 0) {
                fileTypesAllowed = files.every((file) => {
                    const name = file.name.toLowerCase();
                    return this.allowedFileTypes.some((fileType) => name.endsWith(fileType.toLowerCase()));
                });
            }

            let fileSizesAllowed = true;
            if (this.maxFileSize !== null) {
                fileSizesAllowed = files.every((file) => this.maxFileSize !== null && file.size <= this.maxFileSize);
            }

            if (!fileCountAllowed) {
                this.invalidDrop.emit(`Invalid files selected. Only a single file can be uploaded.`);
            } else if (!fileTypesAllowed) {
                this.invalidDrop.emit(
                    `Invalid file(s) selected. Allowed file types: [${this.allowedFileTypes.join(', ')}]`
                );
            } else if (!fileSizesAllowed) {
                this.invalidDrop.emit(
                    `Invalid file(s) selected. Maximum allowed file size: [${this.maxFileSize} bytes]`
                );
            }

            return fileCountAllowed && fileTypesAllowed && fileSizesAllowed;
        }

        return false;
    }

    /**
     * Map of common file extensions to MIME types for dragover validation.
     * During dragover the browser only exposes MIME types; filenames become
     * available on drop. The allowed list is intentionally permissive so a
     * dragover never falsely rejects files whose final extension check (in
     * dataTransferAllowedByFiles) will be the source of truth.
     */
    private static readonly EXTENSION_TO_MIME: { [ext: string]: string[] } = {
        '.jar': ['application/java-archive', 'application/x-java-archive', 'application/octet-stream'],
        '.zip': ['application/zip', 'application/x-zip-compressed'],
        '.json': ['application/json'],
        '.xml': ['application/xml', 'text/xml'],
        '.csv': ['text/csv'],
        '.txt': ['text/plain'],
        '.pdf': ['application/pdf'],
        '.png': ['image/png'],
        '.jpg': ['image/jpeg'],
        '.jpeg': ['image/jpeg'],
        '.gif': ['image/gif'],
        '.svg': ['image/svg+xml']
    };

    private dataTransferAllowedByItems(dataTransferItemList: DataTransferItemList): boolean {
        const items: DataTransferItem[] = Array.from(dataTransferItemList);
        if (items.length > 0) {
            if (!this.allowMultipleFiles && items.length > 1) {
                return false;
            }

            if (this.allowedFileTypes.length > 0) {
                const allowedMimeTypes: string[] = [];
                for (const ext of this.allowedFileTypes) {
                    const mimes = DragAndDropDirective.EXTENSION_TO_MIME[ext.toLowerCase()];
                    if (mimes) {
                        allowedMimeTypes.push(...mimes);
                    }
                }

                if (allowedMimeTypes.length > 0) {
                    const allItemsAllowed = items.every((item) => {
                        // Empty type or octet-stream could be any file; allow it during dragover
                        // and rely on the drop-time filename check to reject mismatches.
                        if (!item.type || item.type === 'application/octet-stream') {
                            return true;
                        }
                        return allowedMimeTypes.includes(item.type);
                    });

                    if (!allItemsAllowed) {
                        return false;
                    }
                }
            }

            return true;
        }

        return false;
    }

    // Angular 21 enables typeCheckHostBindings by default which infers $event as Event
    // rather than DragEvent (see angular/angular#40778). Cast to DragEvent internally.
    @HostListener('dragover', ['$event'])
    onDragOver(event: Event) {
        event.preventDefault();
        event.stopPropagation();
        const dragEvent = event as DragEvent;

        if (dragEvent.dataTransfer) {
            let canDrag = true;
            if (this.canDragAndDrop !== null && !this.canDragAndDrop()) {
                canDrag = false;
            }

            if (canDrag) {
                this.dropAllowed = this.dataTransferAllowedByItems(dragEvent.dataTransfer.items);
                this.dropInvalid = !this.dropAllowed;
            } else {
                this.dropAllowed = false;
                this.dropInvalid = true;
            }
        } else {
            this.dropAllowed = false;
            this.dropInvalid = false;
        }
    }

    @HostListener('dragleave', ['$event'])
    onDragLeave(event: Event) {
        event.preventDefault();
        event.stopPropagation();
        // Only clear feedback when the pointer leaves the original host element so that
        // crossing over child elements does not reset drop-allowed state mid-drag.
        if (this.element.nativeElement === event.target) {
            event.preventDefault();
            event.stopPropagation();

            this.dropAllowed = false;
            this.dropInvalid = false;
        }
    }

    @HostListener('drop', ['$event'])
    onDrop(event: Event) {
        event.preventDefault();
        event.stopPropagation();
        const dragEvent = event as DragEvent;

        let canDrag = true;

        if (this.canDragAndDrop !== null && !this.canDragAndDrop()) {
            canDrag = false;
        }

        if (canDrag) {
            if (dragEvent.dataTransfer && this.dataTransferAllowedByFiles(dragEvent.dataTransfer.files)) {
                this.filesDropped.emit(dragEvent.dataTransfer.files);
            }
        }

        this.dropAllowed = false;
        this.dropInvalid = false;
    }
}
