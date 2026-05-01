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

import {
    ChangeDetectorRef,
    Component,
    ElementRef,
    EventEmitter,
    inject,
    Input,
    Output,
    ViewChild,
    ViewEncapsulation
} from '@angular/core';

import { ControlValueAccessor, NgControl } from '@angular/forms';
import { MatButton } from '@angular/material/button';
import { MatCard } from '@angular/material/card';
import { MatProgressBar } from '@angular/material/progress-bar';
import { MatTooltip } from '@angular/material/tooltip';
import { DragAndDropDirective } from '../../directives/drag-and-drop/drag-and-drop.directive';
import { EllipsisTooltipDirective } from '../../directives/ellipsis-tooltip/ellipsis-tooltip.directive';
import { AssetInfo, UploadProgressInfo } from '../../types';

/**
 * Reusable file-upload component for connector assets.
 *
 * Implements ControlValueAccessor so it can be bound to a reactive form control. The form
 * value is an array of asset IDs (string[]); when {@link multiple} is false the control
 * exposes a single-element array (or empty array).
 *
 * The component is intentionally label-agnostic. The parent is responsible for:
 *  - rendering an external label / heading
 *  - performing the actual upload in response to {@link filesSelected}
 *  - supplying the current uploaded {@link assets} and in-flight {@link uploadProgress}
 */
@Component({
    selector: 'asset-upload',
    standalone: true,
    imports: [MatButton, MatCard, MatProgressBar, MatTooltip, DragAndDropDirective, EllipsisTooltipDirective],
    templateUrl: './asset-upload.component.html',
    styleUrls: ['./asset-upload.component.scss'],
    encapsulation: ViewEncapsulation.None
})
export class AssetUpload implements ControlValueAccessor {
    private cdr = inject(ChangeDetectorRef);

    /** Currently uploaded assets to display. */
    @Input() assets: AssetInfo[] = [];

    /** Active upload progress states. */
    @Input() uploadProgress: UploadProgressInfo[] = [];

    /** Allow multiple files (false for ASSET, true for ASSET_LIST). */
    @Input() multiple = false;

    /** Optional file type restrictions (e.g., ['.jar', '.nar']). */
    @Input() allowedFileTypes: string[] = [];

    /** Optional max file size in bytes (default: 1GB to match the backend cap). */
    @Input() maxFileSize: number = 1024 * 1024 * 1024;

    /** Emitted when files are selected via click-browse or drag-and-drop. */
    @Output() filesSelected = new EventEmitter<File[]>();

    /** Emitted when delete is requested for an uploaded asset. */
    @Output() deleteAsset = new EventEmitter<AssetInfo>();

    /** Emitted when the user dismisses a failed upload row. */
    @Output() dismissFailedUpload = new EventEmitter<UploadProgressInfo>();

    @ViewChild('fileInput') fileInput!: ElementRef<HTMLInputElement>;

    private ngControl = inject(NgControl, { optional: true, self: true });

    private _value: string[] = [];
    disabled = false;
    private onChange: (value: string[] | string | null) => void = () => {
        /* noop until registerOnChange */
    };
    private onTouched: () => void = () => {
        /* noop until registerOnTouched */
    };

    constructor() {
        // Self-register as the value accessor to avoid the cyclic provider dependency
        // that arises if NG_VALUE_ACCESSOR is wired through a multi-provider on this class.
        if (this.ngControl) {
            this.ngControl.valueAccessor = this;
        }
    }

    // ========================================================================================
    // ControlValueAccessor
    // ========================================================================================

    writeValue(value: string[] | string | null): void {
        if (value === null || value === undefined) {
            this._value = [];
        } else if (Array.isArray(value)) {
            this._value = value;
        } else {
            this._value = [value];
        }
        this.cdr.markForCheck();
    }

    registerOnChange(fn: (value: string[] | string | null) => void): void {
        this.onChange = fn;
    }

    registerOnTouched(fn: () => void): void {
        this.onTouched = fn;
    }

    setDisabledState(isDisabled: boolean): void {
        this.disabled = isDisabled;
        this.cdr.markForCheck();
    }

    // ========================================================================================
    // Validation State Accessors
    // ========================================================================================

    get hasError(): boolean {
        return !!this.ngControl?.control?.invalid && !!this.ngControl?.control?.touched;
    }

    get isTouched(): boolean {
        return !!this.ngControl?.control?.touched;
    }

    get hasRequiredError(): boolean {
        return !!this.ngControl?.control?.hasError('required') && this.isTouched;
    }

    // ========================================================================================
    // File Handling
    // ========================================================================================

    openFileBrowser(): void {
        if (!this.disabled) {
            this.fileInput.nativeElement.click();
        }
    }

    onFileInputChange(event: Event): void {
        const input = event.target as HTMLInputElement;
        if (input.files && input.files.length > 0) {
            this.handleFiles(Array.from(input.files));
        }
        // Reset the input so re-selecting the same file fires another change event.
        input.value = '';
    }

    onFilesDropped(fileList: FileList): void {
        this.handleFiles(Array.from(fileList));
    }

    onInvalidDrop(message: string): void {
        // The parent component can react via the form's existing validators.
        console.warn('Invalid file drop:', message);
    }

    private handleFiles(files: File[]): void {
        if (this.disabled) return;

        this.onTouched();

        // In single-file mode, ignore extra files even if the OS file picker permitted them.
        const filesToEmit = this.multiple ? files : files.slice(0, 1);

        this.filesSelected.emit(filesToEmit);
    }

    onDeleteAsset(asset: AssetInfo, event: Event): void {
        event.stopPropagation();
        if (!this.disabled) {
            this.onTouched();
            this.deleteAsset.emit(asset);
        }
    }

    onDismissFailedUpload(progress: UploadProgressInfo, event: Event): void {
        event.stopPropagation();
        this.dismissFailedUpload.emit(progress);
    }

    // ========================================================================================
    // UI State Helpers
    // ========================================================================================

    get hasActiveUploads(): boolean {
        return this.uploadProgress.some((p) => p.status === 'active');
    }

    get acceptAttribute(): string {
        return this.allowedFileTypes.join(',');
    }

    trackByAssetId(_index: number, asset: AssetInfo): string {
        return asset.id;
    }

    trackByFilename(_index: number, progress: UploadProgressInfo): string {
        return progress.filename;
    }
}
