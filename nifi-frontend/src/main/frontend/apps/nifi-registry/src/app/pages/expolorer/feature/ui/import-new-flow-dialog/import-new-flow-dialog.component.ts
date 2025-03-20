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

import { Component, ElementRef, Inject, OnInit, ViewChild } from '@angular/core';
import { CommonModule } from '@angular/common';
import { CloseOnEscapeDialog } from '@nifi/shared';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { Bucket } from 'apps/nifi-registry/src/app/state/buckets';
import { MatInputModule } from '@angular/material/input';
import { Store } from '@ngrx/store';
import { createNewFlow } from 'apps/nifi-registry/src/app/state/droplets/droplets.actions';
import { MatButtonModule } from '@angular/material/button';

interface Data {
    activeBucket: any;
    buckets: any;
}

@Component({
    selector: 'app-import-new-flow-dialog',
    standalone: true,
    imports: [
        CommonModule,
        MatDialogModule,
        FormsModule,
        ReactiveFormsModule,
        MatFormFieldModule,
        MatSelectModule,
        MatInputModule,
        MatButtonModule
    ],
    templateUrl: './import-new-flow-dialog.component.html',
    styleUrl: './import-new-flow-dialog.component.scss'
})
export class ImportNewFlowDialogComponent extends CloseOnEscapeDialog implements OnInit {
    @ViewChild('uploadFlowFileField') uploadFlowFileFieldRef!: ElementRef;

    extensions = 'application/json';
    fileName: string | null = null;
    hoverValidity = '';
    fileToUpload: File | null = null;
    multiple = false;
    writableBuckets: Bucket[] = [];
    activeBucket: string | null = null;
    buckets = null;
    name = '';
    description = '';

    constructor(
        @Inject(MAT_DIALOG_DATA) public data: Data,
        private store: Store
    ) {
        super();
        this.activeBucket = this.data.activeBucket?.identifier;
        this.buckets = data.buckets;
    }

    ngOnInit() {
        this.writableBuckets = this.filterWritableBuckets(this.buckets);

        // if there's only 1 writable bucket, always set as the initial value in the bucket dropdown
        // if opening the dialog from the explorer/grid-list, there is no active bucket
        if (this.activeBucket === undefined) {
            if (this.writableBuckets.length === 1) {
                // set the active bucket
                this.activeBucket = this.writableBuckets[0].identifier;
            }
        }
    }

    filterWritableBuckets(buckets: any) {
        const filteredWritableBuckets: Bucket[] = [];
        buckets.forEach(function (b: Bucket) {
            if (b.permissions.canWrite) {
                filteredWritableBuckets.push(b);
            }
        });
        return filteredWritableBuckets;
    }

    selectFile() {
        this.uploadFlowFileFieldRef.nativeElement.click();
    }

    fileDragHandler(event: DragEvent, extensions: any) {
        event.preventDefault();
        event.stopPropagation();

        this.extensions = extensions;

        const items = event.dataTransfer?.items;
        this.hoverValidity = this.isFileInvalid(items) ? 'invalid' : 'valid';
    }

    fileDragEndHandler() {
        this.hoverValidity = '';
    }

    fileDropHandler(event: DragEvent) {
        event.preventDefault();
        event.stopPropagation();

        const { files } = event.dataTransfer!;

        if (files && !this.isFileInvalid(Array.from(files))) {
            this.handleFileInput(files);
        }

        this.hoverValidity = '';
    }

    handleFileInput(files: FileList): void {
        if (!files || !files.length) {
            return;
        }
        // get the file
        this.fileToUpload = files![0];

        // get the filename
        const fileName = this.fileToUpload.name;

        // trim off the file extension
        this.fileName = fileName.replace(/\..*/, '');
    }

    isFileInvalid(items: any) {
        return (
            items.length > 1 ||
            (this.extensions !== '' && items[0].type === '') ||
            this.extensions.indexOf(items[0].type) === -1
        );
    }

    importNewFlow() {
        const selectedBucket: Bucket = this.writableBuckets.find((b) => {
            return b.identifier === this.activeBucket;
        })!;

        this.store.dispatch(
            createNewFlow({
                request: {
                    bucket: selectedBucket,
                    file: this.fileToUpload!,
                    name: this.name,
                    description: this.description
                }
            })
        );
    }
}
