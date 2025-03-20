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

import { Component, ElementRef, Inject, ViewChild } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { CloseOnEscapeDialog } from '@nifi/shared';
import { FormBuilder, FormsModule, ReactiveFormsModule } from '@angular/forms';
import { Store } from '@ngrx/store';
import { importNewFlow } from 'apps/nifi-registry/src/app/state/droplets/droplets.actions';
import { Droplets } from 'apps/nifi-registry/src/app/state/droplets';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';

interface Data {
    droplet: Droplets;
}

@Component({
    selector: 'app-import-new-flow-version-dialog',
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
    templateUrl: './import-new-flow-version-dialog.component.html',
    styleUrl: './import-new-flow-version-dialog.component.scss'
})
export class ImportNewFlowVersionDialogComponent extends CloseOnEscapeDialog {
    @ViewChild('uploadFlowFileField') uploadFlowFileFieldRef!: ElementRef;

    extensions = 'application/json';
    fileName: string | null = null;
    hoverValidity = '';
    fileToUpload: File | null = null;
    multiple = false;
    activeBucket: string | null = null;
    buckets = null;
    name = '';
    description = '';

    keepDialogOpen = false;
    droplet: any;
    comments = '';

    constructor(
        @Inject(MAT_DIALOG_DATA) public data: Data,
        private formBuilder: FormBuilder,
        private store: Store
    ) {
        super();
        this.droplet = data.droplet;
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

    importNewFlowVersion() {
        this.store.dispatch(
            importNewFlow({
                request: {
                    bucket: this.droplet.bucket,
                    file: this.fileToUpload!,
                    name: this.name,
                    description: this.description
                },
                href: this.droplet.link.href
            })
        );
    }
}
