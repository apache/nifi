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

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { AssetUpload } from './asset-upload.component';
import { AssetInfo, UploadProgressInfo } from '../../types';

describe('AssetUpload', () => {
    interface SetupOptions {
        assets?: AssetInfo[];
        uploadProgress?: UploadProgressInfo[];
        multiple?: boolean;
        disabled?: boolean;
        allowedFileTypes?: string[];
        maxFileSize?: number;
        formControlValue?: string[] | string | null;
    }

    function createMockAsset(overrides: Partial<AssetInfo> = {}): AssetInfo {
        return {
            id: 'asset-123',
            name: 'test-file.jar',
            ...overrides
        };
    }

    function createMockProgress(overrides: Partial<UploadProgressInfo> = {}): UploadProgressInfo {
        return {
            filename: 'uploading-file.jar',
            percentComplete: 50,
            status: 'active',
            ...overrides
        };
    }

    async function setup(options: SetupOptions = {}): Promise<{
        fixture: ComponentFixture<AssetUpload>;
        component: AssetUpload;
        formControl: FormControl;
    }> {
        await TestBed.configureTestingModule({
            imports: [AssetUpload, ReactiveFormsModule, NoopAnimationsModule]
        }).compileComponents();

        const fixture = TestBed.createComponent(AssetUpload);
        const component = fixture.componentInstance;

        component.assets = options.assets ?? [];
        component.uploadProgress = options.uploadProgress ?? [];
        component.multiple = options.multiple ?? false;
        component.allowedFileTypes = options.allowedFileTypes ?? [];
        component.maxFileSize = options.maxFileSize ?? 1024 * 1024 * 1024;

        const formControl = new FormControl(options.formControlValue ?? null);

        component.writeValue(options.formControlValue ?? null);

        if (options.disabled) {
            component.setDisabledState(true);
        }

        fixture.detectChanges();

        return { fixture, component, formControl };
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('component initialization', () => {
        it('should create', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
        });

        it('should show drop zone when no assets are uploaded', async () => {
            const { fixture } = await setup();
            const dropZone = fixture.nativeElement.querySelector('[data-qa="asset-drop-zone"]');
            expect(dropZone).toBeTruthy();
        });

        it('should show drop zone with has-content class when single asset is uploaded', async () => {
            const { fixture } = await setup({
                assets: [createMockAsset()],
                multiple: false
            });
            const dropZone = fixture.nativeElement.querySelector('[data-qa="asset-drop-zone"]');
            expect(dropZone).toBeTruthy();
            expect(dropZone.classList.contains('has-content')).toBe(true);
        });

        it('should show drop zone when assets are uploaded and multiple is true', async () => {
            const { fixture } = await setup({
                assets: [createMockAsset()],
                multiple: true
            });
            const dropZone = fixture.nativeElement.querySelector('[data-qa="asset-drop-zone"]');
            expect(dropZone).toBeTruthy();
        });
    });

    describe('asset display', () => {
        it('should display uploaded assets', async () => {
            const { fixture } = await setup({
                assets: [createMockAsset({ name: 'my-file.jar' })]
            });

            const assetItem = fixture.nativeElement.querySelector('[data-qa="uploaded-asset"]');
            expect(assetItem).toBeTruthy();
            expect(assetItem.textContent).toContain('my-file.jar');
        });

        it('should display multiple uploaded assets', async () => {
            const { fixture } = await setup({
                assets: [
                    createMockAsset({ id: '1', name: 'file1.jar' }),
                    createMockAsset({ id: '2', name: 'file2.jar' })
                ],
                multiple: true
            });

            const assetItems = fixture.nativeElement.querySelectorAll('[data-qa="uploaded-asset"]');
            expect(assetItems.length).toBe(2);
        });

        it('should show delete button for each asset', async () => {
            const { fixture } = await setup({
                assets: [createMockAsset()]
            });

            const deleteButton = fixture.nativeElement.querySelector('[data-qa="delete-asset-button"]');
            expect(deleteButton).toBeTruthy();
        });

        it('should show warning UI when asset has missingContent', async () => {
            const { fixture } = await setup({
                assets: [createMockAsset({ name: 'gone.jar', missingContent: true })]
            });

            const row = fixture.nativeElement.querySelector('[data-qa="uploaded-asset-missing-content"]');
            expect(row).toBeTruthy();
            expect(row.textContent).not.toContain('gone.jar');
            expect(fixture.nativeElement.querySelector('[data-qa="asset-missing-warning-icon"]')).toBeTruthy();
        });

        it('should not show asset id or name for missingContent row', async () => {
            const { fixture } = await setup({
                assets: [createMockAsset({ id: 'asset-id-only', name: 'also-uuid', missingContent: true })]
            });

            const row = fixture.nativeElement.querySelector('[data-qa="uploaded-asset-missing-content"]');
            expect(row).toBeTruthy();
            expect(row.textContent).not.toContain('asset-id-only');
            expect(row.textContent).not.toContain('also-uuid');
        });
    });

    describe('upload progress', () => {
        it('should display upload progress', async () => {
            const { fixture } = await setup({
                uploadProgress: [createMockProgress({ percentComplete: 75 })]
            });

            const progressItem = fixture.nativeElement.querySelector('[data-qa="upload-progress"]');
            expect(progressItem).toBeTruthy();
            expect(progressItem.textContent).toContain('75%');
        });

        it('should display error state for failed uploads', async () => {
            const { fixture } = await setup({
                uploadProgress: [createMockProgress({ status: 'error', error: 'Upload failed' })]
            });

            const progressItem = fixture.nativeElement.querySelector('[data-qa="upload-progress"]');
            expect(progressItem).toBeTruthy();
            expect(progressItem.classList.contains('error')).toBe(true);
        });
    });

    describe('file selection', () => {
        it('should emit filesSelected when files are selected via input', async () => {
            const { component } = await setup();
            const filesSelectedSpy = vi.spyOn(component.filesSelected, 'emit');

            const mockFile = new File(['content'], 'test.jar', { type: 'application/java-archive' });
            const mockFileList = {
                0: mockFile,
                length: 1,
                item: () => mockFile
            } as unknown as FileList;

            const mockEvent = {
                target: { files: mockFileList, value: '' }
            } as unknown as Event;

            component.onFileInputChange(mockEvent);

            expect(filesSelectedSpy).toHaveBeenCalledWith([mockFile]);
        });

        it('should emit filesSelected when files are dropped', async () => {
            const { component } = await setup();
            const filesSelectedSpy = vi.spyOn(component.filesSelected, 'emit');

            const mockFile = new File(['content'], 'test.jar', { type: 'application/java-archive' });
            const mockFileList = {
                0: mockFile,
                length: 1,
                item: () => mockFile,
                [Symbol.iterator]: function* () {
                    yield mockFile;
                }
            } as unknown as FileList;

            component.onFilesDropped(mockFileList);

            expect(filesSelectedSpy).toHaveBeenCalledWith([mockFile]);
        });

        it('should only emit first file when multiple is false', async () => {
            const { component } = await setup({ multiple: false });
            const filesSelectedSpy = vi.spyOn(component.filesSelected, 'emit');

            const mockFile1 = new File(['content1'], 'test1.jar', { type: 'application/java-archive' });
            const mockFile2 = new File(['content2'], 'test2.jar', { type: 'application/java-archive' });
            const mockFileList = {
                0: mockFile1,
                1: mockFile2,
                length: 2,
                item: (i: number) => (i === 0 ? mockFile1 : mockFile2),
                [Symbol.iterator]: function* () {
                    yield mockFile1;
                    yield mockFile2;
                }
            } as unknown as FileList;

            component.onFilesDropped(mockFileList);

            expect(filesSelectedSpy).toHaveBeenCalledWith([mockFile1]);
        });
    });

    describe('asset deletion', () => {
        it('should emit deleteAsset when delete button is clicked', async () => {
            const asset = createMockAsset();
            const { component } = await setup({ assets: [asset] });
            const deleteAssetSpy = vi.spyOn(component.deleteAsset, 'emit');

            const mockEvent = { stopPropagation: vi.fn() } as unknown as Event;
            component.onDeleteAsset(asset, mockEvent);

            expect(deleteAssetSpy).toHaveBeenCalledWith(asset);
        });

        it('should not emit deleteAsset when disabled', async () => {
            const asset = createMockAsset();
            const { component } = await setup({ assets: [asset], disabled: true });
            const deleteAssetSpy = vi.spyOn(component.deleteAsset, 'emit');

            const mockEvent = { stopPropagation: vi.fn() } as unknown as Event;
            component.onDeleteAsset(asset, mockEvent);

            expect(deleteAssetSpy).not.toHaveBeenCalled();
        });
    });

    describe('disabled state', () => {
        it('should show drop zone with disabled class when disabled', async () => {
            const { fixture } = await setup({ disabled: true });
            const dropZone = fixture.nativeElement.querySelector('[data-qa="asset-drop-zone"]');
            expect(dropZone).toBeTruthy();
            expect(dropZone.classList.contains('disabled')).toBe(true);
        });

        it('should disable delete buttons when disabled', async () => {
            const { fixture } = await setup({
                assets: [createMockAsset()],
                disabled: true
            });

            const deleteButton = fixture.nativeElement.querySelector('[data-qa="delete-asset-button"]');
            expect(deleteButton.disabled).toBe(true);
        });
    });

    describe('ControlValueAccessor', () => {
        it('should write value correctly for single asset', async () => {
            const { component } = await setup();

            component.writeValue('asset-123');

            expect(component['_value']).toEqual(['asset-123']);
        });

        it('should write value correctly for multiple assets', async () => {
            const { component } = await setup({ multiple: true });

            component.writeValue(['asset-1', 'asset-2']);

            expect(component['_value']).toEqual(['asset-1', 'asset-2']);
        });

        it('should handle null value', async () => {
            const { component } = await setup();

            component.writeValue(null);

            expect(component['_value']).toEqual([]);
        });

        it('should set disabled state', async () => {
            const { component } = await setup();

            component.setDisabledState(true);

            expect(component.disabled).toBe(true);
        });
    });

    describe('validation state', () => {
        it('should report hasError as false when there is no NgControl', async () => {
            const { component, fixture } = await setup();

            expect(component.hasError).toBe(false);

            fixture.detectChanges();
        });

        it('should report hasRequiredError as false when there is no NgControl', async () => {
            const { component } = await setup();

            expect(component.hasRequiredError).toBe(false);
        });
    });

    describe('UI helpers', () => {
        it('should detect active uploads', async () => {
            const { component } = await setup({
                uploadProgress: [createMockProgress({ status: 'active' })]
            });
            expect(component.hasActiveUploads).toBe(true);
        });

        it('should not detect active uploads when complete', async () => {
            const { component } = await setup({
                uploadProgress: [createMockProgress({ status: 'complete' })]
            });
            expect(component.hasActiveUploads).toBe(false);
        });

        it('should generate accept attribute from allowed file types', async () => {
            const { component } = await setup({
                allowedFileTypes: ['.jar', '.nar']
            });
            expect(component.acceptAttribute).toBe('.jar,.nar');
        });
    });

    describe('trackBy functions', () => {
        it('should track assets by ID', async () => {
            const { component } = await setup();
            const asset = createMockAsset({ id: 'unique-id' });

            expect(component.trackByAssetId(0, asset)).toBe('unique-id');
        });

        it('should track progress by filename', async () => {
            const { component } = await setup();
            const progress = createMockProgress({ filename: 'unique-file.jar' });

            expect(component.trackByFilename(0, progress)).toBe('unique-file.jar');
        });
    });
});
