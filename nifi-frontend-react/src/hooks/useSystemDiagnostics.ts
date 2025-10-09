import { useCallback, useEffect } from 'react';
import { firstValueFrom } from 'rxjs';
import { AjaxError } from 'rxjs/ajax';

import { useAppDispatch, useAppSelector } from './redux';
import {
	setSystemDiagnosticsStatus,
	setSystemDiagnostics,
	setSystemDiagnosticsError,
	type SystemDiagnosticsState
} from '../state/systemDiagnosticsSlice';
import { getSystemDiagnostics$ } from '../service/systemDiagnosticsService';
import { ErrorResponse, getErrorString } from '../service/errorHelper';

export interface UseSystemDiagnosticsOptions {
	nodewise?: boolean;
	autoLoad?: boolean;
	errorStrategy?: 'banner' | 'snackbar';
}

function toErrorResponse(error: unknown): ErrorResponse {
	if (typeof error === 'object' && error !== null && 'status' in error) {
		const ajaxError = error as AjaxError;
		return {
			status: ajaxError.status ?? 0,
			message: ajaxError.message,
			error: ajaxError.response
		};
	}

	return {
		status: 0,
		message: error instanceof Error ? error.message : undefined
	};
}

export interface UseSystemDiagnosticsResult extends SystemDiagnosticsState {
	load: () => Promise<void>;
}

export function useSystemDiagnostics({
	nodewise = false,
	autoLoad = true,
	errorStrategy = 'banner'
}: UseSystemDiagnosticsOptions = {}): UseSystemDiagnosticsResult {
	const dispatch = useAppDispatch();
	const diagnosticsState = useAppSelector((state) => state.systemDiagnostics);

	const load = useCallback(async () => {
		dispatch(setSystemDiagnosticsStatus('loading'));

		try {
			const response = await firstValueFrom(getSystemDiagnostics$(nodewise));
			dispatch(setSystemDiagnostics(response.systemDiagnostics));
			dispatch(setSystemDiagnosticsStatus('success'));
		} catch (error) {
			const errorResponse = toErrorResponse(error);
			const prefix = errorStrategy === 'snackbar' ? 'Failed to load System Diagnostics.' : undefined;
			const message =
				getErrorString(errorResponse, prefix) ??
				'Unable to load system diagnostics. Please contact the system administrator.';
			dispatch(setSystemDiagnosticsError(message));
		}
	}, [dispatch, nodewise, errorStrategy]);

	useEffect(() => {
		if (!autoLoad) {
			return;
		}

		const shouldLoad =
			diagnosticsState.status === 'idle' ||
			(diagnosticsState.status === 'error' && !diagnosticsState.systemDiagnostics);

		if (shouldLoad) {
			void load();
		}
	}, [autoLoad, diagnosticsState.status, diagnosticsState.systemDiagnostics, load]);

	return {
		...diagnosticsState,
		load
	};
}
