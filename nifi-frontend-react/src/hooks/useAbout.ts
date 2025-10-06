import { useCallback, useEffect } from 'react';
import { firstValueFrom } from 'rxjs';
import { AjaxError } from 'rxjs/ajax';

import { useAppDispatch, useAppSelector } from './redux';
import {
  setAboutStatus,
  setAbout,
  setAboutError,
  type AboutState
} from '../state/aboutSlice';
import { getAbout$ } from '../service/aboutService';
import { ErrorResponse, getErrorString } from '../service/errorHelper';

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

export interface UseAboutResult extends AboutState {
  reload: () => Promise<void>;
}

export function useAbout(autoLoad = true): UseAboutResult {
  const dispatch = useAppDispatch();
  const aboutState = useAppSelector((state) => state.about);

  const loadAbout = useCallback(async () => {
    dispatch(setAboutStatus('loading'));

    try {
      const response = await firstValueFrom(getAbout$());
      dispatch(setAbout(response.about));
      dispatch(setAboutStatus('success'));
    } catch (error) {
      const errorResponse = toErrorResponse(error);
      const message =
        getErrorString(errorResponse) ??
        'Unable to load about information. Please contact the system administrator.';
      dispatch(setAboutError(message));
    }
  }, [dispatch]);

  useEffect(() => {
    if (!autoLoad) {
      return;
    }

    const shouldLoad =
      aboutState.status === 'idle' ||
      (aboutState.status === 'error' && !aboutState.about);

    if (shouldLoad) {
      loadAbout();
    }
  }, [aboutState.about, aboutState.status, autoLoad, loadAbout]);

  return {
    ...aboutState,
    reload: loadAbout
  };
}
