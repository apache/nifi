import { ReactNode, useEffect } from 'react';
import { firstValueFrom } from 'rxjs';
import { AjaxError } from 'rxjs/ajax';

import { useAppDispatch, useAppSelector } from '../hooks/redux';
import {
  setBannerTextStatus,
  setBannerText,
  setBannerTextError
} from '../state/bannerTextSlice';
import { getBannerText$ } from '../service/bannerTextService';
import { ErrorResponse, getErrorString } from '../service/errorHelper';

interface BannerTextProps {
  children: ReactNode;
  errorFallback?: (message: string) => ReactNode;
}

const defaultError = 'Unable to load banner text.';

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

const BannerText = ({ children, errorFallback }: BannerTextProps) => {
  const dispatch = useAppDispatch();
  const bannerTextState = useAppSelector((state) => state.bannerText);

  useEffect(() => {
    let isMounted = true;

    const loadBannerText = async () => {
      const shouldLoad =
        bannerTextState.status === 'idle' ||
        (bannerTextState.status === 'error' && !bannerTextState.bannerText);

      if (!shouldLoad) {
        return;
      }

      dispatch(setBannerTextStatus('loading'));

      try {
        const response = await firstValueFrom(getBannerText$());
        if (!isMounted) return;
        dispatch(setBannerText(response.banners));
        dispatch(setBannerTextStatus('success'));
      } catch (error) {
        if (!isMounted) return;
        const errorResponse = toErrorResponse(error);
        const message =
          getErrorString(errorResponse) ??
          'Unable to load banner text. Please contact the system administrator.';
        dispatch(setBannerTextError(message));
      }
    };

    loadBannerText();

    return () => {
      isMounted = false;
    };
  }, [dispatch, bannerTextState.bannerText, bannerTextState.status]);

  const bannerText = bannerTextState.bannerText;
  const errorMessage = bannerTextState.error;

  if (errorMessage && !bannerText) {
    const message = defaultError;
    return <>{errorFallback ? errorFallback(message) : <div>{message}</div>}</>;
  }

  return (
    <div className="flex flex-col h-screen">
      {bannerText?.headerText ? (
        <div className="flex justify-center">{bannerText.headerText}</div>
      ) : null}
      <div className="flex-1">{children}</div>
      {bannerText?.footerText ? (
        <div className="flex justify-center">{bannerText.footerText}</div>
      ) : null}
    </div>
  );
};

export default BannerText;
