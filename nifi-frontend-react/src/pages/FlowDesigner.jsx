import React, { useEffect } from 'react';
import { useDispatch } from 'react-redux';
import { Outlet } from 'react-router-dom';
import { loadExtensionTypesForCanvas } from '../state/extensionTypesSlice';
import BannerText from '../components/BannerText';
import { useAbout } from '../hooks/useAbout';

const FlowDesigner = () => {
  const dispatch = useDispatch();
  useAbout();

  useEffect(() => {
    dispatch(loadExtensionTypesForCanvas());
  }, [dispatch]);

  return (
    <BannerText>
      <Outlet />
    </BannerText>
  );
};

export default FlowDesigner;
