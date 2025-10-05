import React, { useEffect } from 'react';
import { useDispatch } from 'react-redux';
import { Outlet } from 'react-router-dom';
import { loadExtensionTypesForCanvas } from '../state/extensionTypesSlice';
import BannerText from '../components/BannerText';

const FlowDesigner = () => {
  const dispatch = useDispatch();

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
