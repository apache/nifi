package org.apache.nifi.web.api;

import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.entity.ParameterEntity;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ParameterContextResourceTest {
    @Test
    void testMutationAllowsRemovalOfParameterNameLoadedFromVersionedFlow() throws NoSuchMethodException {
        final String invalidParameterName = "OPENFLOW_NETJETS_SECRET_{{ envi }}";
        final ParameterDTO parameter = new ParameterDTO();
        parameter.setName(invalidParameterName);

        final ParameterEntity parameterEntity = new ParameterEntity();
        parameterEntity.setParameter(parameter);

        final ParameterContextDTO parameterContext = new ParameterContextDTO();
        parameterContext.setParameters(Set.of(parameterEntity));

        final Method validateParameterNames = ParameterContextResource.class.getDeclaredMethod("validateParameterNames", ParameterContextDTO.class);
        validateParameterNames.setAccessible(true);

        assertDoesNotThrow(() -> validateParameterNames.invoke(new ParameterContextResource(), parameterContext));
    }

    @Test
    void testMutationRejectsNewIllegalParameterName() throws NoSuchMethodException {
        final ParameterDTO parameter = new ParameterDTO();
        parameter.setName("OPENFLOW_NETJETS_SECRET_{{ envi }}");
        parameter.setSensitive(true);

        final ParameterEntity parameterEntity = new ParameterEntity();
        parameterEntity.setParameter(parameter);

        final ParameterContextDTO parameterContext = new ParameterContextDTO();
        parameterContext.setParameters(Set.of(parameterEntity));

        final Method validateParameterNames = ParameterContextResource.class.getDeclaredMethod("validateParameterNames", ParameterContextDTO.class);
        validateParameterNames.setAccessible(true);

        final InvocationTargetException exception = assertThrows(InvocationTargetException.class,
                () -> validateParameterNames.invoke(new ParameterContextResource(), parameterContext));
        assertInstanceOf(IllegalArgumentException.class, exception.getCause());
    }
}
