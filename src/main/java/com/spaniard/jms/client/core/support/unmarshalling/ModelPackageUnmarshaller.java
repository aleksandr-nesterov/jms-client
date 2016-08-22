package com.spaniard.jms.client.core.support.unmarshalling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spaniard.jms.client.core.support.ReflectionUtils;
import com.spaniard.jms.client.exception.UnmarshallerException;
import com.spaniard.jms.client.core.Strategy;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
public class ModelPackageUnmarshaller extends AbstractUnmarshaller implements Unmarshaller {

    private final ObjectMapper jsonMapper = new ObjectMapper();

    public ModelPackageUnmarshaller(String modelPackage) {
        init(modelPackage);
    }

    private void init(final String modelPackage) {
        Set<Class> modelPackageClasses = modelPackage != null
                        ? ReflectionUtils.getAllClassesInPackage(modelPackage)
                        : new HashSet<>();
        unmarshallerMap.put(Strategy.STRING, message -> message);
        unmarshallerMap.put(Strategy.XML, message -> {
            try {
                final JAXBContext jaxbContext = JAXBContext.newInstance(modelPackage);
                final javax.xml.bind.Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
                return jaxbUnmarshaller.unmarshal(new StringReader(message));
            } catch (JAXBException e) {
                throw new UnmarshallerException(e.getMessage(), e);
            }
        });
        unmarshallerMap.put(Strategy.JSON, message -> {
            try {
                for (Class<?> modelClass : modelPackageClasses) {
                    Object result = null;
                    try {
                        result = jsonMapper.readValue(message, modelClass);
                    } catch (IOException e) {
                        // check deserealization class
                    }
                    if (result != null) {
                        return result;
                    }
                }
                throw new IllegalArgumentException("No suitable class for message " + message);
            } catch (Exception e) {
                throw new UnmarshallerException(e.getMessage(), e);
            }
        });
    }
}
