package com.spaniard.jms.client.core.support.marshalling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spaniard.jms.client.core.support.ReflectionUtils;
import com.spaniard.jms.client.exception.IllegalPropertyException;
import com.spaniard.jms.client.exception.MarshallerException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static com.spaniard.jms.client.core.Strategy.JSON;
import static com.spaniard.jms.client.core.Strategy.STRING;
import static com.spaniard.jms.client.core.Strategy.XML;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
public class ModelPackageMarshaller extends AbstractMarshaller implements Marshaller {

    private final ObjectMapper jsonMapper = new ObjectMapper();

    public ModelPackageMarshaller(String modelPackage) {
        init(modelPackage);
    }

    private void init(final String modelPackage) {
        Set<Class> modelPackageClasses = modelPackage != null
                ? ReflectionUtils.getAllClassesInPackage(modelPackage)
                : new HashSet<>();
        marshaller.put(STRING, modelObject -> modelObject.toString());
        marshaller.put(XML, modelObject -> {
            if (Objects.isNull(modelPackage)) {
                throw new IllegalPropertyException("modelPackage is null!");
            }
            final ClassLoader cl = Thread.currentThread().getContextClassLoader();
            try {
                // NOTE: Change thread context class loader. Need that for case when there are no
                // model classes in the thread's class loader.
                Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
                final JAXBContext jaxbContext = JAXBContext.newInstance(modelPackage);
                final javax.xml.bind.Marshaller marshaller = jaxbContext.createMarshaller();
                final StringWriter stringWriter = new StringWriter();
                marshaller.marshal(modelObject, stringWriter);
                return stringWriter.toString();
            } catch (JAXBException e) {
                throw new MarshallerException(e.getMessage(), e);
            } finally {
                Thread.currentThread().setContextClassLoader(cl);
            }
        });
        marshaller.put(JSON, modelObject -> {
            final ClassLoader cl = Thread.currentThread().getContextClassLoader();
            try {
                if (modelPackageClasses.contains(modelObject.getClass())) {
                    // NOTE: Change thread context class loader. Need that for case when there are no
                    // model classes in the thread's class loader.
                    Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
                    return jsonMapper.writeValueAsString(modelObject);
                } else {
                    throw new IllegalArgumentException("Model package not contains object class " + modelObject.getClass().getName());
                }
            } catch (Exception e) {
                throw new MarshallerException(e.getMessage(), e);
            } finally {
                Thread.currentThread().setContextClassLoader(cl);
            }
        });
    }
}
