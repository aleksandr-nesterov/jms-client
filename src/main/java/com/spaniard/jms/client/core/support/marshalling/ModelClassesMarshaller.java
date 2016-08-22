package com.spaniard.jms.client.core.support.marshalling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spaniard.jms.client.exception.IllegalPropertyException;
import com.spaniard.jms.client.exception.MarshallerException;
import com.spaniard.jms.client.core.Strategy;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.StringWriter;
import java.util.Objects;
import java.util.Set;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
public class ModelClassesMarshaller extends AbstractMarshaller implements Marshaller {

    private final ObjectMapper jsonMapper = new ObjectMapper();

    public ModelClassesMarshaller(Set<Class> modelClasses) {
        init(modelClasses);
    }

    void init(final Set<Class> modelClasses) {
        marshaller.put(Strategy.STRING, modelObject -> modelObject.toString());
        marshaller.put(Strategy.XML, modelObject -> {
            if (Objects.isNull(modelClasses)) {
                throw new IllegalPropertyException("modelClasses is null");
            }
            final ClassLoader cl = Thread.currentThread().getContextClassLoader();
            try {
                // NOTE: Change thread context class loader. Need that for case when there are no
                // model classes in the thread's class loader.
                Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
                final JAXBContext jaxbContext = JAXBContext.newInstance(modelClasses.toArray(new Class[0]));
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
        marshaller.put(Strategy.JSON, modelObject -> {
            if (Objects.isNull(modelClasses)) {
                throw new IllegalPropertyException("modelClasses is null");
            }
            final ClassLoader cl = Thread.currentThread().getContextClassLoader();
            try {
                if (modelClasses.contains(modelObject.getClass())) {
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
