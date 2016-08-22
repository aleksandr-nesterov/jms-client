package com.spaniard.jms.client.core.support.marshalling;

import com.spaniard.jms.client.core.Strategy;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
public class AbstractMarshaller implements Marshaller {

    final Map<Strategy, Function<Object, String>> marshaller = new HashMap<>(Strategy.values().length);

    @Override
    public String marshall(final Strategy strategy, final Object modelObject) {
        return marshaller.get(strategy).apply(modelObject);
    }

    public static class MarshallerFactory {

        public static Marshaller fromModelClasses(final Set<Class> modelClasses) {
            return new ModelClassesMarshaller(modelClasses);
        }

        public static Marshaller fromModelPackage(final String modelPackage) {
            return new ModelPackageMarshaller(modelPackage);
        }
    }
}
