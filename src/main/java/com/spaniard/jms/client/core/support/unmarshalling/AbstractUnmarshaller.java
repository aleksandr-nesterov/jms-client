package com.spaniard.jms.client.core.support.unmarshalling;

import com.spaniard.jms.client.core.Strategy;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
public class AbstractUnmarshaller implements Unmarshaller {

    final Map<Strategy, Function<String, Object>> unmarshallerMap = new HashMap<>(Strategy.values().length);

    @Override
    public Object unmarshall(final Strategy strategy, final String message) {
        return unmarshallerMap.get(strategy).apply(message);
    }

    public static class UnmarshallerFactory {

        public static Unmarshaller fromModelClasses(final Set<Class> modelClasses) {
            return new ModelClassesUnmarshaller(modelClasses);
        }

        public static Unmarshaller fromModelPackage(final String modelPackage) {
            return new ModelPackageUnmarshaller(modelPackage);
        }

    }
}
