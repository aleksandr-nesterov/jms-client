package com.spaniard.jms.client.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Mapping DTO class to Jms queue
 * Used in JmsModelMappingBeanPostProcessor
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface JmsModelMapping {

    // Jms Queue bean name
    String value() default "";

}

