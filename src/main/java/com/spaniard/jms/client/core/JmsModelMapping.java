package com.spaniard.jms.client.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Анотация для мапинга DTO на jms очередь
 * Используется в JmsModelMappingBeanPostProcessor
 * created 06.10.15
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface JmsModelMapping {

    // Название бина очереди jms
    String value() default "";

}

