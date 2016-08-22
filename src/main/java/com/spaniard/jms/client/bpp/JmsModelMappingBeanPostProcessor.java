package com.spaniard.jms.client.bpp;

import com.spaniard.jms.client.core.JmsMessageHandler;
import com.spaniard.jms.client.core.JmsMessageReceiver;
import com.spaniard.jms.client.core.JmsMessageSender;
import com.spaniard.jms.client.core.JmsModelMapping;
import com.spaniard.jms.client.listener.JmsMessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.GenericTypeResolver;
import org.springframework.core.Ordered;
import org.springframework.core.PriorityOrdered;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;

import javax.jms.Destination;
import javax.jms.Queue;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Handler for {@link JmsMessageSender} and {@link JmsMessageListener} beans.
 * 1. Generates mapping (Model class -> JMS Queue) in {@link JmsMessageSender} bean using {@link JmsModelMapping} annotation.
 * 2. Generates mapping {@link JmsMessageListener} bean using JmsMessageHandler type
 */
public class JmsModelMappingBeanPostProcessor implements BeanPostProcessor, PriorityOrdered, ApplicationContextAware {

    private static final Logger log = LoggerFactory.getLogger(JmsModelMappingBeanPostProcessor.class);

    private static final String MODEL_CLASSES_FIELD_NAME = "modelClasses";
    private static final String DESTINATIONS_FIELD_NAME = "destinations";

    private static final Class<JmsMessageHandler> JMS_MESSAGE_HANDLER_CLASS = JmsMessageHandler.class;
    private static final Class<JmsModelMapping> JMS_MODEL_ANNOTATION_CLASS = JmsModelMapping.class;

    private final Map<String, Class<?>> annotatedDtoClasses;

    private ApplicationContext appCtx;

    public JmsModelMappingBeanPostProcessor(String modelClassesPackage) {
        Map<String, Class<?>> annotationDtoClassesMap = new HashMap<>();
        ClassPathScanningCandidateComponentProvider classScanner = new ClassPathScanningCandidateComponentProvider(false);
        classScanner.addIncludeFilter(new AnnotationTypeFilter(JMS_MODEL_ANNOTATION_CLASS));
        Set<BeanDefinition> dtoClassDefinitions = classScanner.findCandidateComponents(modelClassesPackage);
        if (dtoClassDefinitions != null) {
            for (BeanDefinition dtoDefinition : dtoClassDefinitions) {
                String dtoClassName = dtoDefinition.getBeanClassName();
                try {
                    annotationDtoClassesMap.put(dtoClassName, Class.forName(dtoClassName));
                } catch (ClassNotFoundException e) {
                    log.debug("Can not load DTO class {}", dtoClassName);
                }
            }
            annotatedDtoClasses = Collections.unmodifiableMap(annotationDtoClassesMap);
        } else {
            annotatedDtoClasses = Collections.emptyMap();
        }
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> beanClass = AopUtils.getTargetClass(bean);
        if (ClassUtils.isAssignable(JmsMessageSender.class, beanClass) && !CollectionUtils.isEmpty(annotatedDtoClasses)) {
            log.info("Start processing {} for filling mapping DTO->Jms queue", beanName);
            Field destinationsField = ReflectionUtils.findField(beanClass, DESTINATIONS_FIELD_NAME);
            if (destinationsField != null) {
                boolean destinationsFieldAccessible = destinationsField.isAccessible();
                destinationsField.setAccessible(true);
                Map<String, Destination> destinations = (Map<String, Destination>) ReflectionUtils.getField(destinationsField, bean);
                destinationsField.setAccessible(destinationsFieldAccessible);
                if (destinations == null) {
                    destinations = new HashMap<>();
                    ((JmsMessageSender) bean).setDestinations(destinations);
                }
                for (Map.Entry<String, Class<?>> entry : annotatedDtoClasses.entrySet()) {
                    JmsModelMapping annotation = entry.getValue().getAnnotation(JMS_MODEL_ANNOTATION_CLASS);
                    String queueBeanName = annotation.value();
                    if (queueBeanName != null) {
                        Queue queueBean;
                        try {
                            queueBean = appCtx.getBean(queueBeanName, Queue.class);
                            if (queueBean != null && !destinations.containsKey(entry.getKey())) {
                                log.info("Add new destination ({}, {}) for {} bean", entry.getKey(), queueBeanName, beanName);
                                destinations.put(entry.getKey(), queueBean);
                            }
                        } catch (BeansException e) {
                            log.info("Skipped runtime mapping for {}. Probably its already present in the 'destinations' map.", entry.getValue());
                        }
                    }
                }
            }
            log.info("Start processing {} for filling modelClasses (DTO objects)", beanName);
            fillModelClasses(beanClass, bean);
            log.info("End processing {}", beanName);
        } else if (ClassUtils.isAssignable(JmsMessageListener.class, beanClass)) {
            log.info("Start processing {} for filling delegates DTO->Handler object", beanName);
            JmsMessageListener jmsMessageListener = (JmsMessageListener) bean;
            Class<?> handlerClass = jmsMessageListener.getType();
            if (handlerClass != null) {
                Map<String, Object> dtoDelegates = jmsMessageListener.getDelegates();
                if (dtoDelegates == null) {
                    dtoDelegates = new HashMap<>();
                    jmsMessageListener.setDelegates(dtoDelegates);
                }
                Map<String, ?> handlerBeans = appCtx.getBeansOfType(handlerClass);
                if (!CollectionUtils.isEmpty(handlerBeans)) {
                    for (Map.Entry<String, ?> handlerEntry : handlerBeans.entrySet()) {
                        Class<?>[] dtoClasses = GenericTypeResolver.resolveTypeArguments(AopUtils.getTargetClass(handlerEntry.getValue()), JMS_MESSAGE_HANDLER_CLASS);
                        if (dtoClasses != null && dtoClasses.length > 0) {
                            Class<?> requestClass = dtoClasses[0];
                            if (!dtoDelegates.containsKey(requestClass.getName())) {
                                log.info("Add new delegate mapping ({}, {}) for {} bean", requestClass.getName(), handlerEntry.getKey(), beanName);
                                dtoDelegates.put(requestClass.getName(), handlerEntry.getValue());
                            }
                        }
                    }
                }
            }
            log.info("Start processing {} for filling modelClasses (DTO objects)", beanName);
            fillModelClasses(beanClass, bean);
            log.info("End processing {}", beanName);
        } else if (ClassUtils.isAssignable(JmsMessageReceiver.class, beanClass)) {
            log.info("Start processing {} for filling modelClasses (DTO objects)", beanName);
            fillModelClasses(beanClass, bean);
            log.info("End processing {}", beanName);
        }
        return bean;
    }

    // fills `modelClasses` field in JmsMessageSender, JmsMessageListener, JmsMessageReceiver beans
    private void fillModelClasses(Class<?> beanClass, Object bean) {
        Field modelClassesField = ReflectionUtils.findField(beanClass, MODEL_CLASSES_FIELD_NAME);
        if (modelClassesField != null) {
            modelClassesField.setAccessible(true);
            try {
                modelClassesField.set(bean, Collections.unmodifiableSet(new HashSet<>(annotatedDtoClasses.values())));
            } catch (IllegalAccessException e) {
                log.error(String.format("Could not set %s field", MODEL_CLASSES_FIELD_NAME), e);
            }
        }
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.appCtx = applicationContext;
    }

}

