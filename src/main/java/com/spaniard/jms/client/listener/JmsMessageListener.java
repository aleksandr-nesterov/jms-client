package com.spaniard.jms.client.listener;

import com.spaniard.jms.client.core.Strategy;
import com.spaniard.jms.client.core.support.marshalling.AbstractMarshaller.MarshallerFactory;
import com.spaniard.jms.client.core.support.marshalling.Marshaller;
import com.spaniard.jms.client.core.support.unmarshalling.Unmarshaller;
import com.spaniard.jms.client.exception.IllegalPropertyException;
import com.spaniard.jms.client.exception.JmsException;
import com.spaniard.jms.client.core.support.unmarshalling.AbstractUnmarshaller;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MethodInvoker;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static com.spaniard.jms.client.core.support.ClassName.getClassName;

public class JmsMessageListener<H> extends JmsAbstractMessageListener implements MessageListener {

    private static final Logger logger = LoggerFactory.getLogger(getClassName());

    private String modelPackage;
    // default is XML
    private Strategy strategy = Strategy.XML;

    private String delegateMethod;

    private Map<String, H> delegates;

    private boolean propertiesRequired;

    private String replyToQueue;
    // it is used in BeanPostProcessor, in order to avoid using jaxb ObjectFactory
    private Set<Class> modelClasses;
    // field is required for searching class-handler beans in the classpath
    private final Class<H> type;

    // constructor
    public JmsMessageListener() {
        this(null);
    }

    // constructor
    // required for using in java reflection to get class-handler beans i.e (BeanPostProcessor)
    public JmsMessageListener(Class<H> type) {
        this.type = type;
    }

    public void setStrategy(Strategy strategy) {
        this.strategy = strategy;
    }

    public String getDelegateMethod() {
        return delegateMethod;
    }

    public void setDelegateMethod(String delegateMethod) {
        this.delegateMethod = delegateMethod;
    }

    public Map<String, H> getDelegates() {
        return delegates;
    }

    public void setDelegates(Map<String, H> delegates) {
        this.delegates = delegates;
    }

    public boolean isPropertiesRequired() {
        return propertiesRequired;
    }

    public void setPropertiesRequired(boolean propertiesRequired) {
        this.propertiesRequired = propertiesRequired;
    }

    public void setModelPackage(String modelPackage) {
        this.modelPackage = modelPackage;
    }

    public void setReplyToQueue(String replyToQueue) {
        this.replyToQueue = replyToQueue;
    }

    public Class<H> getType() {
        return type;
    }

    @Override
    protected void handleTextMessage(final TextMessage textMessage) throws JmsException {
        if (delegates == null) {
            throw new IllegalPropertyException("No 'delegates' map is set");
        }
        if (delegateMethod == null) {
            throw new IllegalPropertyException("No 'delegateMethod' property is set");
        }

        // unmarshall incoming message
        Unmarshaller unmarshaller = Objects.isNull(modelPackage)
                ? AbstractUnmarshaller.UnmarshallerFactory.fromModelClasses(modelClasses)
                : AbstractUnmarshaller.UnmarshallerFactory.fromModelPackage(modelPackage);
        Object modelObject;
        try {
            modelObject = unmarshaller.unmarshall(strategy, textMessage.getText());
        } catch (JMSException e) {
            throw new JmsException(e.getMessage(), e);
        }

        final Object result = invokeMethod(propertiesRequired ? new Object[]{modelObject, getJmsProperties(textMessage)}
                : new Object[]{modelObject});
        // if we have a response from a handler -> send to reply queue
        if (result != null) {
            sendToReplyQueue(result, textMessage);
        }
    }

    // do extra routing if we need to
    protected void sendToReplyQueue(final Object result, final TextMessage textMessage) throws JmsException {
        try {
            final Destination replyToDestination = textMessage.getJMSReplyTo();
            final String correlationID = textMessage.getJMSCorrelationID();
            if (replyToDestination != null && correlationID != null) {
                // if we have a `routeQueue` set - we should copy and send a message to that queue
                // otherwise `replyTo` queue is used
                final Destination destination = replyToQueue != null ? new ActiveMQQueue(replyToQueue) : replyToDestination;
                final String stringMessage = marshall(result);
                try {
                    jmsMessageSender.execute(destination, (session, messageProducer) -> {
                        final TextMessage newTextMessage = session.createTextMessage(stringMessage);
                        // copy properties
                        final Enumeration en = textMessage.getPropertyNames();
                        while (en.hasMoreElements()) {
                            final String element = en.nextElement().toString();
                            newTextMessage.setStringProperty(element, textMessage.getStringProperty(element));
                        }
                        newTextMessage.setJMSCorrelationID(textMessage.getJMSCorrelationID());
                        newTextMessage.setJMSReplyTo(textMessage.getJMSReplyTo());
                        if (textMessage.getJMSExpiration() > 0) {
                            // ttl is set
                            messageProducer.setTimeToLive(textMessage.getJMSExpiration() - textMessage.getJMSTimestamp());
                        }

                        messageProducer.send(newTextMessage);
                        return null;
                    });
                } catch (Exception e) {
                    logger.warn("Could not sent to reply queue", e);
                }
            }
        } catch (JMSException e) {
            throw new JmsException(e.getMessage(), e);
        }
    }

    protected Map<String, String> getJmsProperties(final TextMessage textMessage) throws JmsException {
        final Map<String, String> properties = new HashMap<>();
        try {
            final Enumeration en = textMessage.getPropertyNames();
            while (en.hasMoreElements()) {
                final String element = en.nextElement().toString();
                properties.put(element, textMessage.getStringProperty(element));
            }
        } catch (JMSException e) {
            throw new JmsException(e.getMessage(), e);
        }
        return properties;
    }

    protected Object invokeMethod(final Object[] delegateArguments) throws JmsException {
        final H delegateObject = delegates.get(delegateArguments[0].getClass().getCanonicalName());
        if (delegateObject == null) {
            throw new IllegalPropertyException("No object found for key '" + delegateArguments[0].getClass().getCanonicalName() + "'");
        }
        // call delegate method
        return invokeMethod(delegateArguments, delegateObject);
    }

    private Object invokeMethod(final Object[] delegateArguments, final Object delegateObject) throws JmsException {
        try {
            final MethodInvoker methodInvoker = new MethodInvoker();
            methodInvoker.setTargetObject(delegateObject);
            methodInvoker.setTargetMethod(delegateMethod);
            methodInvoker.setArguments(delegateArguments);
            methodInvoker.prepare();
            return methodInvoker.invoke();
        } catch (NoSuchMethodException | ClassNotFoundException | InvocationTargetException | IllegalAccessException e) {
            throw new JmsException(e.getMessage(), e);
        }
    }

    private String marshall(final Object modelObject) throws JmsException {
        final Marshaller marshaller = Objects.isNull(modelPackage)
                ? MarshallerFactory.fromModelClasses(modelClasses)
                : MarshallerFactory.fromModelPackage(modelPackage);
        return marshaller.marshall(strategy, modelObject);
    }
}
