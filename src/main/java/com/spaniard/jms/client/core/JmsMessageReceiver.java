package com.spaniard.jms.client.core;

import com.spaniard.jms.client.core.support.unmarshalling.AbstractUnmarshaller;
import com.spaniard.jms.client.core.support.unmarshalling.Unmarshaller;
import com.spaniard.jms.client.exception.JmsException;
import org.springframework.jms.core.BrowserCallback;
import org.springframework.jms.core.JmsTemplate;

import javax.jms.*;
import javax.jms.Queue;
import java.util.*;

public class JmsMessageReceiver {

    private JmsTemplate jmsTemplate;

    private String modelPackage;
    // default is XML
    private Strategy strategy = Strategy.XML;
    // it is used in BeanPostProcessor, in order to avoid using jaxb ObjectFactory
    private Set<Class> modelClasses;

    public void setJmsTemplate(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

    public void setModelPackage(String modelPackage) {
        this.modelPackage = modelPackage;
    }

    public void setStrategy(Strategy strategy) {
        this.strategy = strategy;
    }

    /**
     * Receive unmarshalled object from given destination. Unmarshalling is done using specified strategy.
     * By default XML.
     *
     * @param destination   -- jms destination (topic, queue)
     * @return unmarshalled text message or null
     * @throws JmsException
     */
    public Object receive(final Destination destination) throws JmsException {
        final Message message = jmsTemplate.receive(destination);
        // no more messages
        if (message == null) return null;
        if (message instanceof TextMessage) return unmarshallMessage((TextMessage) message, strategy);
        throw new JmsException(String.format("Received message [%s] is not of type TextMessage", message.getClass().getName()));
    }

    /**
     * Receive text message from given destination. Message text is casted to responseClass type using strategy.
     *
     * @param destination   -- jms destination (topic, queue)
     * @param strategy      -- strategy (xml, json, string)
     * @param responseClass -- response class
     * @return unmarshalled text message or null
     * @throws JmsException
     */
    public <T> T receive(final Destination destination, final Strategy strategy, final Class<T> responseClass) throws JmsException {
        final Message message = jmsTemplate.receive(destination);
        // no more messages
        if (message == null) return null;
        if (message instanceof TextMessage) return unmarshallMessage((TextMessage) message, strategy, responseClass);
        throw new JmsException(String.format("Received message [%s] is not of type TextMessage", message.getClass().getName()));
    }

    /**
     * Receive selected text message from given destination. Message text is casted to responseClass type using strategy.
     *
     * @param destination   -- jms destination (topic, queue)
     * @param selector      -- jms selector
     * @param strategy      -- strategy (xml, json, string)
     * @param responseClass -- response class
     * @return unmarshalled text message or null
     * @throws JmsException
     */
    public <T> T receiveSelected(final Destination destination, final String selector, final Strategy strategy, final Class<T> responseClass) throws JmsException {
        final Message message = jmsTemplate.receiveSelected(destination, selector);
        // no more messages
        if (message == null) return null;
        if (message instanceof TextMessage) return unmarshallMessage((TextMessage) message, strategy, responseClass);
        throw new JmsException(String.format("Received message [%s] is not of type TextMessage", message.getClass().getName()));
    }

    /**
     * Receive text message from given destination. Message text is casted to responseClass type using specified strategy.
     * By default XML.
     *
     * @param destination   -- jms destination (topic, queue)
     * @param responseClass -- response class
     * @return unmarshalled text message or null
     * @throws JmsException
     */
    public <T> T receive(final Destination destination, final Class<T> responseClass) throws JmsException {
        final Message message = jmsTemplate.receive(destination);
        // no more messages
        if (message == null) return null;
        if (message instanceof TextMessage) return unmarshallMessage((TextMessage) message, strategy, responseClass);
        throw new JmsException(String.format("Received message [%s] is not of type TextMessage", message.getClass().getName()));
    }

    /**
     * Receive selected text message from given destination. Message text is casted to responseClass type using default strategy (XML).
     *
     * @param destination   -- jms destination (topic, queue)
     * @param selector      -- jms selector
     * @param responseClass -- response class
     * @return unmarshalled text message or null
     * @throws JmsException
     */
    public <T> T receiveSelected(final Destination destination, final String selector, final Class<T> responseClass) throws JmsException {
        final Message message = jmsTemplate.receiveSelected(destination, selector);
        // no more messages
        if (message == null) return null;
        if (message instanceof TextMessage) return unmarshallMessage((TextMessage) message, strategy, responseClass);
        throw new JmsException(String.format("Received message [%s] is not of type TextMessage", message.getClass().getName()));
    }

    /**
     * Receives a batch of text messages from given destination. Message text is casted to responseClass type
     * using specified strategy.
     * By default XML.
     *
     * @param destination   -- jms destination (topic, queue)
     * @param selector      -- jms selector
     * @param responseClass -- response class
     * @param batchSize -- maximum number of messages to read. Must be greater than 0
     * @return unmarshalled text message or null
     * @throws JmsException
     */
    public <T> List<T> receiveBatchSelected(final Destination destination, final String selector,
                                            final Class<T> responseClass, final int batchSize) throws JmsException {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize is invalid");
        }
        return jmsTemplate.execute(session -> {
            MessageConsumer messageConsumer = session.createConsumer(destination, selector);
            List<T> batch = new ArrayList<>(batchSize);
            int count = 0;
            Message message;
            while (count++ < batchSize && (message = messageConsumer.receive(jmsTemplate.getReceiveTimeout())) != null) {
                if (!(message instanceof TextMessage)) return null;
                try {
                    batch.add(unmarshallMessage((TextMessage) message, strategy, responseClass));
                } catch (JmsException e) {
                    throw new JMSException(e.getMessage());
                }
            }
            return batch;
        }, true);
    }

    /**
     * Browse a given queue and select specific messages
     *
     * @param queue         -- queue name
     * @param selector      -- jms selector
     * @param responseClass -- class type
     * @return list of responseClass
     * @throws JMSException
     */
    public <T> List<T> browseSelected(final Queue queue, final String selector, final Class<T> responseClass) {
        return jmsTemplate.browseSelected(queue, selector, new BrowserCallback<List<T>>() {
            @Override
            public List<T> doInJms(Session session, QueueBrowser browser) throws JMSException {
                final List<T> resultList = new ArrayList<>();
                final Enumeration messages = browser.getEnumeration();
                while (messages.hasMoreElements()) {
                    final Message message = (Message) messages.nextElement();
                    if (message != null && message instanceof TextMessage) {
                        try {
                            resultList.add(unmarshallMessage((TextMessage) message, strategy, responseClass));
                        } catch (JmsException e) {
                            throw new JMSException(e.getMessage());
                        }
                    }
                }
                return resultList;
            }
        });
    }

    /**
     * Browse a given queue and select specific messages
     *
     * @param queue         -- queue name
     * @param selector      -- jms selector
     * @param strategy      -- strategy
     * @param responseClass -- class type
     * @return list of responseClass
     * @throws JMSException
     */
    public <T> List<T> browseSelected(final Queue queue, final String selector, final Strategy strategy, final Class<T> responseClass) {
        return jmsTemplate.browseSelected(queue, selector, new BrowserCallback<List<T>>() {
            @Override
            public List<T> doInJms(Session session, QueueBrowser browser) throws JMSException {
                final List<T> resultList = new ArrayList<>();
                final Enumeration messages = browser.getEnumeration();
                while (messages.hasMoreElements()) {
                    final Message message = (Message) messages.nextElement();
                    if (message != null && message instanceof TextMessage) {
                        try {
                            resultList.add(unmarshallMessage((TextMessage) message, strategy, responseClass));
                        } catch (JmsException e) {
                            throw new JMSException(e.getMessage());
                        }
                    }
                }
                return resultList;
            }
        });
    }

    /**
     * Browse a given queue and select specific messages
     *
     * @param queue         -- queue name
     * @param selector      -- jms selector
     * @param responseClass -- class type
     * @return list of responseClass
     * @throws JMSException
     */
    public <T> List<T> browseSelected(final Queue queue, final Map<String, String> selector, final Class<T> responseClass) {
        return browseSelected(queue, buildSelectorFromMap(selector), responseClass);
    }

    /**
     * Browse a given queue and select specific messages
     *
     * @param queue         -- queue name
     * @param selector      -- jms selector
     * @param strategy      -- strategy
     * @param responseClass -- class type
     * @return list of responseClass
     * @throws JMSException
     */
    public <T> List<T> browseSelected(final Queue queue, final Map<String, String> selector, final Strategy strategy, final Class<T> responseClass) {
        return browseSelected(queue, buildSelectorFromMap(selector), strategy, responseClass);
    }

    // return either NULL or SQL-92 formatted string containing JMS selector
    private String buildSelectorFromMap(final Map<String, String> selectorMap) {
        String selectorStr = null;
        if (selectorMap != null && !selectorMap.isEmpty()) {
            final StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> selEntry : selectorMap.entrySet()) {
                final String selKey = selEntry.getKey();
                final String selValue = selEntry.getValue();
                if (selKey != null && !selKey.isEmpty() && selValue != null && !selValue.isEmpty()) {
                    if (sb.length() > 0) sb.append(" AND ");
                    sb.append(selKey).append(" = '").append(selValue).append("'");
                }
            }
            if (sb.length() > 0) {
                selectorStr = sb.toString();
            }
        }
        return selectorStr;
    }

    private <T> T unmarshallMessage(final TextMessage textMessage, final Strategy strategy, final Class<T> responseClass) throws JmsException {
        final Object modelObject = getModelObject(textMessage, strategy);
        return responseClass.cast(modelObject);
    }

    private Object unmarshallMessage(final TextMessage textMessage, final Strategy strategy) throws JmsException {
        return getModelObject(textMessage, strategy);
    }

    private Object getModelObject(final TextMessage textMessage, final Strategy strategy) throws JmsException {
        final Unmarshaller unmarshaller = Objects.isNull(modelPackage)
                ? AbstractUnmarshaller.UnmarshallerFactory.fromModelClasses(modelClasses)
                : AbstractUnmarshaller.UnmarshallerFactory.fromModelPackage(modelPackage);
        try {
            return unmarshaller.unmarshall(strategy, textMessage.getText());
        } catch (JMSException e) {
            throw new JmsException(e.getMessage(), e);
        }
    }


}
