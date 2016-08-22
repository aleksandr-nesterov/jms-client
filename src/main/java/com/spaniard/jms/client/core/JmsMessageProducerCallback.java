package com.spaniard.jms.client.core;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
@FunctionalInterface
public interface JmsMessageProducerCallback<Response> {

    /**
     *  Session and MessageProducer is closed by JmsMessageSender.
     *
     * @param session -- jms session
     * @param messageProducer -- jms message producer
     * @return Response
     * @throws JMSException
     */
    Response execute(Session session, MessageProducer messageProducer) throws JMSException;
}
