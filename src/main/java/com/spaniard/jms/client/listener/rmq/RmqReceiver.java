package com.spaniard.jms.client.listener.rmq;

import com.spaniard.jms.client.core.JmsMessageSender;
import com.spaniard.jms.client.core.Strategy;
import com.spaniard.jms.client.exception.JmsException;
import com.spaniard.jms.client.core.support.ClassName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Queue;

import java.io.UnsupportedEncodingException;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
public class RmqReceiver {

    private static final Logger logger = LoggerFactory.getLogger(ClassName.getClassName());

    private JmsMessageSender jmsMessageSender;

    private Queue destinationQueue;

    public void setJmsMessageSender(JmsMessageSender jmsMessageSender) {
        this.jmsMessageSender = jmsMessageSender;
    }

    public void setDestinationQueue(Queue destinationQueue) {
        this.destinationQueue = destinationQueue;
    }

    public void receive(final byte[] message) {
        try {
            jmsMessageSender.send(destinationQueue, new String(message, "UTF-8"), Strategy.STRING);
        } catch (JmsException | UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

}
