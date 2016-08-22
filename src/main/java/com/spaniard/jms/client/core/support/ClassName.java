package com.spaniard.jms.client.core.support;

/**
 * @author Alexander Nesterov
 * @version 1.0
 */
public class ClassName {

    public static String getClassName() {
        try {
            throw new Exception();
        } catch (Exception e) {
            return e.getStackTrace()[1].getClassName();
        }
    }
}
