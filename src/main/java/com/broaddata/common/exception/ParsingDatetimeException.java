/*
 * ParsingDatetimeException.java
 *
 */

package com.broaddata.common.exception;
 
public class ParsingDatetimeException extends Exception
{ 
    private static final long serialVersionUID = -6431326419749306521L;
    
    Object[] vals = null;
    
    /** 
     * Creates a new instance of AppException 
     * @param message
     * @param args
     */   
    public ParsingDatetimeException(String message, Object... args) {
        super(message);
        this.vals = args;
    }

    /** 
     * Creates a new instance of AppException 
     * @param cause
     * @param message
     * @param args
     */   
    public ParsingDatetimeException(Throwable cause, String message, Object... args) {
        super(message, cause);
        this.vals = args;
    }
    
    /**
     * Methods returns arguments of the message.
     * @author mgregor
     * @return 
     */
    public Object[] getVals() {
        return vals;
    }
}