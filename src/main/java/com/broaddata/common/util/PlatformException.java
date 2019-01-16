/*
 * PlatformException.java
 *
 */

package com.broaddata.common.util;
 
public class PlatformException extends Exception
{ 
    private static final long serialVersionUID = -6431326419749306521L;
    
    Object[] vals = null;
    
    /** 
     * Creates a new instance of PlatformException 
     */   
    public PlatformException(String message, Object... args) {
        super(message);
        this.vals = args;
    }

    /** 
     * Creates a new instance of PlatformException 
     */   
    public PlatformException(Throwable cause, String message, Object... args) {
        super(message, cause);
        this.vals = args;
    }
    
    /**
     * Methods returns arguments of the message.
     * @author mgregor
     */
    public Object[] getVals() {
        return vals;
    }
}