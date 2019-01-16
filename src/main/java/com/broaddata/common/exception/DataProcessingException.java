/*
 * AppException.java
 *
 */

package com.broaddata.common.exception;
 
import com.broaddata.common.util.Util;

public class DataProcessingException extends Exception
{ 
    private static final long serialVersionUID = -6431326419749306521L;
    
    String errorCode;
    
    /** 
     * Creates a new instance of AppException 
     */   
    public DataProcessingException(String message, String errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }
 
    public String getErrorMessage()
    {
        return Util.getBundleMessage(errorCode);
    }
}