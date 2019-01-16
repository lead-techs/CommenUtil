/*
 * DataBatchProgramBase.java
 *
 */

package com.broaddata.common.modelprocessor;
 
import java.util.HashMap;
import java.util.Map;

public abstract class ModelProcessor
{
    public abstract void init(Map<String,Object> parameters) throws Exception;
    public abstract Map<String,Object> execute() throws Exception;
}
