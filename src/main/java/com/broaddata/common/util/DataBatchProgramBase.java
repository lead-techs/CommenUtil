/*
 * DataBatchProgramBase.java
 *
 */

package com.broaddata.common.util;
 
import java.util.HashMap;
import java.util.Map;

public abstract class DataBatchProgramBase
{
    protected int organizationId;
    public String[] parameterNames = new String[]{};
    public String[] parameterDescriptions = new String[]{};
    public Map<String,Object> parameterValues = new HashMap<>();
    public Container container = null;
 
    public abstract int execute() throws Exception;
          
    public void init(int organizationId,Container container) {
        this.container = container;
        this.organizationId = organizationId;
    }
}
