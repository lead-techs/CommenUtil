/*
 * ProgramMetricsCalculationBase.java
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.util.Map;
 
import com.broaddata.common.model.platform.EntityType;
import com.broaddata.common.model.platform.MetricsDefinition;
import com.broaddata.common.util.EntityMetricsCalculator;

public class ProgramMetricsCalculationBase
{
    static final Logger log = Logger.getLogger("ProgramMetricsCalculationBase");
 
    protected EntityMetricsCalculator metricsCalculator;
    public String[] PARAMETERS = new String[]{};
     
    public ProgramMetricsCalculationBase() {
    }
    
    public ProgramMetricsCalculationBase(EntityMetricsCalculator metricsCalculator)
    {
        this.metricsCalculator = metricsCalculator;
    }
        
    public double calculate(EntityType EntityType,String entityId,MetricsDefinition metricsDefinition,Map<String,Object> parameter) throws Exception
    {
        return 0.0;
    }
}
