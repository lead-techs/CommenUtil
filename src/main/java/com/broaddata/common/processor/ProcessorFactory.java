/*
 * ProcessorFactory.java
 *
 */

package com.broaddata.common.processor;

import org.apache.log4j.Logger;
import java.lang.reflect.Constructor;

import com.broaddata.common.model.platform.ComputingNode;
import com.broaddata.common.model.platform.DatasourceType;
import com.broaddata.common.thrift.commonservice.CommonService;
import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.thrift.dataservice.Job;
import com.broaddata.common.util.ClientRuntimeContext;
import com.broaddata.common.util.CommonServiceConnector;
import com.broaddata.common.util.Counter;
import com.broaddata.common.util.DataServiceConnector;
import com.broaddata.common.util.Util;

public class ProcessorFactory 
{
    static final Logger log=Logger.getLogger("ProcessorFactory");   
    
    public static Processor createProcessor(DatasourceType datasourceType)
    {
        Processor processor;
        
        try 
        {
            Class processorClass = Class.forName(datasourceType.getProcessorClass());
            
            Class[] paramDef = new Class[]{ DatasourceType.class };
            Constructor constructor = processorClass.getConstructor( paramDef );
            Object[] params = new Object[]{ datasourceType };

            processor = (Processor)constructor.newInstance(params);
            
            return processor;
        } 
        catch (Exception e) {
            log.error("failed to createProcessor! e=",e);
        }
        
        return null;
    }

    public static Processor createProcessor(CommonServiceConnector csConn,CommonService.Client csClient,DataServiceConnector dsConn,DataService.Client dsClient,Job job, ComputingNode computingNode,Counter serviceCounter)
    {
        Processor processor;
        
        try
        {
            DatasourceType datasourceType = Util.getDatasourceType(job.getDatasourceType(),ClientRuntimeContext.datasourceTypes);

            Class processorClass = Class.forName(datasourceType.getProcessorClass());
            
            Class[] paramDef = new Class[]{ CommonServiceConnector.class,CommonService.Client.class,DataServiceConnector.class,DataService.Client.class,Job.class,DatasourceType.class,ComputingNode.class,Counter.class };
            Constructor constructor = processorClass.getConstructor( paramDef );
            Object[] params = new Object[]{ csConn,csClient,dsConn,dsClient,job,datasourceType,computingNode,serviceCounter };

            processor = (Processor)constructor.newInstance(params);
            
            return processor;
        }
        catch (Exception e) {
            log.error("failed to createProcessor! e=",e);
        }
        
        return null;
    }
}
