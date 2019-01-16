/*
 * Service.java
 */

package com.broaddata.common.util;

import static java.lang.System.exit;
import java.lang.management.ManagementFactory;
import org.apache.log4j.Logger;
import java.util.Properties;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.broaddata.common.model.enumeration.ComputingNodeServiceStatus;
import com.broaddata.common.model.platform.ComputingNode;
import java.util.Date;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;

public class DataCollectingNode implements DataCollectingNodeMBean
{
    static final Logger log=Logger.getLogger("DataCollectingNodeService");
    
    protected Properties serviceProperties;
    protected ComputingNode computingNode = null;
    protected volatile int serviceStatus = ComputingNodeServiceStatus.STARTING.getValue();
    protected Counter serviceCounter = new Counter();
    
    public DataCollectingNode() 
    {
        initJMX();
 
        initNode();
    }
    
    private void initJMX()
    {
        try 
        {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();  
            ObjectName name = new ObjectName(String.format("%s:type=%s", CommonKeys.JMX_DOMAIN,CommonKeys.JMX_TYPE_EDF_NODE));
            
            if ( !server.isRegistered(name) )
                server.registerMBean(this, name);

            log.info("Mbean registered! name=" + name);
        }
        catch(Exception e)
        {
            log.info("initJMX() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
        }            
    }
    
    private void initNode()
    {
        log.info("Initialize DataCollectingNodeService() ...");

        serviceProperties = new Properties();

        try 
        {
            serviceProperties.load(FileUtil.getFileInputStream(CommonKeys.NODE_CONFIG));
            String nodeTypes = serviceProperties.getProperty("node.type");
        
            if ( !nodeTypes.contains("data_collecting_node") )
            {
                log.info("This node type ["+nodeTypes+"] cannot run data collecting node service!!! ");
                exit(-1);
            }
        
            setServiceStatus(ComputingNodeServiceStatus.CONNECTING_TO_COMMON_SERVICE.getValue());
             
            while(computingNode == null)
            {
                Util.commonServiceIPs = serviceProperties.getProperty(CommonKeys.COMMON_SERVICE_IP);

                String nodeId = serviceProperties.getProperty("node.id");
                computingNode = Util.getComputingNode(Integer.parseInt(nodeId));
                                
                RuntimeContext.computingNode = computingNode;
                
                if (computingNode != null)
                    break;
                                
                log.info(" Failed to get computingNode, try again ...... commonservice IP="+Util.commonServiceIPs); 
                
                Tool.SleepAWhile(3, 0);
            }
        }
        catch(Exception e) 
        {
            log.info(" DataCollectingNodeService() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
        }
    }

    @Override
    public int getServiceStatus() {
       return serviceStatus;
    }
    
    protected void setServiceStatus(int serviceStatus) {
        this.serviceStatus = serviceStatus;
    }

    public Counter getServiceCounter() {
        return serviceCounter;
    }
    
    @Override
    public long getPerformanceData(String key, Date startTime, Date endTime) 
    {
        return serviceCounter.getValue(key, startTime, endTime);
    }    
    
    @Override
    public long getPerformanceData(String key, int timeType, int timeValue) {
        return serviceCounter.getValue(key, timeType, timeValue);
    }
}
