/*
 * Service.java
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import static java.lang.System.exit;
import java.lang.management.ManagementFactory;
import java.util.Properties;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.broaddata.common.model.enumeration.ComputingNodeServiceStatus;
import java.util.Date;

public class ManagementNode implements ManagementNodeMBean
{
    static final Logger log = Logger.getLogger("ManagementNodeService");
    
    protected Properties serviceProperties;
    protected int dataserviceInstanceId; 
    protected volatile int serviceStatus = ComputingNodeServiceStatus.STARTING.getValue();
    protected Counter serviceCounter = new Counter();
    
    public ManagementNode() 
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
            log.info("initJMX() failed! e="+e);
            e.printStackTrace();
        }            
    }
    private void initNode()
    {
        log.info("Initialize Service ...");

        serviceProperties = new Properties();
  
        try 
        {                          
            serviceProperties.load(FileUtil.getFileInputStream(CommonKeys.NODE_CONFIG)); 
            String nodeTypes = serviceProperties.getProperty("node.type");
         
            if ( !nodeTypes.contains("management_node") )
            {
                log.info("This node type ["+nodeTypes+"] cannot run management node service!!! ");
                exit(-1);
            }

            Util.commonServiceIPs = serviceProperties.getProperty(CommonKeys.COMMON_SERVICE_IP); 
        }
        catch(Exception e) 
        {
            log.info(" Exception:getClass().getReesourceAsStream e="+e);
            e.printStackTrace();
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
