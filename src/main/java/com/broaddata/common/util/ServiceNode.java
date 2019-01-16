/*
 * ServiceNode.java
 */

package com.broaddata.common.util;

import com.broaddata.common.model.enumeration.ComputingNodeServiceStatus;
import org.apache.log4j.Logger;
import static java.lang.System.exit;
import java.util.Properties;

import com.broaddata.common.model.platform.ComputingNode;
import java.lang.management.ManagementFactory;
import java.util.Date;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.persistence.EntityManager;
import org.apache.commons.lang.exception.ExceptionUtils;
 
public class ServiceNode implements ServiceNodeMBean
{
    static final Logger log = Logger.getLogger("ServiceNode");
 
    protected Properties serviceProperties = null;
    protected int dataserviceInstanceId = 0;
    protected ComputingNode computingNode = null;
    protected volatile int serviceStatus = ComputingNodeServiceStatus.STARTING.getValue();
    protected Counter serviceCounter = new Counter();
    protected boolean useAsyncSend = false;
    protected String nodeIdStr;
    protected boolean needRegisterToESB; 
    protected boolean needRegisterToRestESB = true; 
    
    public ServiceNode() 
    {
        initJMX();
        
        initNode();
    }
    
    public ServiceNode(boolean useAsyncSend) 
    {
        this.useAsyncSend = useAsyncSend;
        
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
        log.info("Initialize Service Node ...");

        serviceProperties = new Properties();

        try 
        {
            serviceProperties.load(FileUtil.getFileInputStream(CommonKeys.NODE_CONFIG));
                            
            String nodeTypes = serviceProperties.getProperty("node.type");
            nodeIdStr = serviceProperties.getProperty("node.id");
            Util.commonServiceIPs = serviceProperties.getProperty(CommonKeys.COMMON_SERVICE_IP);
            
            log.info("commonServiceIp="+Util.commonServiceIPs+" node.id="+nodeIdStr+" no.type="+nodeTypes);
            
            if ( !nodeTypes.contains("service_node") )
            {
                log.info("This node type cannot run service node service!!! ");
                exit(-1);
            }        
                        
            String needRegisterToESBStr = serviceProperties.getProperty("needRegisterToESB");

            if ( needRegisterToESBStr == null || !needRegisterToESBStr.toLowerCase().equals("yes") )
                needRegisterToESB = false;
            else
                needRegisterToESB = true;

            String needRegisterToRestESBStr = serviceProperties.getProperty("needRegisterToRestESB");

            if ( needRegisterToRestESBStr == null || !needRegisterToRestESBStr.toLowerCase().equals("yes") )
                needRegisterToRestESB = false;
            else
                needRegisterToRestESB = true;
                            
            setServiceStatus(ComputingNodeServiceStatus.CONNECTING_TO_DB.getValue());
            
            while(dataserviceInstanceId == 0)
            {
                dataserviceInstanceId = Util.getDataserviceInstanceId(Integer.parseInt(nodeIdStr));
 
                if (dataserviceInstanceId > 0)
                    break;
                
                Tool.SleepAWhile(3, 0);
                                
                log.info(" Failed to get dataserviceInstanceId, try again ......commonService IP="+Util.commonServiceIPs);              
            }

            log.info("connecting to MQ ...");
            
            setServiceStatus(ComputingNodeServiceStatus.CONNECTING_TO_MQ.getValue());
            
            while(RuntimeContext.MQConn == null)
            {
                EntityManager platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
                RuntimeContext.MQConn = JMSUtil.getConnection(platformEm,useAsyncSend);
                platformEm.close();
                
                if (RuntimeContext.MQConn != null)
                    break;
                
                Tool.SleepAWhile(3, 0);
                                
                log.info(" Failed to connect to MQ, try again .... ");              
            }
            
            log.info("connect to common service, try again ......commonService IP = "+Util.commonServiceIPs); 
                
            setServiceStatus(ComputingNodeServiceStatus.CONNECTING_TO_COMMON_SERVICE.getValue());
            
            while(computingNode == null)
            {
                computingNode = Util.getComputingNode(Integer.parseInt(nodeIdStr));
                
                RuntimeContext.computingNode = computingNode;
 
                if (computingNode != null)
                    break;
                
                Tool.SleepAWhile(3, 0);
                                
                log.info(" Failed to connect to common service, try again ......commonService IP = "+Util.commonServiceIPs);              
            }
            
            setServiceStatus(ComputingNodeServiceStatus.WORKING.getValue());
        }
        catch(Exception e) 
        {
            log.info("ServiceNode.init() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
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

    public ComputingNode getComputingNode() {
        return computingNode;
    }
}
