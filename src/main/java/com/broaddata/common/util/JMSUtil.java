/*
 * JMSUtil.java
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.List;
import org.apache.activemq.ScheduledMessage;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.persistence.EntityManager;  
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.pool.PooledConnection;  
import org.apache.activemq.pool.PooledConnectionFactory;

import com.broaddata.common.model.enumeration.PlatformServiceType;
import com.broaddata.common.model.platform.ServiceInstance;
 
public class JMSUtil 
{
    static final Logger log=Logger.getLogger("JMSUtil");
    static PooledConnection mqConn = null;
            
    public static long getQueuePendingMessageCount(String mqIP,String brokerName,String queueName) throws Exception
    {
        JMXConnector jmxc = null;
        MBeanServerConnection conn;
        long ret = 0l;
        
        try
        {
            JMXServiceURL url = new JMXServiceURL(String.format("service:jmx:rmi:///jndi/rmi://%s:1099/jmxrmi",mqIP));
            jmxc = JMXConnectorFactory.connect(url,null);
            conn = jmxc.getMBeanServerConnection();
                                                        
            ObjectName activeMQ = new ObjectName(String.format("org.apache.activemq:type=Broker,brokerName=%s",brokerName));
            BrokerViewMBean mbean = (BrokerViewMBean) MBeanServerInvocationHandler.newProxyInstance(conn, activeMQ,BrokerViewMBean.class, true);
     
            ObjectName[] list = mbean.getQueues();
            
            for (ObjectName name : list) 
            {
                QueueViewMBean queueMbean = (QueueViewMBean)MBeanServerInvocationHandler.newProxyInstance(conn, name, QueueViewMBean.class, true);

                if (queueMbean.getName().equals(queueName)) {
                    return queueMbean.getQueueSize(); 
                }
            }
        }
        catch(Exception e)
        {
            log.debug("getQueuePendingMessageCount() failed! e="+e);
            throw e;
        }
        finally
        {
            if ( jmxc != null)
                jmxc.close();
        }
       
        return ret;
    }
    
       public static void SendMessageToQueue1(PooledConnection MQConn,String queueName,int organizationId, String objId, String objChecksum,int delaySeconds,boolean isPartialUpdate,boolean needToSaveImmediately) throws Exception 
    {
        String indexRequestMessage = String.format("%d,%s,%s",organizationId, objId, objChecksum);

        if ( isPartialUpdate )
            indexRequestMessage += ",1";

        if ( needToSaveImmediately )
            indexRequestMessage += ",1";
                
        Map<String,Object> properties = new HashMap();
        properties.put("organizationId", String.valueOf(organizationId));
        
        int retry = 3;
        
        while(retry>0)
        {
            retry--;
            
            try 
            {
                sendMessage(MQConn,queueName,indexRequestMessage,properties,delaySeconds);
                break;
            }
            catch(Exception e)
            {
                log.error(" JMSUtil.sendMessage failed! dataobjectId="+objId+" queueName="+queueName+ " Checksum="+objChecksum);
                //Tool.SleepAWhile(1, 0);
            }
        }
    }
 
    public static void SendMessageToQueue(PooledConnection MQConn,String queueName,int organizationId, String objId, String objChecksum,int delaySeconds) throws Exception 
    {
        String indexRequestMessage = String.format("%d,%s,%s",organizationId, objId, objChecksum);
                
        Map<String,Object> properties = new HashMap();
        properties.put("organizationId", String.valueOf(organizationId));
        
        int retry = 3;
        
        while(retry>0)
        {
            retry--;
            
            try 
            {
                sendMessage(MQConn,queueName,indexRequestMessage,properties,delaySeconds);
                break;
            }
            catch(Exception e)
            {
                log.error(" JMSUtil.sendMessage failed! dataobjectId="+objId+" queueName="+queueName+ " Checksum="+objChecksum);
                //Tool.SleepAWhile(1, 0);
            }
        }
    }
    
    public static void SendMessageToQueue2(PooledConnection MQConn,String queueName,int organizationId, String objId, String objChecksum,int dataChannel,int delaySeconds) throws Exception 
    {
        String indexRequestMessage = String.format("%d,%s,%s,%d",organizationId, objId, objChecksum, dataChannel);
                
        Map<String,Object> properties = new HashMap();
        properties.put("organizationId", String.valueOf(organizationId));
        
        int retry = 3;
        
        while(retry>0)
        {
            retry--;
            
            try 
            {
                sendMessage(MQConn,queueName,indexRequestMessage,properties,delaySeconds);
                break;
            }
            catch(Exception e)
            {
                log.error(" JMSUtil.sendMessage failed! dataobjectId="+objId+" queueName="+queueName+ " Checksum="+objChecksum);
                //Tool.SleepAWhile(1, 0);
            }
        }
    }
    
    public static void SendMessagesToQueue(PooledConnection MQConn,String queueName,int organizationId,List<String> indexRequestMessages,int delaySeconds) throws Exception 
    {
        Map<String,Object> properties = new HashMap();
        properties.put("organizationId", String.valueOf(organizationId));
        
        int retry = 3;
        
        while(retry>0)
        {
            retry--;
            
            try 
            {
                sendMessages(MQConn,queueName,indexRequestMessages,properties,delaySeconds);
                break;
            }
            catch(Exception e)
            {
                log.error(" JMSUtil.sendMessage failed! e="+e);
                
                if ( retry == 0 )
                    throw e;
            }
        }
    }
    
    public static void SendMessageToQueueForConfirmIndexed(PooledConnection MQConn,String QueueName,int organizationId, String dataobjectId, String objChecksum, String indexName,int repositoryId,long delaySeconds) throws Exception 
    {
        String indexRequestMessage = String.format("%d,%s,%s,%s,%d",organizationId,dataobjectId,objChecksum,indexName,repositoryId);
        Map<String,Object> properties = new HashMap();
        properties.put("organizationId", String.valueOf(organizationId));

        int retry = 3;
        
        while(retry>0)
        {
            retry--;
            
            try 
            {
                JMSUtil.sendMessage(MQConn,QueueName,indexRequestMessage,properties,delaySeconds);
                break;
            }
            catch(Exception e)
            {
                log.error(" JMSUtil.sendMessage failed!");
                Tool.SleepAWhile(1, 0);
            }
        }
    }
    
    public static void SendBatchMessageToQueueForConfirmIndexed(PooledConnection MQConn,String QueueName,int organizationId, String message, int delaySeconds) throws Exception 
    {
        String indexRequestMessage = message;
        Map<String,Object> properties = new HashMap();
        properties.put("organizationId", String.valueOf(organizationId));

        int retry = 3;
        
        while(retry>0)
        {
            retry--;
            
            try 
            {
                JMSUtil.sendMessage(MQConn,QueueName,indexRequestMessage,properties,delaySeconds);
                break;
            }
            catch(Exception e)
            {
                log.error(" JMSUtil.sendMessage failed!");
                Tool.SleepAWhile(1, 0);
            }
        }
    }
    
    public static void sendMessageToQueue(PooledConnection MQConn,String QueueName,int organizationId, String message, int delaySeconds) throws Exception 
    {
        String indexRequestMessage = message;
        Map<String,Object> properties = new HashMap();
        properties.put("organizationId", String.valueOf(organizationId));

        int retry = 3;
        
        while(retry>0)
        {
            retry--;
            
            try 
            {
                JMSUtil.sendMessage(MQConn,QueueName,indexRequestMessage,properties,delaySeconds);
                break;
            }
            catch(Exception e)
            {
                log.error(" JMSUtil.sendMessage failed!");
                Tool.SleepAWhile(1, 0);
            }
        }
    }
    
    public static String getMessageSelector(int[] organizations) 
    {
        String messageSelector = "";
        
        for(int i=0;i<organizations.length;i++)
        {
            if ( i == organizations.length-1 )
                messageSelector += "organizationId='"+String.valueOf(organizations[i]) + "'";
            else
                messageSelector += "organizationId='"+String.valueOf(organizations[i]) +"' or ";
        }
        
        return messageSelector;
    }
    
    public static String getMessageSelectorForImport(int[] organizations,int dataChannel) 
    {
        String messageSelector = "";
        
        for(int i=0;i<organizations.length;i++)
        {
            if ( i == organizations.length-1 )
                messageSelector += "organizationId='"+String.valueOf(organizations[i]) + "'";
            else
                messageSelector += "organizationId='"+String.valueOf(organizations[i]) +"' OR ";
        }
        
        if ( organizations.length == 1 )
            messageSelector = String.format("%s AND dataChannel='%d'",messageSelector,dataChannel);
        else
            messageSelector = String.format("( %s ) AND dataChannel='%d'",messageSelector,dataChannel);
                
        return messageSelector;
    }
    
    public static synchronized PooledConnection getConnection(EntityManager platformEm,boolean useAsyncSend)
    {
        if ( mqConn != null )
            return mqConn;
                
        try
        {    
            String sql = String.format("from ServiceInstance si where si.type=%d",PlatformServiceType.MQ_SERVICE.getValue() );               
            ServiceInstance mqServiceInstance = (ServiceInstance)platformEm.createQuery(sql).getSingleResult();            
            
            //String url = "failover:(tcp://127.0.0.1:61616)?initialReconnectDelay=1000";
            String url = String.format("failover:(tcp://%s)?initialReconnectDelay=1000",mqServiceInstance.getLocation());
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();

            log.info("connection to MQ ... url="+url+" useAsyncSend="+useAsyncSend);
            factory.setBrokerURL(url);
            log.info(" maxThreadPool size="+factory.getMaxThreadPoolSize());
 
            factory.setUseAsyncSend(useAsyncSend);
            PooledConnectionFactory poolFactory = new PooledConnectionFactory(factory);
            
            //log.info(" MaximumActiveSessionPerConnection="+poolFactory.getMaximumActiveSessionPerConnection());
            //poolFactory.setMaximumActiveSessionPerConnection(100);
            
            mqConn = (PooledConnection) poolFactory.createConnection(); 
            mqConn.start();
        } 
        catch (Exception e) {
            log.error("failed to create conn"+e.getMessage());
            return null;
        }
        
        return mqConn;
    }
           
    public static String sendMessage(PooledConnection MQConn,String QueueName,Serializable message,Map<String,Object> properties,long delaySeconds) throws Exception
    {    
        try 
        {            
            QueueSession session =MQConn.createQueueSession(false,QueueSession.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(QueueName);
            ObjectMessage msg = session.createObjectMessage(message);
            
            if ( delaySeconds > 0 )
                msg.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, delaySeconds*1000);
            
            if ( properties!= null ) 
            {
                for(Object o : properties.keySet())
                   msg.setObjectProperty((String)o,properties.get((String)o));
            }
 
            QueueSender sender = session.createSender(queue);
            sender.send(msg);
            
            session.close();
            
            return msg.getJMSMessageID();
        } 
        catch (Exception e) 
        {
            log.error("sendMessage failed!",e);
            throw e;
        }
    }
    
    public static  String sendMessage(PooledConnection MQConn,String QueueName,String message,Map<String,Object> properties,int delaySeconds) throws Exception
    {    
        try 
        {            
            QueueSession session =MQConn.createQueueSession(false,QueueSession.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(QueueName);
            TextMessage msg = session.createTextMessage(message);
 
            if ( delaySeconds > 0 )
                msg.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, delaySeconds*1000);
            
            if ( properties!= null )
            {
                for(Object o : properties.keySet())
                   msg.setObjectProperty((String)o,properties.get((String)o));
            }
 
            QueueSender sender = session.createSender(queue); 
            sender.send(msg);
            
            session.close();
            
            return msg.getJMSMessageID();
        } 
        catch (Exception e) 
        {
            log.error("sendMessage failed!",e);
            throw e;
        }
    }
    
    public static void sendMessages(PooledConnection MQConn,String QueueName,List<String> messages,Map<String,Object> properties,int delaySeconds) throws Exception
    {
        try
        {
            QueueSession session =MQConn.createQueueSession(false,QueueSession.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(QueueName);
            QueueSender sender = session.createSender(queue); 
            
            //log.info(" finish create sender delaySeconds="+delaySeconds);
            for(String message : messages)
            {                     
                TextMessage msg = session.createTextMessage(message);

                String[] vals = message.split("\\,");                 
                String dataChannel = vals[3];   // String.format("%d,%s,%s,%d",organizationId, obj.getId(),obj.getChecksum(),dataChannel);
                msg.setStringProperty("dataChannel", dataChannel);

                if ( delaySeconds > 0 )
                    msg.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, delaySeconds*1000);

                if ( properties != null )
                {
                    for(Object o : properties.keySet())
                       msg.setObjectProperty((String)o,properties.get((String)o));
                }
                
                sender.send(msg);
                
                //log.info("sending message= "+msg);
            }
            
            session.close();
            
            //log.info(" start close session");
        } 
        catch (Exception e) 
        {
            log.error("sendMessage failed!",e);
            throw e;
        }
    }
        
    public static ObjectMessage receiveMessage(PooledConnection MQConn,String QueueName,String messageSelector, long timeout) throws Exception
    {    
        try 
        {
            QueueSession session =MQConn.createQueueSession(false,QueueSession.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(QueueName);
            MessageConsumer consumer = session.createReceiver(queue, messageSelector);

            Message msg = consumer.receive(timeout); 

            session.close();
            return (ObjectMessage)msg;
        } 
        catch(Exception e) 
        {
            log.error("receiveMessage failed!",e);
            throw e;
        }
    }
    
    public static void setupReciever(PooledConnection MQConn,String QueueName,MessageListener listener,String messageSelector) throws JMSException
    {  
        try
        {
            QueueSession session = MQConn.createQueueSession(false,Session.CLIENT_ACKNOWLEDGE);

            Queue queue = session.createQueue(QueueName);
           
            QueueReceiver queueReceiver = session.createReceiver(queue,messageSelector);
            //QueueReceiver queueReceiver = session.createReceiver(queue);

            queueReceiver.setMessageListener(listener);
        }
        catch(JMSException e)
        {
            log.error("setupReciever failed!",e);
            throw e;
        }
    }
}
