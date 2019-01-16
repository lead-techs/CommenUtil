/*
 * DataServiceConnector.java
 *
 */
 
package com.broaddata.common.util;

import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import com.broaddata.common.thrift.dataservice.DataService;

public class DataServiceConnector
{
        static final Logger log=Logger.getLogger("DataServiceConnector");
        
        private volatile DataService.Client client = null;
        private TTransport transport = null;
        private String serviceIPs;
        private int organizationId;
        private int userId = -1;
        private String key;
                
        public DataService.Client getClient()
        {
            return client;
        }
        
        public DataService.Client getClient(String serviceIPs,int organizationId,int userId,String key) throws Exception
        {
            getClient(serviceIPs);
            
            this.organizationId = organizationId;
            this.userId = userId;
            this.key = key;
            
            client.login(organizationId, userId, key);
            
            return client;
        }
        
        public DataService.Client getClient(String serviceIPs) throws Exception 
        {                    
            this.serviceIPs = serviceIPs;
            String serviceIP = null;
            int retry = CommonKeys.THRIFT_RETRY_NUM;

            log.info(" dataService getClient ips="+serviceIPs);
            
            while(retry > 0)
            {
                retry--;

                try 
                {
                    serviceIP = Util.pickOneServiceIP(serviceIPs);
                    
                    if ( transport!= null && transport.isOpen() )
                        transport.close();
                    
                    transport = new TSocket(serviceIP, CommonKeys.THRIFT_DATA_SERVICE_PORT, CommonKeys.THRIFT_TIMEOUT);

                    TProtocol protocol = new TBinaryProtocol(transport);  
                    client = new DataService.Client(protocol);

                    transport.open();
                    
                    log.info("succeed to open dataservice! ip="+serviceIP);
                } 
                catch (Exception e) 
                {
                    log.error("failed to open dataservice! ip="+serviceIP+" e="+e);

                    //Tool.SleepAWhile(1, 0);
                    
                    if ( retry > 0 )
                        continue;

                    throw e;
                }

                break;
            }

            return client;
        }
 
        
        public DataService.Client reConnect()
        {
            log.info(" reConnection to "+serviceIPs);
            
            close();
            
            try
            {
                if ( userId == -1 )
                    return getClient(serviceIPs);
                else
                    return getClient(serviceIPs,organizationId,userId,key);
            }
            catch(Exception e)
            {
                log.error(" reconnect failed! e="+e);
                return null;
            }
        }

        public String getServiceIPs() {
            return serviceIPs;
        }
        
        public void close() 
        {
            try
            {
                if ( userId != -1 )
                    client.logout(organizationId, userId, key);
                
                if ( client!= null || client.getInputProtocol().getTransport().isOpen() )
                    client.getInputProtocol().getTransport().close();                
            }
            catch(Throwable e)
            {
                log.error("close transport failed! e"+e);
            }
        }
}
