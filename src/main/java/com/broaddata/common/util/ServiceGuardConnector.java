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
import com.broaddata.common.thrift.serviceguard.ServiceGuard;

public class ServiceGuardConnector
{
        static final Logger log=Logger.getLogger("ServiceGuardConnector");
        
        private ServiceGuard.Client client=null;
        private TTransport transport = null;
            
	public ServiceGuard.Client getClient(String serviceIP) throws Exception 
        {                    
            transport = null;
            
            try 
            {
                //int port = Util.getServiceGuardPort(nodeType);
                
                int port = CommonKeys.THRIFT_SERVICE_GUARD_PORT;
                
                transport = new TSocket(serviceIP, port, CommonKeys.THRIFT_TIMEOUT);

                TProtocol protocol = new TBinaryProtocol(transport);  

                client = new ServiceGuard.Client(protocol);
                
                transport.open();
            } 
            catch (Exception e) 
            {
                throw e;
            }
      
            return client;
	}
        
        public void close() 
        {
            if ( client != null )
                client.getInputProtocol().getTransport().close();
        }
}