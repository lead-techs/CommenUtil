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

import com.broaddata.common.thrift.commonservice.CommonService;

public class CommonServiceConnector
{
    static final Logger log=Logger.getLogger("CommonServiceConnector");

    private CommonService.Client client=null;
    private TTransport transport = null;

    public CommonService.Client getClient(String serviceIPs) throws Exception 
    {                    
        String serviceIP = null;
        int retry = CommonKeys.THRIFT_RETRY_NUM;
        
        log.debug(" common Service getClient ips="+serviceIPs);
           
        while(retry > 0)
        {
            retry--;
            
            try 
            {
                serviceIP = Util.pickOneServiceIP(serviceIPs);
                
                if ( transport == null)
                    transport = new TSocket(serviceIP,CommonKeys.THRIFT_COMMON_SERVICE_PORT,CommonKeys.THRIFT_TIMEOUT);

                //TSocket aa = new TSocket(serviceIP,CommonKeys.THRIFT_COMMON_SERVICE_PORT,CommonKeys.THRIFT_TIMEOUT);

                TProtocol protocol = new TBinaryProtocol(transport); 

                client = new CommonService.Client(protocol);
                transport.open(); 
                
                log.debug("succeed to open commonservice! ip="+serviceIP);
            } 
            catch (Exception e)
            {
                log.error("failed to open commonservice! ip="+serviceIP+" e="+e);
                
                Tool.SleepAWhile(1, 0);
                
                if ( retry > 0 )
                    continue;
                
                throw e;
            }
            
            break;
        }
      
        return client;
    }

    public void close() 
    {
        if ( client != null )
            client.getInputProtocol().getTransport().close();
    }
}