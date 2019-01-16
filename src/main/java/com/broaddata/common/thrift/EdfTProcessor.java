/*
 * EdfTProcessor.java
 */
package com.broaddata.common.thrift;
 
import java.net.InetAddress;
import java.net.Socket;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class EdfTProcessor implements TProcessor 
{
    TProcessor processor;
    
    public EdfTProcessor (TProcessor processor) {
        this.processor=processor;
    }
    
    @Override
    public boolean process(TProtocol in, TProtocol out) throws TException 
    {
       TTransport t = in.getTransport();
 
       Socket socket = ((TSocket)t).getSocket();
       InetAddress ia = socket.getInetAddress();
       
       String connetionKey = String.format("%s_%d",ia.getHostAddress(),socket.getPort());
       ThriftUtil.thriftConnectionKeys.set(connetionKey);
       
       return processor.process(in,out); 
    }
}
