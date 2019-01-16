/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.broaddata.common.util;

import java.io.Serializable;
import java.util.Map;
import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import org.apache.activemq.pool.PooledConnection;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author fxy1949
 */
public class JMSUtilTest {
    
    public JMSUtilTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of getConnection method, of class JMSUtil.
     */
 
    @Ignore
    @Test
    public void testGetConnection() {
        System.out.println("getConnection");
        Util.commonServiceIPs = "127.0.0.1";
        PooledConnection expResult = null;
        try {
        long ret = JMSUtil.getQueuePendingMessageCount("127.0.0.1","localhost",CommonKeys.INDEX_REQUEST);
         fail("The test case is a prototype.");
        }
        catch(Exception e)
        {
            
        }  
    }

    /**
     * Test of sendMessage method, of class JMSUtil.
     */
    @Ignore
    @Test
    public void testSendMessage() throws Exception {
        System.out.println("sendMessage");
        PooledConnection MQConn = null;
        String QueueName = "";
        Serializable message = null;
        Map<String, Object> properties = null;
        String expResult = "";
        String result = JMSUtil.sendMessage(MQConn, QueueName, message, properties,0);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of receiveMessage method, of class JMSUtil.
     */
    @Ignore    
    @Test
    public void testReceiveMessage() throws Exception {
        System.out.println("receiveMessage");
        PooledConnection MQConn = null;
        String QueueName = "";
        String messageSelector = "";
        long timeout = 0L;
        ObjectMessage expResult = null;
        ObjectMessage result = JMSUtil.receiveMessage(MQConn, QueueName, messageSelector, timeout);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of setupReciever method, of class JMSUtil.
     */
    @Ignore
    @Test
    public void testSetupReciever() throws JMSException {
        System.out.println("setupReciever");
        PooledConnection MQConn = null;
        String QueueName = "";
        MessageListener listener = null;
        String messageSelector = "";
        JMSUtil.setupReciever(MQConn, QueueName, listener, messageSelector);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
}
