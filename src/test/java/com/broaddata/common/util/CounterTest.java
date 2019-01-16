/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.util;

import java.util.Date;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 *
 * @author edf
 */
public class CounterTest {
    
    public CounterTest() {
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
     * Test of increase method, of class Counter.
     */
   @Ignore
    @Test
    public void testIncrease() {
        Date startTime = new Date();
        Date endTime;
        
        System.out.println("increase");
        String key = "processedItem";
        long increaseNumber = 5;
        Counter instance = new Counter();
        long expResult = 1L;
        
        long result = instance.increase(key, increaseNumber);
        assertEquals(5, result);
 
        result = instance.increase(key, increaseNumber);
        assertEquals(10, result);
        
    
        endTime = new Date();
 
 
        result = instance.getValue(key, startTime, endTime);
        
        assertEquals(10, result);

    }

    
    
}
