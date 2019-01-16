/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.util;

import com.broaddata.common.model.organization.AnalyticDataview;
import com.broaddata.common.model.organization.AnalyticQuery;
import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.thrift.dataservice.Dataset;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 *
 * @author ed
 */
public class DataviewUtilTest {
    
    public DataviewUtilTest() {
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
     * Test of extraceStoreProcedureParameterStrs method, of class DataviewUtil.
     */
    @Ignore
    @Test
    public void testExtraceStoreProcedureParameterStrs() {
        System.out.println("extraceStoreProcedureParameterStrs");
        String sql = "{? = call STORE_PROCEDURE_XXXX(?NUMBER,?VARCHAR2,?VARCHAR2)}";
        List<String> expResult = null;
        List<String> result = DataviewUtil.extraceStoreProcedureParameterStrs(sql,"");
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
 
}
