/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.util;
 
import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 *
 * @author Duenan
 */
public class CSVUtilsTest {
    
    public CSVUtilsTest() {
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
     * Test of createCSVFile method, of class CSVUtils.
     */
    @Ignore
    @Test
    public void testCreateCSVFile() {
        System.out.println("createCSVFile");
        List exportData = null;
        LinkedHashMap map = null;
        String outPutPath = "";
        String fileName = "";
        File expResult = null;
        File result = CSVUtils.createCSVFile(exportData, map, outPutPath, fileName);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of deleteFiles method, of class CSVUtils.
     */
    @Ignore
    @Test
    public void testDeleteFiles() {
        System.out.println("deleteFiles");
        String filePath = "";
        CSVUtils.deleteFiles(filePath);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of deleteFile method, of class CSVUtils.
     */
    @Ignore
    @Test
    public void testDeleteFile() {
        System.out.println("deleteFile");
        String filePath = "";
        String fileName = "";
        CSVUtils.deleteFile(filePath, fileName);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
}
