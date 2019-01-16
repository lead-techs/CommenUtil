/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.util;

import com.broaddata.common.model.enumeration.FileType;
import com.broaddata.common.model.organization.AnalyticDataview;
import com.broaddata.common.model.organization.AnalyticQuery;
import com.broaddata.common.model.organization.DataExportTask;
import com.broaddata.common.model.organization.DatasourceConnection;
import com.broaddata.common.model.vo.DataobjectVO;
import com.broaddata.common.thrift.dataservice.DataService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
public class DataExportUtilTest {
    
    public DataExportUtilTest() {
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
     * Test of retrieveDataviewData method, of class DataExportUtil.
     */
    @Ignore
    @Test
    public void testRetrieveDataviewData() throws Exception {
        System.out.println("retrieveDataviewData");
        int organizationId = 0;
        DataService.Client dataService = null;
        AnalyticDataview dataview = null;
        AnalyticQuery query = null;
        int searchFrom = 0;
        int maxExpectedHits = 0;
        int maxExecutionTime = 0;
        Map<String, Object> expResult = null;
        Map<String, Object> result = DataExportUtil.retrieveDataviewData(organizationId, dataService, dataview, query, searchFrom, maxExpectedHits, maxExecutionTime);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of exportDataToDatabase method, of class DataExportUtil.
     */
    @Ignore
    @Test
    public void testExportDataToDatabase() throws Exception {
        System.out.println("exportDataToDatabase");
        DataService.Client dataService = null;
        DataExportTask exportDataTask = null;
        AnalyticDataview dataview = null;
        AnalyticQuery query = null;
        DatasourceConnection datasourceConnection = null;
        DataExportUtil.exportDataviewDataToDatabase(dataService, exportDataTask, null,dataview, datasourceConnection);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of generateExportDocument method, of class DataExportUtil.
     */
    @Ignore
    @Test
    public void testGenerateExportDocument() throws Exception {
        System.out.println("generateExportDocument");
        DataService.Client dataService = null;
        DataExportTask exportDataTask = null;
        AnalyticDataview dataview = null;
        AnalyticQuery query = null;
        String tmpDirectory = "";
        String expResult = "";
       // String result = DataExportUtil.generateExportDocument(dataService, exportDataTask, dataview, query, tmpDirectory);
       // assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of generateFileFromDataobjectVOs method, of class DataExportUtil.
     */
    @Ignore
    @Test
    public void testGenerateFileFromDataobjectVOs() throws Exception {
        System.out.println("generateFileFromDataobjectVOs");
        List<Map<String, Object>> metadataInfoList = null;
        String fileName = "";
        FileType fileType = null;
        List<String> dataobjectColumnNameList = null;
        List<DataobjectVO> dataobjectVOs = null;
        String expResult = "";
       // String result = DataExportUtil.generateFileFromDataobjectVOs(metadataInfoList, fileName, fileType, dataobjectColumnNameList, dataobjectVOs,"GBK");
      // assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of generateEXCELDocument method, of class DataExportUtil.
     */
    @Ignore
    @Test
    public void testGenerateEXCELDocument() throws Exception {
        
        System.out.println("generateEXCELDocument");
        String tmpFilename = "c:\\tmp\\aaa.xlsx";
        String filetitle = "aaa";
        List documentContents = new ArrayList<>();
        String[] columns = new String[]{"aaa","bbb","ccc"};
        
        String[] line1 = new String[]{"aaaa1","bbb1","ccc1"};
        String[] line2 = new String[]{"aaa2","bbb2","ccc2"};
        
        documentContents.add(line1);
        documentContents.add(line2);
        
        DataExportUtil.generateEXCELDocument(tmpFilename, filetitle, documentContents, columns);
        // TODO review the generated test code and remove the default call to fail.

    }

    /**
     * Test of generatePDFDocument method, of class DataExportUtil.
     */
    @Ignore
    @Test
    public void testGeneratePDFDocument() throws Exception {
        System.out.println("generatePDFDocument");
        String tmpFilename = "";
        String filetitle = "";
        List documentContents = null;
        String[] columns = null;
        DataExportUtil.generatePDFDocument(tmpFilename, filetitle, documentContents, columns);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of generateTXTDocument method, of class DataExportUtil.
     */
    @Ignore
    @Test
    public void testGenerateTXTDocument() throws Exception {
        System.out.println("generateTXTDocument");
        String tmpFilename = "";
        String filetitle = "";
        List documentContents = null;
        String[] columns = null;
        DataExportUtil.generateTXTDocument(tmpFilename, filetitle, documentContents, columns,"GBK");
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
}
