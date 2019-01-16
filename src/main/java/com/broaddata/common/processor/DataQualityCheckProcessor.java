/*
 * DataQualityCheckProcessor.java
 *
 */

package com.broaddata.common.processor;

import org.apache.log4j.Logger;
import java.nio.ByteBuffer;
import org.apache.commons.lang.exception.ExceptionUtils;

import com.broaddata.common.model.enumeration.ClientJobType;
import com.broaddata.common.model.enumeration.DataQualityCheckDatasourceType;
import com.broaddata.common.model.enumeration.TaskJobStatus;
import com.broaddata.common.model.organization.DataQualityCheckJob;
import com.broaddata.common.model.organization.DataQualityCheckTask;
import com.broaddata.common.model.platform.ComputingNode;
import com.broaddata.common.model.platform.DataQualityCheckDefinition;
import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.thrift.dataservice.QualityCheckJob;
import com.broaddata.common.util.DataQualityCheckManager;
import com.broaddata.common.util.DataServiceConnector;
import com.broaddata.common.util.Tool;

public class DataQualityCheckProcessor
{
    static final Logger log = Logger.getLogger("DataQualityCheckProcessor");
      
    public static void process(DataServiceConnector dsConn,DataService.Client dataService,QualityCheckJob qualityCheckJob, ComputingNode computingNode) throws Exception
    {
        DataQualityCheckJob job = null;
        DataQualityCheckTask task = null;
    
        try
        {
            log.info("11111111111111 dataQualityCheckTask organizationId="+qualityCheckJob.jobId);
            
            job = (DataQualityCheckJob)Tool.deserializeObject(qualityCheckJob.getDataQualityCheckJob());
            task = (DataQualityCheckTask)Tool.deserializeObject(qualityCheckJob.getDataQualityCheckTask());
            
            DataQualityCheckManager manager = new DataQualityCheckManager(dsConn,dataService,job,task);
            
            for(ByteBuffer buf : qualityCheckJob.getDataQualityCheckDefinitions())
            {
                DataQualityCheckDefinition definition = (DataQualityCheckDefinition)Tool.deserializeObject(buf.array());
                
                if ( definition.getCheckDatasourceType() == DataQualityCheckDatasourceType.DATABASE_ITEM.getValue() )
                {
                    manager.checkDatabaseItemData(definition);
                }
                else
                if ( definition.getCheckDatasourceType() == DataQualityCheckDatasourceType.EDF_DATAOBJECT.getValue() )
                {
                     
                }
                else // file
                {
                     
                }
            }            
       
            dataService.updateClientJob(ClientJobType.DATA_QUALITY_CHECK_JOB.getValue(),job.getOrganizationId(),job.getId(),TaskJobStatus.COMPLETE.getValue(), "",0,0,0,"");
        }
        catch (Exception e) 
        {
            String errorInfo = "process data quality check job failed!  exception="+e+" stacktrace="+ExceptionUtils.getStackTrace(e);
            log.error(errorInfo);
            
            dataService.updateClientJob(ClientJobType.DATA_QUALITY_CHECK_JOB.getValue(),job.getOrganizationId(),job.getId(),TaskJobStatus.FAILED.getValue(),errorInfo,0,0,0,"");

            throw e;
        }   
    }
}