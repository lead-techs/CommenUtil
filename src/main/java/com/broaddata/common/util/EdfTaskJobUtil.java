/*
 * EdfTaskJobUtil.java
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.LockModeType;

import com.broaddata.common.model.enumeration.DataTaskType;
import com.broaddata.common.model.organization.DatasourceEndJob;
import com.broaddata.common.thrift.dataservice.EdfJob;

public class EdfTaskJobUtil 
{
    static final Logger log = Logger.getLogger("EdfTaskJobUtil");

    public static List<EdfJob> getEdfJobs(EntityManager em, int organizationId, int dataTaskType, int taskId, long jobId)
    {
        List<EdfJob> edfJobs = new ArrayList<>();
        String sql;
         
        if ( dataTaskType == DataTaskType.DATASOURCE_END_TASK.getValue() || dataTaskType == 0 )
        {
            sql = String.format("from DatasourceEndJob where organizationId = %d",organizationId);
            
            if ( jobId > 0 )
                sql += " and id="+jobId;
            
            if ( taskId > 0 )
                sql += " and taskId="+taskId;
            
            List<DatasourceEndJob> jobList =  em.createQuery(sql).getResultList();

            for(DatasourceEndJob job : jobList)
            {
                EdfJob edfJob = new EdfJob();
                
                edfJob.setDataTaskType(DataTaskType.DATASOURCE_END_TASK.getValue());
                edfJob.setJobId(job.getId());
                edfJob.setStatus(job.getStatus());
                edfJob.setTaskId(job.getTaskId());
                
                edfJobs.add(edfJob);
            }
        }
        
        return edfJobs;
    }
    
    public static void changeEdfJobStatus(EntityManager em,int organizationId, long jobId, int newStatus)
    {
        em.getTransaction().begin();
 
        DatasourceEndJob job = em.find(DatasourceEndJob.class, jobId, LockModeType.PESSIMISTIC_READ);
        job.setStatus(newStatus);
        job.setLastUpdatedTime(new Date());
        
        em.getTransaction().commit();
    }    
}
   
   
    