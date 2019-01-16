/*
 * DatasourceProcessingExtension.java
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import com.broaddata.common.model.organization.Datasource;
import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.thrift.dataservice.DataobjectInfo;
import com.broaddata.common.thrift.dataservice.Job;

public abstract class DataProcessingExtensionBase
{
    static final Logger log = Logger.getLogger("DatasourceProcessingExtension");
    
    protected DataService.Client dataService;
    protected Datasource datasource;
    protected Job job;
    protected Map<String,Object> context;
 
    public DataProcessingExtensionBase(DataService.Client dataService,Datasource datasource,Job job)
    {
        this.dataService = dataService;
        this.datasource = datasource;
        this.job = job;
    }
    
    public void setContext(Map<String,Object> context)
    {
        this.context = context;
    }
    
    public Map<String,Object> getContext()
    {
        return this.context;
    }
    
    public abstract void dataProcessing(DataobjectInfo dataobject, List<String> contentIds, List<ByteBuffer> contentBinaries) throws Exception;
}
