/*
 * RuntimeContext.java
 *
 */

package com.broaddata.common.util;

import com.broaddata.common.model.platform.ComputingNode;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.model.platform.DatasourceType;
import com.broaddata.common.thrift.commonservice.ServiceInstanceInfo;
import java.util.List;
import org.apache.activemq.pool.PooledConnection;

public class RuntimeContext 
{
    public static ComputingNode computingNode = null;
    public static PooledConnection MQConn = null;
    public static boolean isDataSavetoCloudStorage = false;
    public static String cloudStorageType = "OSS";   
    public static List<DatasourceType> datasourceTypes = null;
    public static List<DataobjectType> dataobjectTypes = null;
    public static List<ServiceInstanceInfo> serviceInstanceInfo = null;
    
    public static String tempWorkingDirectory = "../tmp";
    
    public static boolean internetVersion = false;
}
