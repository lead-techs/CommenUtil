/*
 * RuntimeContext.java
 *
 */

package com.broaddata.common.util;

import java.util.List;

import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.model.platform.DatasourceType;
import java.util.ResourceBundle;

public class ClientRuntimeContext 
{
    public static boolean isLogged = false;
    public static ResourceBundle rb = null;
    public static String dataServiceIP;
    public static String dataServiceIPs;
    public static List<Integer> organizationIds;
    
    public static boolean needtoGetPlatformConfig = true;
    public static List<DatasourceType> datasourceTypes = null;
    public static List<DataobjectType> dataobjectTypes = null;
    public static Object adminInstance = null;
}
