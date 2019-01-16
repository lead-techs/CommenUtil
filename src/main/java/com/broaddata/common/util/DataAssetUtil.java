/*
 * DataAssetUtil.java
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.EntityManager;
import org.apache.commons.lang.exception.ExceptionUtils;

import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.model.platform.DataobjectTypeCategory;
import com.broaddata.common.thrift.dataservice.DataServiceException;
 
public class DataAssetUtil 
{      
    static final Logger log = Logger.getLogger("DataAssetUtil");      
             
    public static List<String> getDataobjectTypeInfo(EntityManager platformEm,int organizationId, int categoryId) throws Exception
    {
        List<String> list = new ArrayList<>();

        try 
        {
            List<DataobjectType> dataobjectTypeList = getDataobjectTypes(platformEm,organizationId,categoryId);
           
            for(DataobjectType dataobjectType : dataobjectTypeList)
            {
                String name = String.format("%d-%s_type-%s",dataobjectType.getId(),dataobjectType.getName(),dataobjectType.getDescription());                          
                list.add(name);
            }
        } 
        catch (Exception e) 
        {
            String errorInfo = "getDataobjectTypeInfo() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e);
            log.error(errorInfo);
            
            throw new DataServiceException(errorInfo);
        }
  
        return list;
    }
        
    public static List<DataobjectType> getDataobjectTypes(EntityManager platformEm,int organizationId,int dataobjectTypeCategory) 
    {
        List<Integer> childCategoryList = new ArrayList<>();
        
        String childCategoryStr = String.format("%d,",dataobjectTypeCategory);
        getChildCategoryIds(platformEm,childCategoryList,dataobjectTypeCategory);
        
        for(Integer categoryId : childCategoryList)
            childCategoryStr = String.format("%s %d,",childCategoryStr,categoryId);
        
        childCategoryStr = childCategoryStr.substring(0,childCategoryStr.length()-1);
        String sql = String.format("from DataobjectType where organizationId=%d and categoryId in (%s))", organizationId,childCategoryStr);
        log.info("11 sql ="+sql);
        
        List<DataobjectType> dataobjectTypeList = platformEm.createQuery(sql).getResultList();
        
        return dataobjectTypeList;
    }
    
    public static void getChildCategoryIds(EntityManager em, List<Integer> childCategoryList, int dataobjectTypeCategory)
    { 
        String sql = String.format("from DataobjectTypeCategory dtc where dtc.parentCategory = %d",dataobjectTypeCategory);
        List<DataobjectTypeCategory> dataobjectTypeCategoryList = em.createQuery(sql).getResultList(); 
 
        for (DataobjectTypeCategory childDataobjectTypeCategory:dataobjectTypeCategoryList)
        {
            childCategoryList.add(childDataobjectTypeCategory.getId());
            getChildCategoryIds(em,childCategoryList,childDataobjectTypeCategory.getId());
        }
    }
    
    public static Map<String,String> getDataobjectTypeDataInfo(EntityManager platformEm, EntityManager em, int organizationId, int repositoryId, int dataobjectTypeId, boolean useRealtimeData) 
    {
        long count;
        Map<String,String> dataInfo = new HashMap<>();
 
        DataobjectType dataobjectType = platformEm.find(DataobjectType.class, dataobjectTypeId);
        
        if ( useRealtimeData )
        {
            count = Util.getDataobjectTypeDatasourceCount(em,platformEm,organizationId,dataobjectType.getId());
            dataInfo.put("totalItemsInDatasource", String.valueOf(count));
            
            String datasoureTypeName = Util.getDataobjectTypeDatasourceTypeName(em,platformEm,organizationId,dataobjectType.getId());
            dataInfo.put("datasourceTypeName",datasoureTypeName);
            
            count = Util.getDataobjectTypeCount(organizationId,em,platformEm,repositoryId,dataobjectType.getIndexTypeName(),"");
            dataInfo.put("totalItemsInDataLakeES", String.valueOf(count));
            
            count = Util.getDataobjectTypeDatawareHouseCount(em,platformEm,organizationId,repositoryId,dataobjectType.getId(),dataobjectType.getName().trim());
            dataInfo.put("totalItemsInSqlDatawareHouse", String.valueOf(count));
            
            String sqlDatawareHouseTypeName = Util.getDataobjectTypeDatawareHouseTypeName(em,platformEm,organizationId,repositoryId,dataobjectType.getName().trim());
            dataInfo.put("sqlDatawareHouseTypeName",sqlDatawareHouseTypeName);
            
            //dataInfo.put("needToSaveToDatawareHouse",String.valueOf(dataobjectType.getNeedToSaveToDatawareHouse())); log.info("needToSaveToDatawareHouse="+dataobjectType.getNeedToSaveToDatawareHouse());
            
            //dataInfo.put("dataUpdateInfo",map.get("dataUpdateInfo")==null?"":(String)map.get("dataUpdateInfo"));
        }
        else
        {
            Map<String,Object> map = Util.getDataobjectTypeESInfo(organizationId,em,platformEm,repositoryId,dataobjectType.getIndexTypeName());
            dataInfo.put("totalItemsInDataLakeES", map.get("count")==null?"0":String.valueOf((long)map.get("count")));
            dataInfo.put("totalItemsInSqlDatawareHouse", "0");
            dataInfo.put("dataUpdateInfo",map.get("dataUpdateInfo")==null?"":(String)map.get("dataUpdateInfo"));
            
            map = Util.getDataobjectTypeDWInfo(organizationId,em,platformEm,repositoryId,dataobjectType.getId(),dataobjectType.getName());
            dataInfo.put("totalItemsInSqlDatawareHouse", map.get("count")==null?"0":String.valueOf((long)map.get("count")));
            
             map = Util.getDataobjectTypeSourceInfo(organizationId,em,platformEm,repositoryId,dataobjectType.getId(),dataobjectType.getName());
            dataInfo.put("totalItemsInDatasource", map.get("count")==null?"0":String.valueOf((long)map.get("count")));
        }
        
        return dataInfo;
    }
}
