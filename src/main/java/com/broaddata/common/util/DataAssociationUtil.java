/*
 * DataAssociationUtil.java
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;

import com.broaddata.common.model.enumeration.AssociationTypeProperty;
import com.broaddata.common.model.enumeration.SearchType;
import com.broaddata.common.model.organization.Repository;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.model.platform.DataobjectTypeAssociation;
import com.broaddata.common.model.platform.DataobjectTypeAssociationType;
import com.broaddata.common.model.vo.DataobjectAssociatedData;
import com.broaddata.common.model.vo.DataobjectVO;
import com.broaddata.common.thrift.dataservice.SearchRequest;
import com.broaddata.common.thrift.dataservice.SearchResponse;

public class DataAssociationUtil 
{      
    static final Logger log=Logger.getLogger("DataAssociationUtil");      
   
    public static String getSlaveFieldName(EntityManager platformEm,int organizationId,int masterDataobjectTypeId,int slaveDataobjectTypeId,String masterFieldName)
    {
        String sql = String.format("from DataobjectTypeAssociation where organizationId =%d and masterDataobjectTypeId=%d and slaveDataobjectTypeId=%d",organizationId,masterDataobjectTypeId,slaveDataobjectTypeId);
     
        List<DataobjectTypeAssociation> dtaList = platformEm.createQuery(sql).getResultList();
        
        for(DataobjectTypeAssociation dta : dtaList)
        {
            String[] vals = dta.getKeyMapping().split("\\;");
            
            for(String str : vals)
            {
                String[] vals1 = str.split("\\,");
                
                if ( vals1[0].toUpperCase().equals(masterFieldName.toUpperCase()) )
                    return vals1[1];
            }
        }
        
        return null;
    }
    
    public static Map<String,String> getSlaveFieldNames(EntityManager platformEm,int organizationId,int masterDataobjectTypeId,int slaveDataobjectTypeId)
    {
        Map<String,String> map = new HashMap<>();
        
        String sql = String.format("from DataobjectTypeAssociation where organizationId =%d and masterDataobjectTypeId=%d and slaveDataobjectTypeId=%d",organizationId,masterDataobjectTypeId,slaveDataobjectTypeId);
     
        List<DataobjectTypeAssociation> dtaList = platformEm.createQuery(sql).getResultList();
        
        for(DataobjectTypeAssociation dta : dtaList)
        {
            String[] vals = dta.getKeyMapping().split("\\;");
            
            for(String str : vals)
            {
                String[] vals1 = str.split("\\,");                
                map.put(vals1[0],vals1[1]);
            }
        }
        
        return map;
    }
    
    public static DataobjectAssociatedData getSingleLevelAssociatedData(EntityManager em,EntityManager platformEm,int organizationId, int repositoryId, int associationTypeProperty, String associationTypes, int masterDataobjectTypeId,List<Integer> slaveDataobjectTypeIds, DataobjectVO dataobjectVO,String eventStartTime, String eventEndTime) throws Exception 
    {
        String sql;
        List<DataobjectTypeAssociation> dtaList;
        DataobjectAssociatedData dataobjectAssociatedData;
        List<Map<String, Object>> associatedDataList;
        String dataobjectTypeName;
        String indexType;
        Map<String, Object> associationData;
        String selectedColumns;
        String filterStr;
        SearchRequest request;
        SearchResponse response;
        List<DataobjectVO> dataobjectList;
      
        Repository repository = em.find(Repository.class, repositoryId);
        int catalogId = repository.getDefaultCatalogId();
        
        if ( associationTypeProperty == AssociationTypeProperty.ALL_ASSOCIATION.getValue() )
        {
            if ( associationTypes == null || associationTypes.trim().isEmpty() )
                sql = String.format("from DataobjectTypeAssociation dta where dta.organizationId =%d and dta.masterDataobjectTypeId=%d",organizationId,masterDataobjectTypeId);
            else
                sql = String.format("from DataobjectTypeAssociation dta where dta.associationType in ( %s ) and dta.organizationId =%d and dta.masterDataobjectTypeId=%d", associationTypes,organizationId,masterDataobjectTypeId);
        }
        else
        {
            if ( associationTypes == null || associationTypes.trim().isEmpty() )
                sql = String.format("from DataobjectTypeAssociation dta where dta.associationType in (select t.id from DataobjectTypeAssociationType t where (t.organizationId=0 or t.organizationId =%d) and t.property=%d ) and dta.organizationId =%d and dta.masterDataobjectTypeId=%d",organizationId,associationTypeProperty,organizationId,masterDataobjectTypeId);
            else
                sql = String.format("from DataobjectTypeAssociation dta where (dta.associationType in ( %s ) and dta.associationType in (select t.id from DataobjectTypeAssociationType t where (t.organizationId=0 or t.organizationId =%d) and t.property=%d )) and dta.organizationId =%d and dta.masterDataobjectTypeId=%d",associationTypes, organizationId,associationTypeProperty,organizationId,masterDataobjectTypeId);
        }
        
        dtaList = platformEm.createQuery(sql).getResultList();
        dataobjectAssociatedData = new DataobjectAssociatedData();
        dataobjectAssociatedData.setAssociationTypeProperty(associationTypeProperty);
        dataobjectAssociatedData.setEventStartTime(null);
        dataobjectAssociatedData.setEventEndTime(null);
        
        associatedDataList = new ArrayList<>();
        dataobjectAssociatedData.setAssociatedDataList(associatedDataList);
        // map key: dataobjectTypeName, dataobjectVOList, dataobjectTypeAssociation, columns
        
        for(DataobjectTypeAssociation dta : dtaList)
        {
            if ( slaveDataobjectTypeIds != null && !slaveDataobjectTypeIds.isEmpty() )
            {
                boolean found = false;
                
                for(Integer selectedSlaveDataobjectTypeId : slaveDataobjectTypeIds )
                {
                    if ( selectedSlaveDataobjectTypeId == dta.getSlaveDataobjectTypeId() )
                    {
                        found = true;
                        break;
                    }
                }
                
                if ( !found )
                    continue;
            }
                    
            DataobjectType dataobjectType = platformEm.find(DataobjectType.class, dta.getSlaveDataobjectTypeId());
            DataobjectTypeAssociationType associationType = platformEm.find(DataobjectTypeAssociationType.class, dta.getAssociationType());
            AssociationTypeProperty atProperty = AssociationTypeProperty.findByValue(associationType.getProperty());
            
            dataobjectTypeName = dataobjectType.getDescription();
            indexType = dataobjectType.getIndexTypeName();
            
            associationData = new HashMap<>();
            
            //selectedColumns = dta.getSlaveSelectedColumns();
            
            filterStr = Util.getSlaveFilterStr(dataobjectVO,dta);
            
            request = new SearchRequest(organizationId,repositoryId,catalogId,SearchType.SEARCH_GET_ONE_PAGE.getValue());
            
            request.setTargetIndexKeys(null);
            request.setTargetIndexTypes(Arrays.asList(indexType));
            request.setFilterStr(filterStr);
            request.setSearchFrom(0);
            request.setMaxExpectedHits(CommonKeys.MAX_EXPECTED_SEARCH_HITS);
            request.setMaxExecutionTime(CommonKeys.MAX_EXPECTED_EXCUTION_TIME);
            //request.setColumns(selectedColumns);
            
            if ( atProperty == AssociationTypeProperty.EVENT_ASSOCIATION && eventStartTime != null && eventEndTime != null)
            {
                String timeQueryStr =String.format("%s:[%s TO %s]", dta.getSlaveTimeEventColumn(), eventStartTime, eventEndTime);
                request.setQueryStr(timeQueryStr);               
                request.setSortColumns(dta.getSlaveTimeEventColumn()+",ASCEND");  // order by time
            }
            
            response = SearchUtil.getDataobjects(em,platformEm,request);
            dataobjectList = Util.generateDataobjectsForDataset(response.getSearchResults(),false);
            
            if ( dataobjectList.size() > 0 )
            {
                selectedColumns = Util.getDataobjectTypeColumns(platformEm, dta.getSlaveDataobjectTypeId(), null, atProperty, dta);

                associationData.put("dataobjectTypeName",dataobjectTypeName);
                associationData.put("dataobjectTypeId",dta.getSlaveDataobjectTypeId());
                associationData.put("dataobjectVOList", dataobjectList);
                associationData.put("dataobjectTypeAssociation", dta);
                associationData.put("dataobjectTypeAssociationProperty", associationType.getProperty());
                associationData.put("selectedColumns", selectedColumns);

                associatedDataList.add(associationData);
                associationData.put("id", associatedDataList.indexOf(associationData));
            }
        }

        return dataobjectAssociatedData;
    }
}
