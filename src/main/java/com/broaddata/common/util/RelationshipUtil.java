/*
 * RelationShipUtil.java
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.UUID;
import javax.persistence.EntityManager;
import org.neo4j.graphdb.Direction;
 
import com.broaddata.common.model.enumeration.RelationshipDiscoveryType;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.model.platform.DataobjectTypeAssociation;
import com.broaddata.common.model.platform.RelationshipDiscoveryRule;
import com.broaddata.common.model.vo.DataobjectVO;
import com.broaddata.common.model.vo.Entity;
import com.broaddata.common.model.vo.EntityRelationship;

public class RelationshipUtil 
{      
    static final Logger log=Logger.getLogger("RelationshipUtil");     
     
    private List<Map<String,Object>> discoveryRelationshipsBetweenTwo(EntityManager em,EntityManager platformEm,int organizationId,int dataobjectTypeId,DataobjectVO objectA,DataobjectVO objectB) throws Exception 
    {
        Map<String,Object> relationship;
        RelationshipDiscoveryType relationshipType;
        List<RelationshipDiscoveryRule> relationshipDefinitionList;
        boolean foundRelationship;
        String connectingValues = "";
        DataobjectType dataobjectType;
        DataobjectTypeAssociation dta;
        List<Map<String,Object>> entityRelationshipList = new ArrayList<>();
 
        dataobjectType = platformEm.find(DataobjectType.class, dataobjectTypeId);
        
        //DataobjectVO newObjectA = getDataobjectDetails(organizationId, objectA.getDataobject().getId(),objectA.getDataobject().getCurrentVersion(),objectA.getDataobject().getDataobjectType(),objectA.getDataobject().getRepositoryId() );
        //DataobjectVO newObjectB = getDataobjectDetails(organizationId, objectB.getDataobject().getId(),objectA.getDataobject().getCurrentVersion(),objectA.getDataobject().getDataobjectType(),objectA.getDataobject().getRepositoryId() );

        String sql = String.format("from RelationshipDefinition rd where rd.organizationId=%d and rd.dataobjectTypeId=%d",organizationId,dataobjectTypeId);
            
        relationshipDefinitionList =  platformEm.createQuery(sql).getResultList();

        for (RelationshipDiscoveryRule relationshipDefinition : relationshipDefinitionList)
        {
            foundRelationship = true;
            
            relationshipType = RelationshipDiscoveryType.findByValue(relationshipDefinition.getDiscoveryType());
        
            switch(relationshipType)
            {
                case SAME_ENITY_TYPE_SAME_ATTRIBUTE_SAME_VALUE:
                    
                    connectingValues = "";
                    String[] sameAttributeColumns = relationshipDefinition.getSameAttributeColumns().split("\\,");
  
                    for(String sameAttributeColumn : sameAttributeColumns)
                    {
                        String dataobjectTypeName = sameAttributeColumn.substring(0,sameAttributeColumn.indexOf(":"));
                        String columnName = sameAttributeColumn.substring(sameAttributeColumn.indexOf(":")+1);
                        columnName = columnName.substring(0,columnName.indexOf("-")).trim();
                        
                        if ( dataobjectTypeName.equals(dataobjectType.getName()) )
                        {
                            if ( !objectA.getMetadatas().get(columnName).equals(objectB.getMetadatas().get(columnName)) )
                            {
                                foundRelationship = false;
                                break;
                            }
                                                    
                            connectingValues += String.format("%s ",objectA.getMetadatas().get(columnName));
                        }
                        else
                        {
                            DataobjectType slaveDataobjectType = Util.getDataobjectTypeByName(platformEm,dataobjectTypeName);
                            
                           /* List<DataobjectVO> dataobjectVOsA = getDataobjectAssociatedData(objectA,slaveDataobjectType.getId(),AssociationTypeProperty.PROFILE_ASSOCIATION);
                            if ( dataobjectVOsA == null || dataobjectVOsA.size()<1 )
                            {
                                foundRelationship = false;
                                break;
                            }
                            
                            List<DataobjectVO> dataobjectVOsB = getDataobjectAssociatedData(objectB,slaveDataobjectType.getId(),AssociationTypeProperty.PROFILE_ASSOCIATION);
                   
                            if ( dataobjectVOsB == null || dataobjectVOsB.size()<1 )
                            {
                                foundRelationship = false;
                                break;
                            }
                            
                            if ( !dataobjectVOsA.get(0).getThisDataobjectTypeMetadatas().get(columnName).equals(dataobjectVOsB.get(0).getThisDataobjectTypeMetadatas().get(columnName)) )
                            {
                                foundRelationship = false;
                                break;
                            }                      
                            
                            connectingValues += String.format("%s ",dataobjectVOsA.get(0).getThisDataobjectTypeMetadatas().get(columnName));*/
                        }
                    }
                    
                    break;
                           
                case SAME_ENITY_TYPE_SAME_ACTION:
                    
                    foundRelationship = false;
                    connectingValues = "";
                    
                    dta = platformEm.find(DataobjectTypeAssociation.class, relationshipDefinition.getDataobjectTypeAssociationId());
                    
                    // get object A all behavior
                  /*  List<DataobjectVO> dataobjectVOsA = getDataobjectAssociatedData(objectA,dta);
                    
                    // get object B all behavior
                    List<DataobjectVO> dataobjectVOsB = getDataobjectAssociatedData(objectB,dta);
                     
                    String[] sameEventColumns = dta.getSameEventColumns().split("\\,");
                    
                    // compare and get result           
                    for(DataobjectVO objA : dataobjectVOsA)
                    {
                        String objAValue="";
                        for(String key : sameEventColumns)
                        {
                            String value = objA.getThisDataobjectTypeMetadatas().get(key);
                            
                            if ( value.contains(".000Z") )
                                value = value.substring(0,value.indexOf(".000Z"));
                            
                            objAValue += value + " ";
                        }
                            
                        for(DataobjectVO objB : dataobjectVOsB)
                        {
                            String objBValue="";
                            for(String key : sameEventColumns)
                            {
                                String value = objB.getThisDataobjectTypeMetadatas().get(key);
                            
                                if ( value.contains(".000Z") )
                                    value = value.substring(0,value.indexOf(".000Z"));
                            
                                objBValue += value + " ";
                            }
                            
                            if ( objAValue.equals(objBValue) ) // found
                            {
                                foundRelationship = true;
                                connectingValues += String.format("%s ", objAValue);
                                break;
                            }
                        }
                    }*/

                    break;
            }
            
            if ( foundRelationship )
            {
                relationship = new HashMap<>();
                relationship.put("id", UUID.randomUUID().toString());
                relationship.put("firstEntityName", objectA.getDataobject().getName());
                relationship.put("secondEntityName", objectB.getDataobject().getName());
                relationship.put("name", relationshipDefinition.getName());
                relationship.put("connectingValues",connectingValues);
                entityRelationshipList.add(relationship);
            }
        }
        
        return entityRelationshipList;
    }
   
    /*
    public Map<String,Object> getDataobjectSameEventEntityData(DataobjectVO masterDataobjectVO,DataobjectVO slaveDataobjectVO)throws Exception
    {       
        Map<String,Object> sameEventEntityData = null;
        
        DataServiceConnector conn = null;
        DataService.Client dataService;
        SearchRequest request;
        SearchResponse response;    
        List<DataobjectVO> dataobjectList;
        Repository repository;
        DataobjectType dataobjectType;
        
        int organizationId;
        int catalogId;
        String indexType;
        String dataobjectTypeName;
        String filterStr;
        String selectedColumns;
        
        DataobjectTypeAssociation dta;
        
        try
        {
            organizationId = masterDataobjectVO.getDataobject().getOrganizationId();
            
            EntityManager platformEm = platformManagerBean.getPlatformEntityManagerFactory().createEntityManager();
            EntityManager em = platformManagerBean.getEntityManagerFactory(organizationId).createEntityManager();
            
            String dataServiceIp = Util.getDataServiceIP(platformEm, organizationId);
            
            conn = new DataServiceConnector();
            dataService = conn.getClient(dataServiceIp);

            repository = em.find(Repository.class, masterDataobjectVO.getDataobject().getRepositoryId());
            catalogId = repository.getDefaultCatalogId();
                        
            String sql = String.format("from DataobjectTypeAssociation dta where dta.organizationId =%d and dta.masterDataobjectTypeId=%d and dta.slaveDataobjectTypeId=%d",organizationId,masterDataobjectVO.getDataobject().getDataobjectType(),slaveDataobjectVO.getDataobject().getDataobjectType());
            dta = (DataobjectTypeAssociation)platformEm.createQuery(sql).getSingleResult();
 
            dataobjectType = platformEm.find(DataobjectType.class, dta.getSlaveDataobjectTypeId());
            dataobjectTypeName = dataobjectType.getDescription();
            indexType = dataobjectType.getIndexTypeName();

            sameEventEntityData = new HashMap<>();
            sameEventEntityData.put("title",dataobjectTypeName);

            selectedColumns = dta.getSlaveSelectedColumns();              

            //filterStr = Util.getSlaveFilterStr(dataobjectVO,dta);
            filterStr = Util.getSameEventFilterStr(slaveDataobjectVO,dta);

            request = new SearchRequest(organizationId,repository.getId(),catalogId,SearchType.SEARCH_WITHOUT_AGGREGATION.getValue());

            request.setTargetIndexKeys(null);
            request.setTargetIndexTypes(Arrays.asList(indexType));
            request.setQueryStr(null);
            request.setFilterStr(filterStr);
            request.setSearchFrom(0);
            request.setMaxExpectedHits(CommonKeys.MAX_EXPECTED_SEARCH_HITS);
            request.setMaxExecutionTime(CommonKeys.MAX_EXPECTED_EXCUTION_TIME);
            request.setColumns(selectedColumns);  // all columns
            request.setSortColumns(dta.getSlaveTimeEventColumn()+",ASCEND");  // order by time

            response = dataService.getDataset(request);
            dataobjectList = Util.generateDataobjectsForDataset(response.getSearchResults(),false);                

            sameEventEntityData.put("dataobjectList", dataobjectList);
            sameEventEntityData.put("dataobjectTypeAssociation", dta);

            selectedColumns = getDataobjectTypeColumns(dta.getSlaveDataobjectTypeId(),selectedColumns,AssociationTypeProperty.EVENT_ASSOCIATION,dta);
            sameEventEntityData.put("displayColumns", selectedColumns);
        }
        catch (Exception e)
        {
            throw e;
        }
        finally
        {
            if ( conn != null )
               conn.close();
        }

        return sameEventEntityData;
    }
    
    
        public List<Map<String, Object>> getDataobjectAssociatedData(DataobjectVO dataobjectVO,AssociationTypeProperty associationType) throws AppException, Exception 
    {       
        List<Map<String, Object>> associationDataList;
        Map<String,Object> associationData;
        
        DataServiceConnector conn = null;
        DataService.Client dataService;
        SearchRequest request;
        SearchResponse response;
        List<DataobjectVO> dataobjectList;
        Repository repository;
        DataobjectType dataobjectType;
        
        int catalogId;
        String indexType;
        String dataobjectTypeName;
        String filterStr;
        String selectedColumns;
        
        List<DataobjectTypeAssociation> dtaList;
        
        log.info(" orgid="+dataobjectVO.getDataobject().getOrganizationId());
        
        try
        {
            EntityManager platformEm = platformManagerBean.getPlatformEntityManagerFactory().createEntityManager();
            EntityManager em = platformManagerBean.getEntityManagerFactory(dataobjectVO.getDataobject().getOrganizationId()).createEntityManager();
            
            String dataServiceIp = Util.getDataServiceIP(platformEm, dataobjectVO.getDataobject().getOrganizationId());
            
            conn = new DataServiceConnector();
            dataService = conn.getClient(dataServiceIp);

            repository = em.find(Repository.class, dataobjectVO.getDataobject().getRepositoryId());
            catalogId = repository.getDefaultCatalogId();
                        
            String sql = String.format("from DataobjectTypeAssociation dta where dta.associationType in (select t.id from DataobjectTypeAssociationType t where (t.organizationId=0 or t.organizationId =%d) and t.property=%d ) and dta.organizationId =%d and dta.masterDataobjectTypeId=%d",dataobjectVO.getDataobject().getOrganizationId(),associationType.getValue(),dataobjectVO.getDataobject().getOrganizationId(),dataobjectVO.getDataobject().getDataobjectType());
            dtaList = platformEm.createQuery(sql).getResultList();

            associationDataList = new ArrayList<>();
             
            for(DataobjectTypeAssociation dta : dtaList)
            {
                dataobjectType = platformEm.find(DataobjectType.class, dta.getSlaveDataobjectTypeId());
                dataobjectTypeName = dataobjectType.getDescription();
                indexType = dataobjectType.getIndexTypeName();
                    
                associationData = new HashMap<>();
                 
                associationData.put("title",dataobjectTypeName);
                
                selectedColumns = dta.getSlaveSelectedColumns();              

                filterStr = Util.getSlaveFilterStr(dataobjectVO,dta);

                request = new SearchRequest(dataobjectVO.getDataobject().getOrganizationId(),dataobjectVO.getDataobject().getRepositoryId(),catalogId,SearchType.SEARCH_WITHOUT_AGGREGATION.getValue());

                request.setTargetIndexKeys(null);
                request.setTargetIndexTypes(Arrays.asList(indexType));
                request.setQueryStr(null);
                request.setFilterStr(filterStr);
                request.setSearchFrom(0);
                request.setMaxExpectedHits(CommonKeys.MAX_EXPECTED_SEARCH_HITS);
                request.setMaxExecutionTime(CommonKeys.MAX_EXPECTED_SEARCH_HITS);
                request.setColumns(selectedColumns);  // all columns
                
                if ( associationType == AssociationTypeProperty.EVENT_ASSOCIATION )
                    request.setSortColumns(dta.getSlaveTimeEventColumn()+",ASCEND");  // order by time

                response = dataService.getDataset(request);
                dataobjectList = Util.generateDataobjectsForDataset(response.getSearchResults(),false);                

                associationData.put("dataobjectList", dataobjectList);
                associationData.put("dataobjectTypeAssociation", dta);
                
                selectedColumns = getDataobjectTypeColumns(dta.getSlaveDataobjectTypeId(),selectedColumns,associationType,dta);
                associationData.put("displayColumns", selectedColumns);
                
                associationDataList.add(associationData);
                associationData.put("id", associationDataList.indexOf(associationData));
            }
        }
        catch (Exception e)
        {
            throw e;
        }
        finally
        {
            if ( conn != null )
               conn.close();
        }

        return associationDataList;
    }
       
    public  List<DataobjectVO> getDataobjectAssociatedData(DataobjectVO dataobjectVO,int slaveDataobjectTypeId,AssociationTypeProperty associationType) throws AppException, Exception 
    {       
        DataServiceConnector conn = null;
        DataService.Client dataService;
        SearchRequest request;
        SearchResponse response;    
        List<DataobjectVO> dataobjectList = null;
        Repository repository;
        DataobjectType dataobjectType;
        
        int catalogId;
        String indexType;
        String dataobjectTypeName;
        String filterStr;
        String selectedColumns;
        
        DataobjectTypeAssociation dta;
        
        log.info(" orgid="+dataobjectVO.getDataobject().getOrganizationId());
        
        try
        {
            EntityManager platformEm = platformManagerBean.getPlatformEntityManagerFactory().createEntityManager();
            EntityManager em = platformManagerBean.getEntityManagerFactory(dataobjectVO.getDataobject().getOrganizationId()).createEntityManager();
            
            String dataServiceIp = Util.getDataServiceIP(platformEm, dataobjectVO.getDataobject().getOrganizationId());
            
            conn = new DataServiceConnector();
            dataService = conn.getClient(dataServiceIp);

            repository = em.find(Repository.class, dataobjectVO.getDataobject().getRepositoryId());
            catalogId = repository.getDefaultCatalogId();
                        
            String sql = String.format("from DataobjectTypeAssociation dta where dta.associationType in (select t.id from DataobjectTypeAssociationType t where (t.organizationId=0 or t.organizationId =%d) and t.property=%d ) and dta.organizationId =%d and dta.masterDataobjectTypeId=%d and dta.slaveDataobjectTypeId=%d",dataobjectVO.getDataobject().getOrganizationId(),associationType.getValue(),dataobjectVO.getDataobject().getOrganizationId(),dataobjectVO.getDataobject().getDataobjectType(),slaveDataobjectTypeId);
            dta = (DataobjectTypeAssociation)platformEm.createQuery(sql).getSingleResult();
           
            dataobjectType = platformEm.find(DataobjectType.class, dta.getSlaveDataobjectTypeId());
            
            dataobjectTypeName = dataobjectType.getDescription();
            indexType = dataobjectType.getIndexTypeName();
            selectedColumns = dta.getSlaveSelectedColumns();              

            filterStr = Util.getSlaveFilterStr(dataobjectVO,dta);

            request = new SearchRequest(dataobjectVO.getDataobject().getOrganizationId(),dataobjectVO.getDataobject().getRepositoryId(),catalogId,SearchType.SEARCH_WITHOUT_AGGREGATION.getValue());

            request.setTargetIndexKeys(null);
            request.setTargetIndexTypes(Arrays.asList(indexType));
            request.setQueryStr(null);
            request.setFilterStr(filterStr);
            request.setSearchFrom(0);
            request.setMaxExpectedHits(CommonKeys.MAX_EXPECTED_SEARCH_HITS);
            request.setMaxExecutionTime(CommonKeys.MAX_EXPECTED_SEARCH_HITS);
            request.setColumns(selectedColumns);  // all columns

            if ( associationType == AssociationTypeProperty.EVENT_ASSOCIATION )
                request.setSortColumns(dta.getSlaveTimeEventColumn()+",ASCEND");  // order by time

            response = dataService.getDataset(request);
            dataobjectList = Util.generateDataobjectsForDataset(response.getSearchResults(),false);                
        }
        catch (Exception e)
        {
            throw e;
        }
        finally
        {
            if ( conn != null )
               conn.close();
        }

        return dataobjectList;
    }
   
    public  List<DataobjectVO> getDataobjectAssociatedData(DataobjectVO dataobjectVO,DataobjectTypeAssociation dta) throws AppException, Exception 
    {       
        DataServiceConnector conn = null;
        DataService.Client dataService;
        SearchRequest request;
        SearchResponse response;    
        List<DataobjectVO> dataobjectList = null;
        Repository repository;
        DataobjectType dataobjectType;
        
        int catalogId;
        String indexType;
        String dataobjectTypeName;
        String filterStr;
        String selectedColumns;
        AssociationTypeProperty associationType;
        
        log.info(" orgid="+dataobjectVO.getDataobject().getOrganizationId());
        
        try
        {
            EntityManager platformEm = platformManagerBean.getPlatformEntityManagerFactory().createEntityManager();
            EntityManager em = platformManagerBean.getEntityManagerFactory(dataobjectVO.getDataobject().getOrganizationId()).createEntityManager();
            
            String dataServiceIp = Util.getDataServiceIP(platformEm, dataobjectVO.getDataobject().getOrganizationId());
            
            conn = new DataServiceConnector();
            dataService = conn.getClient(dataServiceIp);

            repository = em.find(Repository.class, dataobjectVO.getDataobject().getRepositoryId());
            catalogId = repository.getDefaultCatalogId();
                        
            dataobjectType = platformEm.find(DataobjectType.class, dta.getSlaveDataobjectTypeId());
            
            dataobjectTypeName = dataobjectType.getDescription();
            indexType = dataobjectType.getIndexTypeName();
            selectedColumns = dta.getSlaveSelectedColumns();

            filterStr = Util.getSlaveFilterStr(dataobjectVO,dta);

            request = new SearchRequest(dataobjectVO.getDataobject().getOrganizationId(),dataobjectVO.getDataobject().getRepositoryId(),catalogId,SearchType.SEARCH_WITHOUT_AGGREGATION.getValue());

            request.setTargetIndexKeys(null);
            request.setTargetIndexTypes(Arrays.asList(indexType));
            request.setQueryStr(null);
            request.setFilterStr(filterStr);
            request.setSearchFrom(0);
            request.setMaxExpectedHits(CommonKeys.MAX_EXPECTED_SEARCH_HITS);
            request.setMaxExecutionTime(CommonKeys.MAX_EXPECTED_SEARCH_HITS);
            request.setColumns(selectedColumns);  // all columns

            associationType = AssociationTypeProperty.findByValue(dta.getAssociationType());
            
            if ( associationType == AssociationTypeProperty.EVENT_ASSOCIATION )
                request.setSortColumns(dta.getSlaveTimeEventColumn()+",ASCEND");  // order by time

            response = dataService.getDataset(request);
            dataobjectList = Util.generateDataobjectsForDataset(response.getSearchResults(),false);                
        }
        catch (Exception e)
        {
            throw e;
        }
        finally
        {
            if ( conn != null )
               conn.close();
        }

        return dataobjectList;
    }
    */
    public static Entity createEntity(String type,String id, String name, Map<String,Object> properties)
    {
        Entity entity = new Entity(type, id, name,properties);
        
        entity = createEntity(entity);
        
        return entity;
    }
    
    public static Entity createEntity(Entity entity)
    {
        return entity;
    } 
    
    public static Entity getEntity(String type, String id)
    {
        Entity entity = new Entity();
         
        return entity;
    }    
 
    public static Entity getEntityRelationships(Entity entity, String relationshipType, Direction direction, int relationshipDeep)
    {
      
        return null;
    }        
    
    public static void addRelationship(Entity fromEntity, Entity toEntity, EntityRelationship relationship) 
    {
   
    }
    
    public static Entity getRelationShipsBetweenTwoEntity(Entity fromEntity, Entity toEntity,EntityRelationship relationship)
    {
        return null;
    }
}
