/*
 * DataobjectHelper.java
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList; 
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.File;
import java.util.TreeMap;
import org.json.JSONArray;
import org.json.JSONObject;
import javax.persistence.EntityManager;

import com.broaddata.common.model.organization.Content;
import com.broaddata.common.model.organization.Dataobject;
import com.broaddata.common.model.organization.DatasourceConnection;
import com.broaddata.common.model.organization.IndexSet;
import com.broaddata.common.model.organization.Repository;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.model.platform.ServiceInstance;
import com.broaddata.common.model.vo.DataobjectVO;
import com.broaddata.common.thrift.dataservice.DataServiceException;
import com.broaddata.common.model.enumeration.ComputingNodeOSType;
import com.broaddata.common.model.organization.Catalog;
import org.elasticsearch.client.Client;

public class DataobjectHelper 
{
    static final Logger log = Logger.getLogger("DataobjectHelper"); 
                  
    public static String getDataobjectContentId(int organizationId,int repositoryId,String dataobjectId)
    {
        String contentId = "";
        
        Client esClient = ESUtil.getClient(organizationId,repositoryId,false);

        String queryStr = String.format("_id:%s", dataobjectId);          

        Map<String,Object> ret = ESUtil.retrieveSimpleDataset(esClient, new String[]{}, new String[]{}, queryStr, null, 1, 0, new String[]{}, new String[]{},new String[]{},null);

        List<Map<String,Object>> searchResults = (List<Map<String,Object>>)ret.get("searchResults");

        for(Map<String,Object> result : searchResults)
        {
            String id = (String)result.get("id");
            id = id.substring(0, 40);
            Map<String,Object> fieldMap = (Map<String,Object>)result.get("fields");

            contentId = (String)fieldMap.get("contents.content_id");
            break;
        }

        return contentId;
    }
    
    public static DataobjectVO getDataobjectDetailsWithFullContents(EntityManager em, EntityManager platformEm, int organizationId, String dataobjectId, int dataobjectVersion, int dataobjectTypeId, int repositoryId) throws DataServiceException, Exception 
    {
        JSONObject jsonObject;
        DataobjectVO dataobjectVO;
        Dataobject dataobject;
        
        jsonObject = ESUtil.getDocumentWithFullContents(em,platformEm,organizationId,dataobjectId,dataobjectVersion,dataobjectTypeId,repositoryId);
        
        if ( jsonObject == null )
            throw new DataServiceException("dataservice.error.dataobject_not_existing");
        
        dataobjectVO = new DataobjectVO();
        dataobject = dataobjectVO.getDataobject();

        String dataobjectVOId = dataobjectId;
        dataobjectVO.setId(dataobjectVOId); // dataobject id + version
        dataobject.setId(dataobjectId);
        dataobject.setCurrentVersion(dataobjectVersion); // dataobject version
        dataobject.setName(jsonObject.optString("dataobject_name"));
        dataobject.setObjectSize(jsonObject.optLong("object_size"));
        dataobject.setOrganizationId(jsonObject.optInt("organization_id"));
        dataobject.setDataobjectType(jsonObject.optInt("dataobject_type"));
        dataobject.setRepositoryId(jsonObject.optInt("target_repository_id"));
        dataobject.setDataobjectType(dataobjectTypeId);
        dataobjectVO.setMetadatas(Tool.parseJson(jsonObject));
        DataobjectType dataobjectType = Util.getDataobjectType(platformEm,dataobjectTypeId);
        dataobjectVO.getMetadatas().put("dataobject_type_name", dataobjectType.getName()+"-"+dataobjectType.getDescription());
        dataobjectVO.getMetadatas().put("index_name",jsonObject.optString("index_name"));
        // exclude dataobject metadata
        
        List<Map<String,Object>> dataobjectTypeMetadataDefinitions = Util.getDataobjectTypeMetadataDefinition(platformEm,dataobjectTypeId,true,true);
        for(Map<String,Object> definition : dataobjectTypeMetadataDefinitions)
        {
            for(Map.Entry entry : dataobjectVO.getMetadatas().entrySet())
            {
                if ( ((String)entry.getKey()).equals((String)definition.get("name")) )
                {
                    String displayName = (String)definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                    if ( displayName == null || displayName.isEmpty() )
                        displayName = (String)entry.getKey();
                    
                    dataobjectVO.getThisDataobjectTypeMetadatas().put(displayName,(String)entry.getValue());
                    break;
                }
            }
        }
        // process contents
        JSONArray contentArray = jsonObject.optJSONArray("contents");
        if ( contentArray != null )
        {
            dataobjectVO.setHasContent(true);
            
            for(int i=0;i<contentArray.length();i++)
            {
                JSONObject contentObject = contentArray.optJSONObject(i);
                
                Map<String,Object> map = new HashMap<>();
                map.put("content_id", contentObject.optString("content_id"));
                map.put("content_name", contentObject.optString("content_name"));
                map.put("mime_type", contentObject.optString("mime_type"));
                map.put("encoding", contentObject.optString("encoding"));
                map.put("content_size",contentObject.optString("content_size"));
                map.put("content_text",contentObject.optString("content_text"));
                map.put("content_type",contentObject.optString("content_type"));
                
                JSONArray contentInfoArray = contentObject.optJSONArray("content_info");
                if ( contentInfoArray != null )
                {
                    Map<String,String> contentInfo = new TreeMap<>();
                    
                    for(int j=0;j<contentInfoArray.length();j++)
                    {
                        JSONObject data = contentInfoArray.optJSONObject(j);
                        
                        String key = data.optString("key");
                        String value = data.optString("value");
                        
                        contentInfo.put(key, value);
                    }
                    map.put("content_info", contentInfo);
                }
                
                JSONArray similarContentsArray = contentObject.optJSONArray("similar_contents");
                if ( similarContentsArray != null )
                {
                    List<Map<String,Object>> sList = new ArrayList<>();
                    
                    for(int j=0;j<similarContentsArray.length();j++)
                    {
                        JSONObject data = similarContentsArray.optJSONObject(j);
                        
                        String contentId = data.optString("content_id");
                        
                        Map<String,Object> sMap = new HashMap<>();
                        sMap.put("content_id", contentId);
                        sMap.put("score", data.optString("score"));
                        
                        List<Map<String,Object>> contentDataobjectInfoList = DataobjectHelper.getDataobjectInfoListByContentId(em,contentId);
                        
                        for(Map<String,Object> contentDataobjectInfo : contentDataobjectInfoList )
                        {
                            String similarContentDataobjectId = (String)contentDataobjectInfo.get("dataobject_id");
                            int version = (Integer)contentDataobjectInfo.get("version");
                            
                            String dataobjectName = DataobjectHelper.getDataobjectNameById(em,similarContentDataobjectId,version);
                            
                            contentDataobjectInfo.put("dataobject_name", dataobjectName);
                        }
                        
                        if ( contentDataobjectInfoList.get(0)!= null )
                            sMap.put("dataobject_name",contentDataobjectInfoList.get(0).get("dataobject_name"));
                        
                        sMap.put("content_dataobject_info", contentDataobjectInfoList);
                        
                        sList.add(sMap);
                    }
                    
                    Util.sortListAccordingToScore(sList);
                    
                    map.put("similar_contents", sList);
                }
                
                dataobjectVO.getContents().add(map);
            }
        }
        return dataobjectVO;
    }
    
    public static DataobjectVO getDataobjectDetails(EntityManager em, EntityManager platformEm, int organizationId, String dataobjectId, int dataobjectVersion, int dataobjectTypeId, int repositoryId) throws DataServiceException, Exception 
    {
        JSONObject jsonObject;
        DataobjectVO dataobjectVO;
        Dataobject dataobject;
        
        jsonObject = ESUtil.getDocument(em,platformEm,organizationId,dataobjectId,dataobjectVersion,dataobjectTypeId,repositoryId);
        
        if ( jsonObject == null )
            throw new DataServiceException("dataservice.error.dataobject_not_existing");
        
        dataobjectVO = new DataobjectVO();
        dataobject = dataobjectVO.getDataobject();

        String dataobjectVOId = dataobjectId;
        dataobjectVO.setId(dataobjectVOId); // dataobject id + version
        dataobject.setId(dataobjectId);
        dataobject.setCurrentVersion(dataobjectVersion); // dataobject version
        dataobject.setName(jsonObject.optString("dataobject_name"));
        dataobject.setObjectSize(jsonObject.optLong("object_size"));
        dataobject.setOrganizationId(jsonObject.optInt("organization_id"));
        dataobject.setDataobjectType(jsonObject.optInt("dataobject_type"));
        dataobject.setRepositoryId(jsonObject.optInt("target_repository_id"));
        dataobject.setDataobjectType(dataobjectTypeId);
        dataobjectVO.setMetadatas(Tool.parseJson(jsonObject));
        DataobjectType dataobjectType = Util.getDataobjectType(platformEm,dataobjectTypeId);
        dataobjectVO.getMetadatas().put("dataobject_type_name", dataobjectType.getName()+"-"+dataobjectType.getDescription());
        // exclude dataobject metadata
        
        List<Map<String,Object>> dataobjectTypeMetadataDefinitions = Util.getDataobjectTypeMetadataDefinition(platformEm,dataobjectTypeId,true,true);
        for(Map<String,Object> definition : dataobjectTypeMetadataDefinitions)
        {
            for(Map.Entry entry : dataobjectVO.getMetadatas().entrySet())
            {
                if ( ((String)entry.getKey()).equals((String)definition.get("name")) )
                {
                    String displayName = (String)definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                    if ( displayName == null || displayName.isEmpty() )
                        displayName = (String)entry.getKey();
                    
                    dataobjectVO.getThisDataobjectTypeMetadatas().put(displayName,(String)entry.getValue());
                    break;
                }
            }
        }
   
        return dataobjectVO;
    }
    
    public static String getDataobjectNameById(EntityManager em, String dataobjectId, int version)
    {
        Dataobject dataobject;
        
        try {
            dataobject = em.find(Dataobject.class, dataobjectId);
            return dataobject.getName();
        } 
        catch (Exception e) 
        {
            return "";
        }    
    }
    
    public static String getDataobjectIndexTypeName(EntityManager em, int dataobjectTypeId)
    {
        DataobjectType dataobjectType = em.find(DataobjectType.class, dataobjectTypeId);
        
        if ( dataobjectType != null)
            return dataobjectType.getIndexTypeName();
        else
            return null;
    }
        
    public static List<Map<String,Object>> getDataobjectInfoListByContentId(EntityManager em,String contentId)
    {
        List<Map<String,Object>> contentDataobjectInfoList = new ArrayList<>();
        
        try
        {
            String sql = String.format("select dc.dataobjectContentPK.dataobjectId,dc.dataobjectContentPK.version from DataobjectContent dc where dc.contentId='%s'",contentId);
            List<Object[]> objsList = em.createQuery(sql).getResultList();
            
            for(Object[] objs : objsList)
            {
                String dataobjectId = (String)objs[0];
                int version = (Integer)objs[1];
                
                Map<String,Object> map = new HashMap<>();
                map.put("dataobject_id", dataobjectId);
                map.put("version", version);
                
                contentDataobjectInfoList.add(map);
            }
        } 
        catch (Exception e) 
        {
            log.error(" getDataobjectInfoListByContentId failed! e="+e.getMessage() + " contentId="+contentId);
        } 
        
        return contentDataobjectInfoList;
    }
    
   /* public static List<String> getDataobjectIndexName(EntityManager em, String dataobjectId)
    {
        String sql = String.format("select i.name from IndexSet i where i.id in (select id.indexDataobjectPK.indexId from IndexDataobject id where id.indexDataobjectPK.dataobjectId='%s')",dataobjectId);
        List<String> indexNameList = em.createQuery(sql).getResultList();
        
        return indexNameList;
    }*/
           
    public static String getDataobjectIndexName(EntityManager em, Dataobject dataobject)
    {
        Repository repository = em.find(Repository.class, dataobject.getRepositoryId());
        Catalog catalog = em.find(Catalog.class, repository.getDefaultCatalogId());
        
        return Util.getIndexName(dataobject.getOrganizationId(),dataobject.getRepositoryId(),catalog.getPartitionMetadataName());
    }
        
    public static List<String> getDataobjectIndexNameList(EntityManager em, int repositoryId)
    {
        Repository repository = em.find(Repository.class, repositoryId);
        Catalog catalog = em.find(Catalog.class, repository.getDefaultCatalogId());
        
        String sql = String.format("select i.name from IndexSet i where i.catalogId=%d",catalog.getId());
        List<String> indexNameList = em.createQuery(sql).getResultList();
        
        return indexNameList;
    }
    
    public static List<IndexSet> getDataobjectIndexSetList(EntityManager em, int repositoryId)
    {
        Repository repository = em.find(Repository.class, repositoryId);
        Catalog catalog = em.find(Catalog.class, repository.getDefaultCatalogId());
       
        String sql = String.format("from IndexSet where catalogId=%d",catalog.getId());
                 
        if ( repository.getDataMartCatalogId() > 0 )
        {
            catalog = em.find(Catalog.class, repository.getDataMartCatalogId());
            sql += String.format(" or catalogId=%d",catalog.getId());
        }
                
        List<IndexSet> indexSetList = em.createQuery(sql).getResultList();
                    
        //log.info(" sql="+sql+" size="+indexSetList.size());
        
        return indexSetList;
    }
    
    public static Dataobject getDataobject(EntityManager em, String dataobjectId) 
    {
        Dataobject  dataobject = null;
        
        try {
            dataobject  =  em.find(Dataobject.class, dataobjectId);
        } 
        catch (Exception e) 
        {
            e.printStackTrace();
            return null;
        }
         
        return dataobject;
    }
    
    public static DataobjectVO getDataobjectVO(EntityManager em, EntityManager platformEm, Dataobject dataobject, int dataobjectVersion) throws Exception 
    {
        String contentId;
        Map<String,ByteBuffer> contentBinaryStreams = new HashMap<>();
        
        DataobjectVO vo = new DataobjectVO();
        
        vo.setDataobject(dataobject);
        
        // get content binary
        contentId = getContentId(em,dataobject.getId(),dataobject.getCurrentVersion(),"main_content");
        contentBinaryStreams.put("main_content", getDataobjectContentBinaryByContentName(dataobject.getOrganizationId(),em,platformEm,dataobject.getId(), dataobject.getCurrentVersion(),"main_content"));
        vo.setContentBinaryStreams(contentBinaryStreams);

        return vo;
    }
        
    public static String getDataobjectContentFileUNCPath(int organizationId,EntityManager em, EntityManager platformEm, String dataobjectId, int dataobjectVersion, String contentName,String workingDirectory) throws Exception
    {
        String contentId = getContentId(em,dataobjectId,dataobjectVersion,contentName);
        
        Content content = em.find(Content.class, contentId);
        
        if (content.getLocation().startsWith(CommonKeys.CONTENT_LOCATION_FILE_SERVER) )
        {
            if ( RuntimeContext.computingNode.getNodeOsType() == ComputingNodeOSType.Windows.getValue() )
                return content.getLocation().substring(CommonKeys.CONTENT_LOCATION_FILE_SERVER.length()+1);            
            else
            {
                String fileLink = content.getLocation().substring(CommonKeys.CONTENT_LOCATION_FILE_SERVER.length()+1);
                String accessFileLink = getNonWindowsAccessFileLink(fileLink,organizationId,em);
                return accessFileLink;
            }
        }
        else
        if (content.getLocation().startsWith(CommonKeys.CONTENT_LOCATION_BLOB_STORE))
        {
            ServiceInstance objectStorageServiceInstance = platformEm.find(ServiceInstance.class, content.getObjectStorageServiceInstanceId());
            String objectStoreServiceProvider = objectStorageServiceInstance.getServiceProvider();
            Map<String,String> objectStoreServiceProviderProperties = Util.getServiceInstancePropertyConfigMap(objectStorageServiceInstance.getConfig());
        
            ByteBuffer contentStream = ContentStoreUtil.retrieveContent(organizationId,em,contentId,objectStorageServiceInstance,objectStoreServiceProvider,objectStoreServiceProviderProperties); 
            String filename = String.format("%s/%s",workingDirectory,contentId);
            FileUtil.writeFileFromByteBuffer(filename,contentStream);
            
            return filename;
        }
        else
            return null;
    }
    
    public static ByteBuffer getDataobjectContentBinaryByContentName(int organizationId,EntityManager em, EntityManager platformEm,String dataobjectId, int dataobjectVersion, String contentName) throws Exception
    {
        String contentId = getContentId(em,dataobjectId,dataobjectVersion,contentName);
        Content content = em.find(Content.class, contentId);
        
        ServiceInstance objectStorageServiceInstance = platformEm.find(ServiceInstance.class, content.getObjectStorageServiceInstanceId());
        String objectStoreServiceProvider = objectStorageServiceInstance.getServiceProvider();
        Map<String,String> objectStoreServiceProviderProperties = Util.getServiceInstancePropertyConfigMap(objectStorageServiceInstance.getConfig());
                    
        if (content.getLocation().startsWith(CommonKeys.CONTENT_LOCATION_BLOB_STORE))
            return ContentStoreUtil.retrieveContent(organizationId,em,contentId,objectStorageServiceInstance,objectStoreServiceProvider,objectStoreServiceProviderProperties); 
        else
        if (content.getLocation().startsWith(CommonKeys.CONTENT_LOCATION_FILE_SERVER) )
        {
            if ( RuntimeContext.computingNode.getNodeOsType() == ComputingNodeOSType.Windows.getValue() )
                return FileUtil.readFileToByteBuffer(new File(content.getLocation().substring(CommonKeys.CONTENT_LOCATION_FILE_SERVER.length()+1)));
            else // read from linux
            {
                String fileLink = content.getLocation().substring(CommonKeys.CONTENT_LOCATION_FILE_SERVER.length()+1);
                String accessFileLink = getNonWindowsAccessFileLink(fileLink,organizationId,em);
                log.info("fileLink ="+fileLink+ " accessFileLink="+accessFileLink);
                return FileUtil.readFileToByteBuffer(new File(accessFileLink)); 
            }
        }
        else
        if (content.getLocation().startsWith(CommonKeys.CONTENT_LOCATION_DATABASE) )
            return Util.getContentFromDatabase(em, platformEm,dataobjectId, dataobjectVersion,content.getLocation());
        else
            throw new PlatformException("platformException.error.0001");  // content not in system
    }
    
    public static ByteBuffer getDataobjectContentBinaryByContentId(int organizationId,EntityManager em, EntityManager platformEm,String contentId) throws Exception
    {
        Content content = em.find(Content.class, contentId);
        ServiceInstance objectStorageServiceInstance = platformEm.find(ServiceInstance.class, content.getObjectStorageServiceInstanceId());
        String objectStoreServiceProvider = objectStorageServiceInstance.getServiceProvider();
        Map<String,String> objectStoreServiceProviderProperties = Util.getServiceInstancePropertyConfigMap(objectStorageServiceInstance.getConfig());
            
        if (content.getLocation().startsWith(CommonKeys.CONTENT_LOCATION_BLOB_STORE))
            return ContentStoreUtil.retrieveContent(organizationId,em,contentId,objectStorageServiceInstance,objectStoreServiceProvider,objectStoreServiceProviderProperties); 
        else
        if (content.getLocation().startsWith(CommonKeys.CONTENT_LOCATION_FILE_SERVER) )
        {
            if ( RuntimeContext.computingNode.getNodeOsType() == ComputingNodeOSType.Windows.getValue() )
                return FileUtil.readFileToByteBuffer(new File(content.getLocation().substring(CommonKeys.CONTENT_LOCATION_FILE_SERVER.length()+1)));
            else // read from linux
            {
                String fileLink = content.getLocation().substring(CommonKeys.CONTENT_LOCATION_FILE_SERVER.length()+1);
                String accessFileLink = getNonWindowsAccessFileLink(fileLink,organizationId,em);
                     log.info("fileLink ="+fileLink+ " accessFileLink="+accessFileLink);
                return FileUtil.readFileToByteBuffer(new File(accessFileLink)); 
            }            
        }
        else
        if (content.getLocation().startsWith(CommonKeys.CONTENT_LOCATION_DATABASE) )
            return Util.getContentFromDatabase(em, platformEm,contentId,content.getLocation());        
        else
            throw new PlatformException("platformException.error.0001");  // content not in system
    }
        
    public static ByteBuffer getDataobjectContentBinaryByContent(int organizationId,EntityManager em, EntityManager platformEm,int repositoryId,Content content) throws Exception
    {
        ServiceInstance objectStorageServiceInstance = platformEm.find(ServiceInstance.class, content.getObjectStorageServiceInstanceId());
        String objectStoreServiceProvider = objectStorageServiceInstance.getServiceProvider();
        Map<String,String> objectStoreServiceProviderProperties = Util.getServiceInstancePropertyConfigMap(objectStorageServiceInstance.getConfig());
            
        if (content.getLocation().startsWith(CommonKeys.CONTENT_LOCATION_BLOB_STORE))
            return ContentStoreUtil.retrieveContent(organizationId,em,content.getId(),objectStorageServiceInstance,objectStoreServiceProvider,objectStoreServiceProviderProperties); 
        else
        if (content.getLocation().startsWith(CommonKeys.CONTENT_LOCATION_FILE_SERVER) )
        {
            if ( RuntimeContext.computingNode.getNodeOsType() == ComputingNodeOSType.Windows.getValue() )
                return FileUtil.readFileToByteBuffer(new File(content.getLocation().substring(CommonKeys.CONTENT_LOCATION_FILE_SERVER.length()+1)));
            else // read from linux
            {
                String fileLink = content.getLocation().substring(CommonKeys.CONTENT_LOCATION_FILE_SERVER.length()+1);
                String accessFileLink = getNonWindowsAccessFileLink(fileLink,organizationId,em);
                     log.info("fileLink ="+fileLink+ " accessFileLink="+accessFileLink);
                return FileUtil.readFileToByteBuffer(new File(accessFileLink)); 
            }            
        }
        else
        if (content.getLocation().startsWith(CommonKeys.CONTENT_LOCATION_DATABASE) )
            return Util.getContentFromDatabase(em, platformEm,content.getId(),content.getLocation());        
        else
            throw new PlatformException("platformException.error.0001");  // content not in system
    }    
    
    private static String getNonWindowsAccessFileLink(String fileLink,int organizationId, EntityManager em) 
    {
        String accessFileLink = null;
        DatasourceConnection dc;
        String sql;
        
        int k = fileLink.lastIndexOf(':');
        
        if ( k>0 )
        {
            String dcName = fileLink.substring(0, k);
            String path = fileLink.substring(k+1);

            try
            {
                sql = String.format("from DatasourceConnection dc where dc.organizationId=%d and dc.name='%s'",organizationId,dcName);
                dc = (DatasourceConnection)em.createQuery(sql).getSingleResult();
                Util.mountDatasourceConnectionFolder(dc); 
                accessFileLink =  String.format("%s",path);
            }
            catch(Exception e) {
            }
        }
      
        return accessFileLink;
    } 
    
    private static String getContentId(EntityManager em, String dataobjectId,int dataobjectVersion, String contentName)
    {
        String sql = String.format("select dc.contentId from DataobjectContent dc where dc.dataobjectContentPK.dataobjectId='%s' and dc.dataobjectContentPK.version=%d and dc.dataobjectContentPK.contentName='%s'",
                dataobjectId, dataobjectVersion, contentName );
        String contentId = (String)em.createQuery(sql).getSingleResult();
        
        return contentId;
    }
   
    public static List<String> getDataobjectContentNames(EntityManager em, String dataobjectId,int dataobjectVersion)
    {
        List<String> contentNames = null;
        
        String sql = String.format("select dc.dataobjectContentPK.contentName from DataobjectContent dc where dc.dataobjectContentPK.dataobjectId='%s' and dc.dataobjectContentPK.version=%d",
                dataobjectId, dataobjectVersion );
        contentNames = em.createQuery(sql).getResultList();
        
        return contentNames;
    }
    
    private static ServiceInstance getObjectStorageServiceInstance(EntityManager em, EntityManager platformEm,String dataobjectId) 
    {       
        String sql = String.format("select r.objectStorageServiceInstanceId from Repository r where r.id =( select d.repositoryId from Dataobject d where d.id ='%s')",dataobjectId );
        int objectStorageServiceInstanceId  = (Integer)em.createQuery(sql).getSingleResult();
        
        sql = String.format("from ServiceInstance si where si.id = %d",objectStorageServiceInstanceId );
        ServiceInstance objectStorageServiceInstance = (ServiceInstance)platformEm.createQuery(sql).getSingleResult();
        
        return objectStorageServiceInstance;      
    }
}
