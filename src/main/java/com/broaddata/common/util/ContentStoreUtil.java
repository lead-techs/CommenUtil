/*
 * BlobStoreUtil.java
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import javax.persistence.EntityManager;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext; 
import org.jclouds.blobstore.domain.Blob; 
import static org.jclouds.blobstore.options.PutOptions.Builder.*;
import com.broaddata.common.model.enumeration.ComputingNodeOSType;
import com.broaddata.common.model.organization.DatasourceConnection;
import com.broaddata.common.model.platform.ComputingNode;
import com.broaddata.common.model.platform.ServiceInstance;
import com.broaddata.common.thrift.dataservice.DataService;
import com.google.common.io.ByteSource;
import org.jclouds.filesystem.reference.FilesystemConstants;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.ByteSourcePayload;
import org.jclouds.io.payloads.FilePayload;

public class ContentStoreUtil 
{
    static final Logger log=Logger.getLogger("ContentStoreUtil");
    
    public static ByteBuffer retrieveContent(int organizationId,EntityManager em,String contentId,ServiceInstance objectStorageServiceInstance,String providerName,Map<String,String> providerProperties) throws PlatformException,Exception
    {
        ByteBuffer contentStream = null;
        
        BlobStoreContext context = getBlobStoreContext(organizationId,em,RuntimeContext.computingNode,objectStorageServiceInstance,providerProperties);
        
        String containerName = providerProperties.get("bucket_name");
        BlobStore blobStore = context.getBlobStore();
        
        Blob blob = blobStore.getBlob(containerName, contentId);
        
        if(null==blob)
        {
            return null;
        }
        Payload payload = blob.getPayload();
        
        if ( payload instanceof ByteSourcePayload )
        {
            ByteSourcePayload p = (ByteSourcePayload)payload;
            ByteSource source = p.getRawContent();
            InputStream in = source.openStream();
            contentStream = FileUtil.readFileToByteBuffer(in);
        }   

        context.close();
        
        return contentStream;
    }

    public static ByteBuffer retrieveContent(int organizationId,DataService.Client dataService,String contentId,ServiceInstance objectStorageServiceInstance,Map<String,String> providerProperties) throws PlatformException,Exception
    {
        ByteBuffer contentStream = null;
        
        BlobStoreContext context = getBlobStoreContext(organizationId,dataService,RuntimeContext.computingNode.getNodeOsType(),objectStorageServiceInstance,providerProperties);
        
        String containerName = providerProperties.get("bucket_name");
        BlobStore blobStore = context.getBlobStore();
        
        Blob blob = blobStore.getBlob(containerName, contentId);
 
        Object content = blob.getPayload().getRawContent();

        if ( content instanceof File )
        {
            contentStream = FileUtil.readFileToByteBuffer((File)content);
        }
        
        //close context
        context.close();
        
        return contentStream;
    }
    
    public static void storeContent(int organizationId,DataService.Client dataService,int nodeOSType,String contentId,ByteBuffer contentStream,ServiceInstance objectStorageServiceInstance, Map<String,String> providerProperties) throws PlatformException,Exception
    {
        log.info("start storeContent! contentId="+contentId);
        
        BlobStoreContext context = getBlobStoreContext(organizationId,dataService,nodeOSType,objectStorageServiceInstance,providerProperties);
                
        String containerName = providerProperties.get("bucket_name");
        BlobStore blobStore = context.getBlobStore();
        
        if ( !blobStore.containerExists(containerName) )
            blobStore.createContainerInLocation(null, containerName);

        contentStream.position(0);
        InputStream bis = new ByteBufferBackedInputStream(contentStream);
         
        Blob blob = blobStore.blobBuilder(contentId).payload(bis).build();
         
        if ( contentStream.capacity() > (RuntimeContext.internetVersion?CommonKeys.INTERNET_BIG_FILE_SIZE:CommonKeys.INTRANET_BIG_FILE_SIZE) )
            blobStore.putBlob(containerName, blob, multipart());
        else
            blobStore.putBlob(containerName, blob);
         
        log.info("storeContent succeeded! contentId="+contentId);
    }    

    private static BlobStoreContext getBlobStoreContext(int organizationId,DataService.Client dataService,int nodeOSType,ServiceInstance objectStorageServiceInstance, Map<String,String> providerProperties) throws PlatformException, Exception
    {
        DatasourceConnection datasourceConnection = null;
        Properties properties = new Properties();
        BlobStoreContext context = null;
        String storagePath = null;
        String localFolder = "";
        
        if ( objectStorageServiceInstance.getServiceProvider().equals("filesystem"))
        {
            if ( nodeOSType == ComputingNodeOSType.Windows.getValue() )
                localFolder = Tool.convertToWindowsPathFormat(providerProperties.get("storage_path_for_windows"));
            else
            {
                storagePath = Tool.convertToNonWindowsPathFormat(providerProperties.get("storage_path_for_linux"));

                if ( !storagePath.startsWith("datasourceConnection:") )
                    localFolder = storagePath;
                else
                {
                    String datasourceConnectionName = storagePath.substring("datasourceConnection:".length());
                    
                    try {
                         datasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getDatasourceConnectionByName(organizationId,datasourceConnectionName).array());
                    }
                    catch(Exception e) 
                    {
                        log.error(" get dataousrceConnection failed! name="+datasourceConnectionName);
                        throw new Exception("2345");
                    }
 
                    localFolder = Util.mountDatasourceConnectionFolder(datasourceConnection);
                }
            }
            // setup where the provider must store the files
            properties.setProperty(FilesystemConstants.PROPERTY_BASEDIR, localFolder);

            // get a context with filesystem that offers the portable BlobStore api
            context = ContextBuilder.newBuilder("filesystem")
                             .overrides(properties)
                             .buildView(BlobStoreContext.class);
        }
        else
        if ( objectStorageServiceInstance.getServiceProvider().equals("oss"))
        {
            
        }
        else
        if ( objectStorageServiceInstance.getServiceProvider().equals("swift"))
        {
            
        }
        else            
            throw new Exception("2345");
        
        //log.info("1111111111 localFolder="+localFolder);
                
        return context;
    }
        
    private static BlobStoreContext getBlobStoreContext(int organizationId,EntityManager em,ComputingNode computingNode,ServiceInstance objectStorageServiceInstance, Map<String,String> providerProperties) throws Exception
    {
        DatasourceConnection datasourceConnection = null;
        Properties properties = new Properties();
        BlobStoreContext context = null;
        String storagePath = null;
        String localFolder;
        
        if ( objectStorageServiceInstance.getServiceProvider().equals("filesystem"))
        {
            if ( computingNode.getNodeOsType() == ComputingNodeOSType.Windows.getValue() )
                localFolder = Tool.convertToWindowsPathFormat(providerProperties.get("storage_path_for_windows"));
            else
            {
                storagePath = Tool.convertToNonWindowsPathFormat(providerProperties.get("storage_path_for_linux"));

                if ( !storagePath.startsWith("datasourceConnection:") )
                    localFolder = storagePath;
                else
                {
                    String datasourceConnectionName = storagePath.substring("datasourceConnection:".length());

                    datasourceConnection = Util.getDatasourceConnectionByName(em,organizationId,datasourceConnectionName);
                    
                    if ( datasourceConnection == null )
                        throw new Exception("2345");

                    localFolder = Util.mountDatasourceConnectionFolder(datasourceConnection);
                }
            }
            
            //log.info("computingNode.getNodeOsType()"+computingNode.getNodeOsType()+" localfolder="+localFolder);
            
            // setup where the provider must store the files
            properties.setProperty(FilesystemConstants.PROPERTY_BASEDIR, localFolder);

            // get a context with filesystem that offers the portable BlobStore api
            context = ContextBuilder.newBuilder("filesystem")
                             .overrides(properties)
                             .buildView(BlobStoreContext.class);
        }
        else
        if ( objectStorageServiceInstance.getServiceProvider().equals("oss"))
        {
            
        }
        else
        if ( objectStorageServiceInstance.getServiceProvider().equals("swift"))
        {
            
        }
        else            
            throw new Exception("2345");
                
        return context;
    }
}
