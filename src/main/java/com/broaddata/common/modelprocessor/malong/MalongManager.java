/*
 * CAUtil.java  - Content Analysis Utility
 *
 */

package com.broaddata.common.modelprocessor.malong;

import org.apache.log4j.Logger;
import java.io.File;

import cn.productai.api.core.DefaultProductAIClient;
import cn.productai.api.core.DefaultProfile;
import cn.productai.api.core.IProfile;
import cn.productai.api.core.IWebClient;
import cn.productai.api.core.enums.ColorReturnType;
import cn.productai.api.core.enums.Granularity;
import cn.productai.api.core.enums.LanguageType;
import cn.productai.api.core.enums.SearchScenario;
import cn.productai.api.core.enums.ServiceType;
import cn.productai.api.core.enums.SubType;
import cn.productai.api.core.exceptions.ClientException;
import cn.productai.api.core.exceptions.ServerException;
import cn.productai.api.pai.entity.classify.ClassifyByImageFileRequest;
import cn.productai.api.pai.entity.color.ColorAnalysisResponse;
import cn.productai.api.pai.entity.color.ColorClassifyByImageFileRequest;
import cn.productai.api.pai.entity.dataset.CreateDataSetRequest;
import cn.productai.api.pai.entity.dataset.CreateDataSetResponse;
import cn.productai.api.pai.entity.dataset.DataSetBaseResponse;
import cn.productai.api.pai.entity.dataset.DataSetSingleAddByImageUrlRequest;
import cn.productai.api.pai.entity.detect.DetectByImageFileRequest;
import cn.productai.api.pai.entity.detect.DetectResponse;
import cn.productai.api.pai.entity.dressing.DressingClassifyByImageFileRequest;
import cn.productai.api.pai.entity.dressing.DressingClassifyByImageUrlRequest;
import cn.productai.api.pai.entity.dressing.DressingClassifyResponse;
import cn.productai.api.pai.entity.search.ImageSearchByImageFileRequest;
import cn.productai.api.pai.entity.search.ImageSearchResponse;
import cn.productai.api.pai.entity.service.CreateSearchServiceRequest;
import cn.productai.api.pai.entity.service.CreateSearchServiceResponse;
import cn.productai.api.pai.response.SearchResult;
import com.broaddata.common.util.Tool;
import org.apache.commons.lang3.exception.ExceptionUtils;

public class MalongManager 
{      
    static final Logger log=Logger.getLogger("MalongManager");      
            
    private IProfile profile = new DefaultProfile();
    private IWebClient client;
    private String host;
    
    public MalongManager(String host,String keyId,String secretKey,String version)
    {
        //profile.setAccessKeyId("6b4d91154cba26e4c82dc1c95052e715");
        //profile.setSecretKey("aad069f6e5b4eb5f81792bb4fd5ee367");
        //profile.setVersion("1");
        profile.setAccessKeyId(keyId);
        profile.setSecretKey(secretKey);
        profile.setVersion(version);
        profile.setGlobalLanguage(LanguageType.Chinese);
        
        this.host = host;
    }
    
    public void init()
    {
        client = new DefaultProductAIClient(profile);
        client.setHost(host);
        //client.setHost("api.productai.cn");
    }
    
    public SearchResult[] searchImageFromDataset(String serviceId,String imageFilepath,int maxExpectedResult) throws Exception
    {
        ImageSearchResponse response;
      
        try 
        {          
            ImageSearchByImageFileRequest request = new ImageSearchByImageFileRequest(serviceId);
            request.setImageFile(new File(imageFilepath));
            request.setLanguage(LanguageType.Chinese);
            
            if ( maxExpectedResult > 0 )
                request.setCount(maxExpectedResult);
  
            response = client.getResponse(request);

        /*    System.out.println("==============================Result==============================");

            // 服务返回的原始Json字符串
            System.out.println(response.getResponseJsonString());

            for (SearchResult r : response.getResults() ) {
                // access the response directly
                System.out.println(String.format("%s - %s", r.getUrl(), r.getScore()));
            }

            System.out.println("==============================Result==============================");*/
     
        } 
        catch (ClientException e)
        { 
            log.error("1111777  ImageSearchByImageFileRequest() failed! errorCode="+e.getErrorCode()+", filename="+imageFilepath+"e="+e.getMessage()+" request id="+e.getRequestId());  
            throw e;
        }
        
        return response.getResults();
    }
    
    public boolean ifImageDetectable(String serviceId,String imageFilepath) throws Exception
    {
        try 
        {          
            ImageSearchByImageFileRequest request = new ImageSearchByImageFileRequest(serviceId);
            request.setImageFile(new File(imageFilepath));
            request.setLanguage(LanguageType.Chinese);
             
            ImageSearchResponse response = client.getResponse(request);
        } 
        catch (ClientException e)
        {
            log.error(String.format("ifImageDetectable() %s occurred. ErrorMessage: %s", e.getClass().getName(), e.getMessage()));
               
            if ( e.getErrorCode().equals("1014") )
                return false;
        }
        
        return true;
    }
    
    public String saveImageToImageSet(String imageFileUrl,String imageSetId,String imageSetName) throws Exception
    {
        String newDatasetId = "";
        String serviceId = "";
        
        while(true)
        {
            if ( imageSetId == null || imageSetId.trim().isEmpty() )
            {
                try 
                {            
                    CreateDataSetRequest requestAdd = new CreateDataSetRequest(imageSetName, imageSetName);

                    CreateDataSetResponse responseAdd = client.getResponse(requestAdd);

                    System.out.println("==============================Result==============================");

                    // access the response directly
                    System.out.println(String.format("DataSet Id - %s", responseAdd.getDataSetId()));
                    System.out.println(String.format("Response Json : %s", responseAdd.getResponseJsonString()));

                    newDatasetId = responseAdd.getDataSetId();

                    System.out.println("==============================Result==============================");

                    String serviceName = String.format("service-%s",imageSetName);
                    CreateSearchServiceRequest requestService = new CreateSearchServiceRequest(newDatasetId, serviceName, SearchScenario.Fashion_V5_4);

                    CreateSearchServiceResponse response = client.getResponse(requestService);

                    serviceId = response.getServiceId();

                } catch (Exception e) {
                    log.error(String.format("carete imageset and service %s occurred. ErrorMessage: %s", e.getClass().getName(), e.getMessage()));
                    throw e;
                }
            }

            DataSetSingleAddByImageUrlRequest request = new DataSetSingleAddByImageUrlRequest(!newDatasetId.isEmpty()?newDatasetId:imageSetId,null,null);
            request.setImageUrl(imageFileUrl);
            request.setLanguage(LanguageType.Chinese);

            int retry =0;
            
            while(true)
            {
                retry++;
                
                try 
                {
                    log.info(" save image imageFileUrl="+imageFileUrl+" retry="+retry);
                    
                    DataSetBaseResponse response = client.getResponse(request);

                    // access the response directly
                    System.out.println(String.format("LastModifiedTime - %s", response.getLastModifiedTime())); 

                    break;
                }
                catch(ClientException e)
                {
                    log.error(" ClientException failed! e="+e+" e.getErrorCode()="+e.getErrorCode());
                    
                    Tool.SleepAWhileInMS(1000);
                        
                    if ( e.getErrorCode().equals("2001") )
                    {
                        imageSetId = null;
                        
                        if ( retry > 5 )
                            throw e;
                    }
                    
                    if ( retry > 100 )
                        throw e;
                }
                catch (Exception e) 
                {
                    log.error(String.format("%s occurred. ErrorMessage: %s imageFileUrl=%s", e.getClass().getName(), e.getMessage(),imageFileUrl));
                     
                    if ( retry > 5 )
                        throw e;
                    
                    Tool.SleepAWhileInMS(1000);
                }
            }
            
            break;
        }  
         
        return String.format("%s,%s",newDatasetId,serviceId);
    }
    
    public int convertMalongBoxValueToStandard(double boxValue,int total)
    {
        double totalInDouble = (double)total;
        return (int)Math.round(boxValue*totalInDouble);
    }
    
    public DetectResponse invokeDressingDetectByFile(File imageFile,int retryTime) throws ServerException, ClientException, Exception 
    {
        DetectResponse response = null;
                
        log.info("call invokeDressingClassifyingByUrl（） imageFile="+imageFile);
   
        DetectByImageFileRequest request = new DetectByImageFileRequest("detect", "_0000173", imageFile, "");
        //request.setImageFile(imageFile);
        request.setLanguage(LanguageType.Chinese);

        // 可选参数列表，不需要请注释掉
        //request.getOptions().put("ret_img_tags", "1");

        int retry = 0;
        
        while(true)
        {
            retry++;
            
            try 
            {
                response = client.getResponse(request);

                log.info("==============================Result==============================");

                // 原始的JSON返回
                log.info(response.getResponseJsonString());                
                break;
            } 
            catch(Exception e)
            {
                log.info(String.format("invokeDressingDetectByFile() failed! %s occurred. ErrorMessage: %s", e.getClass().getName(), e.getMessage()));    
                if ( retry > retryTime )
                    throw e;
            }
        }
               
        return response;
    }
    
    public DressingClassifyResponse invokeDressingClassifyingByFile(File imageFile,int retryTime) throws ServerException, ClientException, Exception 
    {
        DressingClassifyResponse response = null;
                
        log.info("call invokeDressingClassifyingByUrl（） imageFile="+imageFile);
   
        DressingClassifyByImageFileRequest request = new DressingClassifyByImageFileRequest("0-0-1-1");
        request.setImageFile(imageFile);
        request.setLanguage(LanguageType.Chinese);

        // 可选参数列表，不需要请注释掉
        request.getOptions().put("ret_img_tags", "1");

        int retry = 0;
        
        while(true)
        {
            retry++;
            
            try 
            {
                response = client.getResponse(request);

                log.info("==============================Result==============================");

                // 原始的JSON返回
                log.info(response.getResponseJsonString());                
                break;
            } 
            catch(Exception e)
            {
                log.info(String.format("invokeDressingClassifyingByFile() failed! %s occurred. ErrorMessage: %s", e.getClass().getName(), e.getMessage()));    
                if ( retry > retryTime )
                    throw e;
            }
        }
               
        return response;
    }

    public DressingClassifyResponse invokeDressingClassifyingByUrl(String imageUrl,int retryTime) throws ServerException, ClientException, Exception 
    {
        DressingClassifyResponse response = null;
                
        log.info("call invokeDressingClassifyingByUrl（） url="+imageUrl);
   
        DressingClassifyByImageUrlRequest request = new   DressingClassifyByImageUrlRequest("0-0-1-1");
        request.setUrl(imageUrl);
        request.setLanguage(LanguageType.Chinese);

        // 可选参数列表，不需要请注释掉
        request.getOptions().put("ret_img_tags", "1");

        int retry = 0;
        
        while(true)
        {
            retry++;
            
            try 
            {
                response = client.getResponse(request);

                log.info("==============================Result==============================");

                // 原始的JSON返回
                log.info(response.getResponseJsonString());
                
                break;
            } 
            catch (Exception e)
            {
                log.info(String.format("%s occurred. ErrorMessage: %s", e.getClass().getName(), e.getMessage()));    
                if ( retry > retryTime )
                    throw e;
            }
        }
               
        return response;
    }
    
    public ColorAnalysisResponse invokeDetectObjectColorByFile(File imageFile,int retryTime) throws ServerException, ClientException, Exception 
    {
        ColorAnalysisResponse response = null;
                
        ColorClassifyByImageFileRequest request = new ColorClassifyByImageFileRequest("0-0-1-1");
        request.setSubType(SubType.EveryThing);
        request.setGranularity(Granularity.Major);
        request.setReturnType(ColorReturnType.Basic);
        request.setImageFile(imageFile);
        request.setLanguage(LanguageType.Chinese);

        // option paras
        request.getOptions().put("ret_img_tags", "1");

        int retry = 0;
        
        while(true)
        {
            retry++;
            
            try 
            {
                response = client.getResponse(request);

                log.info("==============================Result==============================");

                // 原始的JSON返回
                log.info(response.getResponseJsonString());                
                break;
            } 
            catch(Exception e)
            {
                log.info(String.format("invokeDetectObjectColorByFile() failed! e=%s stacktrace=%s", e.toString(), ExceptionUtils.getStackTrace(e)));    
                if ( retry > retryTime )
                    throw e;
            }
        }
               
        return response;
    }
}
