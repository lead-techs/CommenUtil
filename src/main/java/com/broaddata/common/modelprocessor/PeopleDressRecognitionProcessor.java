/*
 * PeopleDressModelProcessor.java
 *
 */

package com.broaddata.common.modelprocessor;
 
import cn.productai.api.pai.entity.detect.DetectResponse;
import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.Date;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;

import cn.productai.api.pai.entity.dressing.DressingClassifyResponse;
import cn.productai.api.pai.response.DetectResult;
import cn.productai.api.pai.response.DressingClassifyResult;
 
import com.broaddata.common.model.platform.ComputingNode;
import com.broaddata.common.model.platform.VideoAnalysisModel;
import com.broaddata.common.model.platform.VideoObjectFeatureValue;
import com.broaddata.common.model.vo.DataobjectVO;
import com.broaddata.common.model.vo.PictureObjectData;
import com.broaddata.common.modelprocessor.malong.DressingClassifyResultBD;
import com.broaddata.common.modelprocessor.malong.MalongManager;
import com.broaddata.common.util.FileUtil;
import com.broaddata.common.util.Util;
import com.broaddata.common.util.VideoProcessUtil;

import static com.broaddata.common.util.VideoProcessUtil.getTimeInVideoFromPictureName;

public class PeopleDressRecognitionProcessor extends ModelProcessor
{
    static final Logger log = Logger.getLogger("PeopleDressModelProcessor");
 
    private ComputingNode computingNode;
    private VideoAnalysisModel model;
    private EntityManager platformEm;
    private DataobjectVO dataobjectVO;
    private float secondsBetweenTwoFrame;
    private List<VideoObjectFeatureValue> videoObjectFeatureValueList;
    private String videoServerHost;
    private String currentWorkingDirKey;
    private String videoProcessingServiceInstanceId;
    private boolean thisNodeIsAVideoPrcoessingServer;
    private int videoStartTimeInSecond;
    private int videoLength;
    
    public void init(Map<String,Object> parameters) throws Exception
    {
        computingNode = (ComputingNode)parameters.get("computingNode");
        model = (VideoAnalysisModel)parameters.get("model");
        platformEm = (EntityManager)parameters.get("platformEm");
        dataobjectVO = (DataobjectVO)parameters.get("dataobjectVO");
        secondsBetweenTwoFrame = (Float)parameters.get("secondsBetweenTwoFrame");           
        currentWorkingDirKey = (String)parameters.get("currentWorkingDirKey"); 
        videoObjectFeatureValueList = (List<VideoObjectFeatureValue>)parameters.get("videoObjectFeatureValueList");
        videoServerHost = (String)parameters.get("videoServerHost");
        videoProcessingServiceInstanceId = (String)parameters.get("videoProcessingServiceInstanceId");
        thisNodeIsAVideoPrcoessingServer = (Boolean)parameters.get("thisNodeIsAVideoPrcoessingServer");
         
        videoStartTimeInSecond = (Integer)parameters.get("videoStartTimeInSecond");
        videoLength = (Integer)parameters.get("videoLength");
    }
    
    @Override
    public Map<String,Object> execute() throws Exception
    {
        Map<String,Object> result = new HashMap<>();
                
        log.info(" run people dressing recogonition model!   ");
        
        Map<String,String> map = Util.getEDFVideoProcessingService(platformEm,videoProcessingServiceInstanceId);
 
        //String videoServerHost = Util.pickOneServiceIP(map.get("ips"));
        String videoServerUser = map.get("user");
        String videoServerPassword = map.get("password");
        
        map = Util.getEDFVideoProcessingService(platformEm,model.getServiceInstanceId());
 
        String host = Util.pickOneServiceIP(map.get("ips"));
        String keyId=map.get("keyId");
        String secretKey = map.get("secretKey");
        String version = map.get("version");
        
        Map<String,String> map1 = VideoProcessUtil.getVideoProcessingWorkingDirectory(thisNodeIsAVideoPrcoessingServer, computingNode, currentWorkingDirKey,videoServerHost,videoServerUser,videoServerPassword);

        //String videoServerWorkingDirectory = map1.get("videoServerWorkingDirectory");
        String edfServerWorkingDirectory =  map1.get("edfServerWorkingDirectory");
   
        // extract object             
        MalongManager manager = new MalongManager(host,keyId,secretKey,version);
        
        manager.init();
        
        List<String> filepathList = new ArrayList<>();
        
        String imgFolder = String.format("%s/img",edfServerWorkingDirectory);
        
        FileUtil.getAllFilesWithSamePrefix(new File(imgFolder), "Image", filepathList);
        
        List<PictureObjectData> videoObjectData = new ArrayList<>();
        
        int i=0;
        
        for(String imageFilepath : filepathList)
        {
            i++;
            String imageFilename = imageFilepath.replaceAll("\\\\", "/");
            imageFilename = imageFilepath.substring(imageFilename.lastIndexOf("/")+1);

            log.info("i="+i+",total="+filepathList.size()+", imageFilepath="+imageFilepath);
       
            PictureObjectData pictureVideoObjectData = null; // dectect model
            PictureObjectData pictureVideoObjectData1 = null; // dress classify model
            
            DetectResponse detectResponse = manager.invokeDressingDetectByFile(new File(imageFilepath),10);  
        
            if ( detectResponse.getDetectedBoxes().length > 0 )
                pictureVideoObjectData = getOneImageVideoObjectDataForDressingDetect(manager,model.getId(),imageFilename,imageFilepath,detectResponse);
                 
            DressingClassifyResponse response = manager.invokeDressingClassifyingByFile(new File(imageFilepath),10);  
        
            if ( response.getResults().length> 0 )
                pictureVideoObjectData1 = getOneImageVideoObjectDataForDressingClassify(manager,model.getId(),imageFilename,imageFilepath,response);
        
            if ( pictureVideoObjectData == null && pictureVideoObjectData1 == null )
                continue;
          
            if ( pictureVideoObjectData == null && pictureVideoObjectData1 != null )
                videoObjectData.add(pictureVideoObjectData1);
            else
            if ( pictureVideoObjectData != null && pictureVideoObjectData1 == null )  
                videoObjectData.add(pictureVideoObjectData);
            else 
            {
                processOverlapping(pictureVideoObjectData,pictureVideoObjectData1);
                
                mergeData(pictureVideoObjectData,pictureVideoObjectData1);
                 
                videoObjectData.add(pictureVideoObjectData);
            }
        }
                 
        result.put("videoObjectData", videoObjectData);
        
        return result;
    }            
    
    private void mergeData(PictureObjectData pictureVideoObjectData,PictureObjectData pictureVideoObjectData1)
    {
        for(int i=0;i<pictureVideoObjectData1.getObjectTypes().size();i++) // each object
        {            
            pictureVideoObjectData.getObjectTypes().add(pictureVideoObjectData1.getObjectTypes().get(i));
            pictureVideoObjectData.getObjectPositions().add(pictureVideoObjectData1.getObjectPositions().get(i));
            pictureVideoObjectData.getObjectFeatures().add(pictureVideoObjectData1.getObjectFeatures().get(i));
        }
    }
    
    private void processOverlapping(PictureObjectData pictureVideoObjectData,PictureObjectData pictureVideoObjectData1)
    { 
        List<Integer> removeList = new ArrayList<>();
        
        for(int i=0;i<pictureVideoObjectData1.getObjectTypes().size();i++) // each object
        {            
            for(int j=0;j<pictureVideoObjectData.getObjectTypes().size();j++)
            {
                boolean isOverlapping = checkIfOverlap(pictureVideoObjectData1.getObjectPositions().get(i),pictureVideoObjectData.getObjectPositions().get(j));
                
                if ( isOverlapping )
                {
                    if ( !removeList.contains(j) )
                        removeList.add(j);
                }
            }
        }
        
        List<Integer> objectTypes = new ArrayList<>();
        List<Map<String,Integer>> objectPositions = new ArrayList<>(); // 
        List<Map<String,String>> objectFeatures = new ArrayList<>(); 
            
        for(int j=0; j<pictureVideoObjectData.getObjectTypes().size();j++)
        {
            if ( removeList.contains(j) )
                continue;   
                    
            objectTypes.add(pictureVideoObjectData.getObjectTypes().get(j));
            objectPositions.add(pictureVideoObjectData.getObjectPositions().get(j));
            objectFeatures.add(pictureVideoObjectData.getObjectFeatures().get(j));
        }
        
        pictureVideoObjectData.setObjectTypes(objectTypes);
        pictureVideoObjectData.setObjectPositions(objectPositions);
        pictureVideoObjectData.setObjectFeatures(objectFeatures);
    }
      
    private boolean checkIfOverlap(Map<String,Integer> position,Map<String,Integer> position1)
    {
        double[] rectangle1 = new double[4];

        rectangle1[0] = position.get("top_x");
        rectangle1[1] = position.get("top_y");
        rectangle1[2] = position.get("bottom_x");
        rectangle1[3] = position.get("bottom_y");
                
        double[] rectangle2 = new double[4];

        rectangle2[0] = position1.get("top_x");
        rectangle2[1] = position1.get("top_y");
        rectangle2[2] = position1.get("bottom_x");
        rectangle2[3] = position1.get("bottom_y");

        double rate = VideoProcessUtil.getOverlappingRateBetweenRectangels(rectangle1,rectangle2);

        if ( rate > 0.5 )
        {
            log.info(" found overlaping. rate = "+rate);
            return true;
        }
        else
            return false;
    }
      
    private PictureObjectData getOneImageVideoObjectDataForDressingClassify(MalongManager manager,int modelId,String imageFilename,String imageFilepath,DressingClassifyResponse response) throws Exception
    {        
        Map<String,Object> map = VideoProcessUtil.getImageInfo(FileUtil.readFileToByteBuffer(new File(imageFilepath)));
        
        int imageHeight = (Integer)map.get("height");
        int imageWidth = (Integer)map.get("width");
        
        //log.info(" imageHeight="+imageHeight+" imageWidth="+imageWidth);
            
        PictureObjectData pictureObjectData = new PictureObjectData();
        pictureObjectData.setRecognitionModelId(modelId);
        
        Date videoTime = new Date();
        pictureObjectData.setVideoTime(videoTime);
                   
        List<Map<String, String>> objectFeatures = new ArrayList<>();
        List<Integer> objectTypes = new ArrayList<>();
        List<Map<String,Integer>> objectPositions = new ArrayList<>();
        
        pictureObjectData.setObjectFeatures(objectFeatures);
        pictureObjectData.setObjectTypes(objectTypes);
        pictureObjectData.setPictureFilename(imageFilename);
        pictureObjectData.setObjectPositions(objectPositions);
 
        pictureObjectData.setPictureFilepath(imageFilepath);

        int timeInVideoInSeconds = videoStartTimeInSecond+getTimeInVideoFromPictureName(imageFilename,(int)secondsBetweenTwoFrame);
        pictureObjectData.setTimeInVideoInSeconds(timeInVideoInSeconds);
   
        for(DressingClassifyResult result : response.getResults())  
        {
            // process one item 
            DressingClassifyResultBD resultBD = DressingClassifyResultBD.fromDressingClassifyResult(result);
                                                
            String objectItemName = resultBD.getItemEN();
            
            int objectTypeId = VideoProcessUtil.getObjectTypeIdByItem(videoObjectFeatureValueList,objectItemName,model.getId()); 
            
            if ( objectTypeId == 0 )
            {
                log.warn(" object Type not found! item="+objectItemName);
                continue;
            }
            
            objectTypes.add(objectTypeId);
            
            Map<String,Integer> position = new HashMap<>();
            
            try
            {
                int value = manager.convertMalongBoxValueToStandard(resultBD.getLeft(),imageWidth);
                position.put("top_x", value);

                value = manager.convertMalongBoxValueToStandard(resultBD.getTop(),imageHeight);
                position.put("top_y", value);

                value = manager.convertMalongBoxValueToStandard(resultBD.getRight(),imageWidth);
                position.put("bottom_x", value);

                value = manager.convertMalongBoxValueToStandard(resultBD.getBottom(),imageHeight);
                position.put("bottom_y", value);
            }
            catch(Exception e)
            {
                log.error(" position failed! e="+e);
                continue;
            }
            
            objectPositions.add(position);
                                        
            Map<String,String> features = new HashMap<>();
            
            String valueInDBFormat;
            
            valueInDBFormat = VideoProcessUtil.convertToDBFormat(platformEm,model.getId(),"item",resultBD.getItemEN());
            features.put("1",valueInDBFormat);
            
            valueInDBFormat = VideoProcessUtil.convertToDBFormat(platformEm,model.getId(),"dress_color",resultBD.getColorEN1());
            features.put("2", valueInDBFormat);
                        
            valueInDBFormat = VideoProcessUtil.convertToDBFormat(platformEm,model.getId(),"texture",resultBD.getTextureEN());
            features.put("3", valueInDBFormat);

            objectFeatures.add(features);
        }

        return pictureObjectData;        
    }
     
    private PictureObjectData getOneImageVideoObjectDataForDressingDetect(MalongManager manager,int modelId,String imageFilename,String imageFilepath,DetectResponse response) throws Exception
    {        
        Map<String,Object> map = VideoProcessUtil.getImageInfo(FileUtil.readFileToByteBuffer(new File(imageFilepath)));
        
        int imageHeight = (Integer)map.get("height");
        int imageWidth = (Integer)map.get("width");
        
        log.info(" imageHeight="+imageHeight+" imageWidth="+imageWidth);
            
        PictureObjectData pictureObjectData = new PictureObjectData();
        pictureObjectData.setRecognitionModelId(modelId);
        
        Date videoTime = new Date();
        pictureObjectData.setVideoTime(videoTime);
                   
        List<Map<String, String>> objectFeatures = new ArrayList<>();
        List<Integer> objectTypes = new ArrayList<>();
        List<Map<String,Integer>> objectPositions = new ArrayList<>();
        
        pictureObjectData.setObjectFeatures(objectFeatures);
        pictureObjectData.setObjectTypes(objectTypes);
        pictureObjectData.setPictureFilename(imageFilename);
        pictureObjectData.setObjectPositions(objectPositions);
 
        pictureObjectData.setPictureFilepath(imageFilepath);

        int timeInVideoInSeconds = videoStartTimeInSecond+getTimeInVideoFromPictureName(imageFilename,(int)secondsBetweenTwoFrame);
        pictureObjectData.setTimeInVideoInSeconds(timeInVideoInSeconds);
   
        for(DetectResult result : response.getDetectedBoxes())
        {
            Map<String,Integer> position = new HashMap<>();
                                                                          
            String itemName = result.getPuid();
            itemName = itemName.replace("-", "_");
            
            int objectTypeId = VideoProcessUtil.getObjectTypeIdByItem(videoObjectFeatureValueList,itemName,model.getId());
            
            if ( objectTypeId != 3 && objectTypeId != 4 && objectTypeId != 5 && objectTypeId != 6 && objectTypeId != 7  ) // if not dress
            {
                log.warn("555555555555555 objectTypeId = "+objectTypeId+" puid="+result.getPuid());
                continue;
            }
            
            objectTypes.add(objectTypeId);
            
            try
            {
                int value = manager.convertMalongBoxValueToStandard(result.getBoxLocation()[0],imageWidth);
                position.put("top_x", value);

                value = manager.convertMalongBoxValueToStandard(result.getBoxLocation()[1],imageHeight);
                position.put("top_y", value);

                value = manager.convertMalongBoxValueToStandard(result.getBoxLocation()[0]+result.getBoxLocation()[2],imageWidth);
                position.put("bottom_x", value);

                value = manager.convertMalongBoxValueToStandard(result.getBoxLocation()[1]+result.getBoxLocation()[3],imageHeight);
                position.put("bottom_y", value);
            }
            catch(Exception e)
            {
                log.error(" position failed! e="+e);
                continue;
            }
            
            objectPositions.add(position);
                                        
            Map<String,String> features = new HashMap<>();
            
            String valueInDBFormat;
            
            valueInDBFormat = VideoProcessUtil.convertToDBFormat(platformEm,model.getId(),"item",itemName);
            features.put("1",valueInDBFormat);
            features.put("2","0");
            features.put("3","0");
            
           // valueInDBFormat = convertToDBFormat("dress_color",resultBD.getColorEN1());
           // features.put("2", valueInDBFormat);
                        
          //  valueInDBFormat = convertToDBFormat("texture",resultBD.getTextureEN());
          //  features.put("3", valueInDBFormat);
                        
            objectFeatures.add(features);
        }

        return pictureObjectData;        
    }
}
