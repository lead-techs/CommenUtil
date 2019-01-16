/*
 * CarBasicPropertyModelProcessor.java
 *
 */

package com.broaddata.common.modelprocessor;
 
import org.apache.log4j.Logger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;
import javax.persistence.EntityManager;
import cn.productai.api.pai.entity.color.ColorAnalysisResponse;

import com.broaddata.common.model.enumeration.EncodingType;
import com.broaddata.common.model.platform.ComputingNode;
import com.broaddata.common.model.platform.VideoAnalysisModel;
import com.broaddata.common.model.platform.VideoObjectFeatureValue;
import com.broaddata.common.model.vo.PictureObjectData;
import com.broaddata.common.modelprocessor.malong.MalongManager;
import com.broaddata.common.util.FileUtil;
import com.broaddata.common.util.Tool;
import com.broaddata.common.util.Util;
import com.broaddata.common.util.VideoProcessUtil;

public class CarRecognitionProcessor extends ModelProcessor
{
    static final Logger log = Logger.getLogger("CarRecognitionProcessor");
    
    private ComputingNode computingNode;
    private VideoAnalysisModel model;
    private EntityManager platformEm;
    private float secondsBetweenTwoFrame;
    private String videoServerHost;
    private String currentWorkingDirKey;
    private String videoProcessingServiceInstanceId;
    private boolean thisNodeIsAVideoPrcoessingServer;
    private List<VideoObjectFeatureValue> videoObjectFeatureValueList;
    private int videoStartTimeInSecond;
    private int videoLength;
    
    public void init(Map<String,Object> parameters) throws Exception
    {
        computingNode = (ComputingNode)parameters.get("computingNode");
        model = (VideoAnalysisModel)parameters.get("model");
        platformEm = (EntityManager)parameters.get("platformEm");
        secondsBetweenTwoFrame = (Float)parameters.get("secondsBetweenTwoFrame"); 
        currentWorkingDirKey = (String)parameters.get("currentWorkingDirKey"); 
        videoServerHost = (String)parameters.get("videoServerHost");
        videoProcessingServiceInstanceId = (String)parameters.get("videoProcessingServiceInstanceId");
        thisNodeIsAVideoPrcoessingServer = (Boolean)parameters.get("thisNodeIsAVideoPrcoessingServer");
        videoObjectFeatureValueList = (List<VideoObjectFeatureValue>)parameters.get("videoObjectFeatureValueList");
        
        videoStartTimeInSecond = (Integer)parameters.get("videoStartTimeInSecond");
        videoLength = (Integer)parameters.get("videoLength");
    }
    
    @Override
    public Map<String,Object> execute() throws Exception
    {
        Date videoTime;
        String featureResultFile = "object-detect-result.txt";
        List<PictureObjectData> dataList = new ArrayList<>();
        Map<String,Object> result = new HashMap<>();
        
        log.info(" run car propery recogonition model!   ");
        
        Map<String,String> map = Util.getEDFVideoProcessingService(platformEm,videoProcessingServiceInstanceId);
 
        //String videoServerHost = Util.pickOneServiceIP(map.get("ips"));
        String videoServerUser = map.get("user");
        String videoServerPassword = map.get("password");
        
        map = Util.getEDFVideoProcessingService(platformEm,model.getServiceInstanceId());
 
        String host = Util.pickOneServiceIP(map.get("ips"));
        String keyId=map.get("keyId");
        String secretKey = map.get("secretKey");
        String version = map.get("version");
             
        MalongManager mlManager = new MalongManager(host,keyId,secretKey,version);
        
        mlManager.init();
        
        Map<String,String> map1 = VideoProcessUtil.getVideoProcessingWorkingDirectory(thisNodeIsAVideoPrcoessingServer,computingNode, currentWorkingDirKey,videoServerHost,videoServerUser,videoServerPassword);
        String edfServerWorkingDirectory =  map1.get("edfServerWorkingDirectory");
        
        String videoObjectExtractionAttributeFilename = String.format("%s/%s",edfServerWorkingDirectory,featureResultFile);
                    
        // get result csv file, process csv file

        String lastPictureFilename = "";
        String pictureFilepath = "";
        List<String> rows = FileUtil.readTxtFileToStringList(videoObjectExtractionAttributeFilename, EncodingType.UTF_8.getValue(), 1, 0,true);
       
        if ( rows.isEmpty() )
        {
            result.put("videoObjectData", dataList);
            return result;
        }
        
        PictureObjectData pictureObjectData = new PictureObjectData(); // first picture
        pictureObjectData.setRecognitionModelId(model.getId());
        //data.setNumberOfObject(3);

        videoTime = new Date();
        pictureObjectData.setVideoTime(videoTime);
                   
        List<Map<String, String>> objectFeatures = new ArrayList<>();
        List<Integer> objectTypes = new ArrayList<>();
        List<Map<String,Integer>> objectPositions = new ArrayList<>();
         
        for(String row : rows)
        {
            String[] columns = row.split("\\,");
            float score = Float.parseFloat(columns[3]);
            
            if ( score < 0.9 )
            {
                log.info(" skip low score! score="+score);
                continue;
            }
            
            String objectName = columns[2];
 
            int objectTypeId = VideoProcessUtil.getObjectTypeIdByItem(videoObjectFeatureValueList,objectName,model.getObjectDetectionModelId()); 
            
            if ( objectTypeId != 2 )
            {
                log.info("only select viehcle, skip other object type="+objectName);
                continue;
            }
               
            String pictureFilename = columns[0];
            
            if ( pictureFilename.contains("/") )
            {
                pictureFilename = pictureFilename.substring(pictureFilename.lastIndexOf("/")+1);
            }
                        
            if ( !lastPictureFilename.equals(pictureFilename) && !lastPictureFilename.isEmpty() )
            {          
                pictureObjectData.setObjectFeatures(objectFeatures);
                pictureObjectData.setObjectTypes(objectTypes);
                pictureObjectData.setPictureFilename(lastPictureFilename);
                pictureObjectData.setObjectPositions(objectPositions);
                
                pictureFilepath = String.format("%s/img/%s",edfServerWorkingDirectory,lastPictureFilename);
                pictureObjectData.setPictureFilepath(pictureFilepath);
                
                int timeInVideoInSeconds = videoStartTimeInSecond+VideoProcessUtil.getTimeInVideoFromPictureName(lastPictureFilename,(int)secondsBetweenTwoFrame);
                pictureObjectData.setTimeInVideoInSeconds(timeInVideoInSeconds);
                
                dataList.add(pictureObjectData);
                
                pictureObjectData = new PictureObjectData(); // first picture
                pictureObjectData.setRecognitionModelId(model.getId());
                
                videoTime = new Date(0);
                pictureObjectData.setVideoTime(videoTime);
                
                objectFeatures = new ArrayList<>();
                objectTypes = new ArrayList<>();
                objectPositions = new ArrayList<>();
            }
         
            Map<String,Integer> position = new HashMap<>();
            
            try
            {
                String p = Tool.removeLastString(columns[4],".");
                position.put("top_x", Integer.parseInt(p));

                p = Tool.removeLastString(columns[5],".");
                position.put("top_y", Integer.parseInt(p));

                p = Tool.removeLastString(columns[6],".");
                position.put("bottom_x", Integer.parseInt(p));

                p = Tool.removeLastString(columns[7],".");
                position.put("bottom_y", Integer.parseInt(p));
            }
            catch(Exception e)
            {
                log.error(" position failed! e="+e+" row="+row);
                continue;
            }
                                    
            objectTypes.add(objectTypeId);            
            objectPositions.add(position);
                                        
            Map<String,String> features = new HashMap<>();
                        
            String filename = String.format("%s/img/%s",edfServerWorkingDirectory,pictureFilename);
            
            String carColor = detectCarColor(mlManager,filename,position);
           
            String valueInDBFormat = VideoProcessUtil.convertToDBFormat(platformEm,model.getObjectDetectionModelId(),"item",objectName);
            features.put("1", valueInDBFormat);
                         
            String carType = "unknown";
            valueInDBFormat = VideoProcessUtil.convertToDBFormat(platformEm,model.getObjectDetectionModelId(),"car_type",carType);
            features.put("2", valueInDBFormat);
            
            valueInDBFormat = VideoProcessUtil.convertToDBFormat(platformEm,model.getObjectDetectionModelId(),"car_color",carColor);
            features.put("3", valueInDBFormat);
  
            features.put("4","沪J07579");         // plate number
                                                  
            objectFeatures.add(features);

            lastPictureFilename = pictureFilename;
        }
        
        if ( objectPositions.isEmpty() )
        {
            result.put("videoObjectData", dataList);
            return result;
        }
        
        pictureObjectData.setObjectFeatures(objectFeatures);
        pictureObjectData.setObjectTypes(objectTypes);
        pictureObjectData.setPictureFilename(lastPictureFilename);
        pictureObjectData.setObjectPositions(objectPositions);

        pictureFilepath = String.format("%s/img/%s",edfServerWorkingDirectory,lastPictureFilename);
        pictureObjectData.setPictureFilepath(pictureFilepath);

        int timeInVideoInSeconds = VideoProcessUtil.getTimeInVideoFromPictureName(lastPictureFilename,(int)secondsBetweenTwoFrame);
        pictureObjectData.setTimeInVideoInSeconds(timeInVideoInSeconds);
                
        pictureObjectData.setTimeInVideoInSeconds(timeInVideoInSeconds);

        dataList.add(pictureObjectData);
        
        result.put("videoObjectData", dataList);
        
        return result;
    }            
    
    String detectCarColor(MalongManager mlManager,String pictureFilepath, Map<String,Integer> position)
    {
        String carColor = "";
 
        try
        {
            byte[] contentStream = FileUtil.readFileToByteArray(new File(pictureFilepath));
   
            int topX = position.get("top_x");
            int topY = position.get("top_y");
            int bottomX = position.get("bottom_x");
            int bottomY = position.get("bottom_y");
            
            // create sub picture, save to tmp folder
            ByteBuffer subPicture = VideoProcessUtil.getSubImage(ByteBuffer.wrap(contentStream),topX,topY,bottomX-topX,bottomY-topY);   
            String tmpFilename = String.format("%s.jpg",UUID.randomUUID().toString());
            String subPictureFilePath = String.format("%s/%s",Tool.getSystemTmpdir(),tmpFilename);
                
            FileUtil.writeFileFromByteBuffer(subPictureFilePath, subPicture);
            
            ColorAnalysisResponse response = mlManager.invokeDetectObjectColorByFile(new File(subPictureFilePath), 10);
            
            if ( response.getResults().length > 0 )
                carColor = response.getResults()[0].getBasic();
        }
        catch(Exception e)
        {
            log.info(" detectCarColor() failed! file="+pictureFilepath+" e="+e);
        }
            
        return carColor;
    }
}
