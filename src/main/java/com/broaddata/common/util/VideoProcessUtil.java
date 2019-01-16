/*
 * VideoProcessUtil.java
 *
 */

package com.broaddata.common.util;

import com.broaddata.common.exception.DataProcessingException;
import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import javax.imageio.ImageIO;
import javax.persistence.EntityManager;
import org.apache.tools.ant.util.FileUtils;
import java.io.IOException;
import java.util.Calendar;
import java.util.UUID;
import org.json.JSONException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.StrictDynamicMappingException;
import org.elasticsearch.indices.TypeMissingException;

import com.broaddata.common.model.enumeration.EncodingType;
import com.broaddata.common.model.enumeration.ImageSetType;
import com.broaddata.common.model.organization.Content;
import com.broaddata.common.model.organization.ImageSet;
import com.broaddata.common.model.organization.Repository;
import com.broaddata.common.model.organization.UnstructuredFileProcessingJob;
import com.broaddata.common.model.organization.UnstructuredFileProcessingTask;
import com.broaddata.common.model.platform.VideoObjectType;
import com.broaddata.common.model.platform.ComputingNode;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.model.platform.VideoAnalysisModel;
import com.broaddata.common.model.platform.VideoObjectFeatureValue;
import com.broaddata.common.model.vo.DataobjectVO;
import com.broaddata.common.model.vo.PictureObjectData;
import com.broaddata.common.thrift.commonservice.CommonService;
import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.model.enumeration.ComputingNodeOSType;
import com.broaddata.common.model.enumeration.DataobjectStatus;
import com.broaddata.common.model.enumeration.VideoToImageFrequencyType;

public class VideoProcessUtil 
{      
    static final Logger log = Logger.getLogger("VideoProcessUtil");

    public static final int minObjectSize = 51;
            
    public static int getMediaDurationInSecond(String mediaFileUNCPath) 
    {
        String outputFilename = String.format("%s/%d.mediainfo",Tool.getSystemTmpdir(),new Date().getTime());
        String commandStr = String.format("ffmpeg -i %s",mediaFileUNCPath);

        RCUtil.executeCommand(commandStr, null, false,outputFilename, null,false);

        String fileInfo = FileUtil.readTextFileToString(outputFilename);
        
        int position = fileInfo.indexOf("Duration:"); 
        String time  =  fileInfo.substring(position+10,position+10+8);
        
        int hour = Integer.parseInt(time.substring(0,2));
        int minute = Integer.parseInt(time.substring(3,5));
        int second = Integer.parseInt(time.substring(6,8));
        
        return hour*3600+minute*60+second;
    }
     
    public static List<Map<String,Object>> retrieveVideoObjectData(EntityManager em, EntityManager platformEm,int organizationId, int repositoryId, List<String> dataobjectIds,String[] selectedFields,String[] sortFields) throws Exception 
    {
        List<Map<String,Object>> resultList = new ArrayList<>();
        String queryStr;
        
        try
        {
            Client esClient = ESUtil.getClient(organizationId,repositoryId,false);
                     
            queryStr = "_id:( ";
            
            int i=0;
            for(String videoObjectId : dataobjectIds)
            {
                i++;
                queryStr += "\""+videoObjectId + "\" ";
        
                if ( i == 500 )
                {
                    queryStr += ")";

                    //log.info("1111 queryStr="+queryStr);
                    
                    Map<String,Object> ret = ESUtil.retrieveSimpleDataset(esClient, new String[]{}, new String[]{}, queryStr, null, 1000, 0, selectedFields, sortFields,new String[]{},null);
    
                    List<Map<String,Object>> searchResults = (List<Map<String,Object>>)ret.get("searchResults");
                   
                    for(Map<String,Object> result : searchResults)
                    {
                        String dataobjectId = (String)result.get("id");
                        dataobjectId = dataobjectId.substring(0, 40);
                        result.put("dataobjectId",dataobjectId);
                        
                        ((Map<String,Object>)result.get("fields")).put("dataobjectId",dataobjectId);

                        resultList.add((Map<String,Object>)result.get("fields"));
                    }
                    
                    i = 0;
                    
                    queryStr = "_id:( "; 
                }
            }
            
            //log.info("2222 queryStr="+queryStr);
            
            if ( i > 0 )
            {
                queryStr += " )";

                Map<String,Object> ret = ESUtil.retrieveSimpleDataset(esClient, new String[]{}, new String[]{}, queryStr, null, 1000, 0, selectedFields, sortFields,new String[]{},null);

                List<Map<String,Object>> searchResults = (List<Map<String,Object>>)ret.get("searchResults");

                for(Map<String,Object> result : searchResults)
                {
                    String dataobjectId = (String)result.get("id");
                    dataobjectId = dataobjectId.substring(0, 40);
                    
                    ((Map<String,Object>)result.get("fields")).put("dataobjectId",dataobjectId);
                    
                    resultList.add((Map<String,Object>)result.get("fields"));
                }
            }

            return resultList;
        }
        catch (Exception e)
        {
            String errorInfo = "retrieveVideoObjectData() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e);
            log.error(errorInfo);
                         
            throw e;
        }
    }
        
    public static ByteBuffer retrieveVideoObjectPictureContent(EntityManager em, EntityManager platformEm,int organizationId, int repositoryId, String pictureDataobjectId, int frameTopX, int frameTopY, int frameBottomX, int frameBottomY) throws Exception 
    {
        try
        {
            String contentId = "";
        
            Client esClient = ESUtil.getClient(organizationId,repositoryId,false);
         
            String queryStr = String.format("_id:%s", pictureDataobjectId);          
            
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
            
            Content content = Util.getDataobjectContentInfo(em, platformEm, organizationId, repositoryId, contentId);
            ByteBuffer pictureContent = DataobjectHelper.getDataobjectContentBinaryByContent(organizationId,em,platformEm,repositoryId,content);
          
            return getSubImage(pictureContent,frameTopX,frameTopY,frameBottomX-frameTopX,frameBottomY-frameTopY);
        }
        catch (Exception e)
        {
            String errorInfo = "retrieveVideoObjectPictureContent() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e);
            log.error(errorInfo);
                         
            throw e;
        }
    }

    public static String convertToDBFormat(EntityManager platformEm,int modelId,String featureName,String featureValue)
    {
        VideoObjectFeatureValue fValue = null;
        
        if ( featureValue == null || featureValue.isEmpty() )
            return "0";
        
        try
        {
            String sql = String.format("from VideoObjectFeatureValue where model=%d and featureName='%s' and featureValue='%s'",modelId,featureName,featureValue);
            fValue = (VideoObjectFeatureValue)platformEm.createQuery(sql).getSingleResult();
        }
        catch(Exception e)
        {
            log.error("convertToDBFormat() failed! e="+e+" featureName="+featureName+" featureValue="+featureValue);
            return "0";
        }
        
        return String.valueOf(fValue.getId());
    }
        
    public static ImageSet saveImageDataset(int organizationId,EntityManager em,String imageSetName,String imageSetDescription,String datasetIdAndServiceId)
    {          
        String[] vals = datasetIdAndServiceId.split("\\,");
        
        ImageSet imageSet = new ImageSet();
          
        try
        {
            em.getTransaction().begin();                
       
            imageSet.setOrganizationId(organizationId);
            imageSet.setType(ImageSetType.MALONG_TYPE.getValue());
            imageSet.setName(imageSetName);
            imageSet.setDescription(imageSetDescription);
            imageSet.setKeyId(vals[0]);
            imageSet.setServiceId(vals[1]);
            imageSet.setCreateTime(new Date());
            imageSet.setCreatedBy(0);
            imageSet.setStatusTime(new Date());
            
            em.persist(imageSet);
                      
            em.getTransaction().commit();
        }
        catch(Exception e)
        {
            log.error(" saveImageSet() failed! e="+e+" imageSetName="+imageSetName);
            throw e;
        }
        
        return imageSet;
    }
    
    public static String getImageSetServiceId(EntityManager em,String imageSetName)
    {
        ImageSet imageSet = null;
        
        try
        {
            String sql = String.format("from ImageSet where name='%s'",imageSetName.trim());
            imageSet = (ImageSet)em.createQuery(sql).getSingleResult();
        }
        catch(Exception e)
        {
            log.error(" getDatasetServiceId() failed! e="+e+" imageDatasetName="+imageSetName);
            return "";
        }
        
        return imageSet.getServiceId();
    }
  
    public static ImageSet getImageSet(EntityManager em,String imageSetName)
    {
        ImageSet imageSet = null;
        
        try
        {
            String sql = String.format("from ImageSet where name='%s'",imageSetName.trim());
            imageSet  = (ImageSet)em.createQuery(sql).getSingleResult();
        }
        catch(Exception e)
        {
            log.error(" getImageDataset() failed! e="+e+" imageDatasetName="+imageSetName);
        }
        
        return imageSet;
    }
      /*
     * rectangle contains [x1, y1, x2, y2)
     */
    public static double getOverlappingRateBetweenRectangels(double[] rect1, double[] rect2)
    {
       if (rect1[2] < rect2[0] || rect1[0] > rect2[2] || rect1[3] < rect2[1] || rect1[1] > rect2[3])
           return 0;
        
        double x1 = Math.max(rect1[0], rect2[0]);
        double y1 = Math.max(rect1[1], rect2[1]);
        double x2 = Math.min(rect1[2], rect2[2]);
        double y2 = Math.min(rect1[3], rect2[3]);
        
        double overlapRectArea = (x2 - x1) * (y2 - y1);
        double rectArea1 = (rect1[2] - rect1[0]) * (rect1[3] - rect1[1]);
        double rectArea2 = (rect2[2] - rect2[0]) * (rect2[3] - rect2[1]);
        
        double compareArea = rectArea1<rectArea2?rectArea1:rectArea2; //取小的做比较
        
        return overlapRectArea / compareArea;
    }
     
     public static double getDistanceRateBetweenRectangels(double[] rect1, double[] rect2)
    {
        double x1 = Math.min(rect1[0], rect2[0]);
        double y1 = Math.min(rect1[1], rect2[1]);
        double x2 = Math.max(rect1[2], rect2[2]);
        double y2 = Math.max(rect1[3], rect2[3]);
        
        double totalRectArea = (x2 - x1) * (y2 - y1);
        double rectArea1 = (rect1[2] - rect1[0]) * (rect1[3] - rect1[1]);
        double rectArea2 = (rect2[2] - rect2[0]) * (rect2[3] - rect2[1]);
        
        return (rectArea1 + rectArea2) / totalRectArea;
    }
    
    public static double getOverlappingRateBetweenRectangels1(double[] rect1, double[] rect2)
    {
        if (rect1[2] < rect2[0] || rect1[0] > rect2[2] || rect1[3] < rect2[1] || rect1[1] > rect2[3])
            return 0;
        
        double x1 = Math.max(rect1[0], rect2[0]);
        double y1 = Math.max(rect1[1], rect2[1]);
        double x2 = Math.min(rect1[2], rect2[2]);
        double y2 = Math.min(rect1[3], rect2[3]);
        
        double overlapRectArea = (x2 - x1) * (y2 - y1);
        double rectArea1 = (rect1[2] - rect1[0]) * (rect1[3] - rect1[1]);
        double rectArea2 = (rect2[2] - rect2[0]) * (rect2[3] - rect2[1]);
        
        return overlapRectArea / (rectArea1 + rectArea2 - overlapRectArea);
    }
    
    /*
     * rectangle contains [x1, y1, x2, y2]
     */
    public static double[] findOverlappingRectangel(double[] rect1, double[] rect2)
    {
        double[] ret = {0, 0, 0, 0};
        
        if (rect1[2] < rect2[0] || rect1[0] > rect2[2] || rect1[3] < rect2[1] || rect1[1] > rect2[3])
            return ret;
        
        ret[0] = Math.max(rect1[0], rect2[0]);
        ret[1] = Math.max(rect1[1], rect2[1]);
        ret[2] = Math.min(rect1[2], rect2[2]);
        ret[3] = Math.min(rect1[3], rect2[3]);
        
        return ret;
    }
    
    public static Map<String,Object> getImageInfo(ByteBuffer imageContent) throws Exception
    {
        Map<String,Object> info = new HashMap<>();
        
        ByteArrayInputStream in = new ByteArrayInputStream(imageContent.array());
        BufferedImage image = ImageIO.read(in);
        
        info.put("height",image.getHeight());
        info.put("width",image.getWidth());
     
        return info;
    }
    
    public static ByteBuffer drawObjectOnImage(Color rectColor,Color textColor,String label,ByteBuffer imageContent,int x,int y,int width,int height) throws Exception
    {
        if ( rectColor == null )
            rectColor = Color.BLUE;
        
        if ( textColor == null )
            textColor = Color.WHITE;
        
        ByteArrayInputStream in = new ByteArrayInputStream(imageContent.array());
        BufferedImage image = ImageIO.read(in);
        
        Graphics2D g = (Graphics2D)image.getGraphics();
        
        drawOneBox(image,g,label,x,y,width,height,rectColor,textColor);
                
        ByteArrayOutputStream out=new ByteArrayOutputStream();
        ImageIO.write(image, "jpeg", out);
        
        return ByteBuffer.wrap(out.toByteArray());
    }
    
    public static ByteBuffer drawObjectsOnImage(ByteBuffer imageContent,List<Map<String,Object>> objectInfoList) throws Exception, IOException, IOException
    {              
        ByteArrayInputStream in = new ByteArrayInputStream(imageContent.array());
        BufferedImage image = ImageIO.read(in);

        Graphics2D g = (Graphics2D)image.getGraphics();
            
        for(Map<String,Object> objectInfo : objectInfoList)
        {            
            Color rectColor = (Color)objectInfo.get("rectColor");
            if ( rectColor == null )
                rectColor = Color.BLUE;
            
            Color textColor = (Color)objectInfo.get("textColor");
            if ( textColor == null )
                textColor = Color.WHITE;
            
            String label = (String)objectInfo.get("label");
            int x = (Integer)objectInfo.get("x");
            int y = (Integer)objectInfo.get("y");
            int width = (Integer)objectInfo.get("width");
            int height = (Integer)objectInfo.get("height");
            
            log.info(" 111111 label="+label+" x="+x);
            
            drawOneBox(image,g,label,x,y,width,height,rectColor,textColor);
        }
         
        ByteArrayOutputStream out=new ByteArrayOutputStream();
        ImageIO.write(image, "jpeg", out);
        
        return ByteBuffer.wrap(out.toByteArray());
    }

    private static void drawOneBox(BufferedImage image,Graphics2D graph,String label,int x,int y,int width,int height,Color rectColor,Color textColor) 
    {
        graph.setColor(rectColor);
        graph.setStroke(new BasicStroke(3));
        graph.drawRoundRect(x, y, width, height,5,5); //log.info("x="+x+" y="+y+" width="+width+" height="+height);
        
        if ( label != null && !label.trim().isEmpty() )
        {
            float fontSize = 50f;
            
            int textX = x+width/2-6;
            int textY = y - 5;
            int circleX = x+width/2 - 15;
            int circleY = y - 50;
            
            if ( label.length() > 1 )
            {
                fontSize = 35f;
                textX = x+width/2-17;
                textY = y - 12;
                circleX = x+width/2 - 25;
                circleY = y - 50;
            }
            
            graph.setColor(rectColor);
            graph.fillOval(circleX,circleY,50,50);
            
            graph.setColor(textColor);
            graph.setFont(graph.getFont().deriveFont(fontSize));
            graph.drawString(label, textX, textY);
        }
    
        for(int  y1 = y; y1<y+height; y1++) 
        {
            for(int x1 = x; x1< x+width; x1++) 
            {
                int pixel = image.getRGB(x1, y1);

                int r = (pixel >> 16) & 0xff;
                int g = (pixel >> 8) & 0xff; 
                int b = pixel & 0xff;  
                                
                if ( rectColor == Color.RED || rectColor == Color.YELLOW )
                {
                    r += 50;

                    if ( r > 255 )
                        r=255;
                }
             
                if ( rectColor == Color.GREEN || rectColor == Color.YELLOW )
                {
                    g += 50;

                    if ( g > 255 )
                        g=255;
                }
                
                if ( rectColor == Color.BLUE )
                {
                    b += 50;

                    if ( b > 255 )
                        b=255;
                }
        
                
                if ( rectColor != Color.RED && rectColor != Color.GREEN && rectColor != Color.BLUE && rectColor != Color.YELLOW )
                {
                    b -= 50;

                    if ( b < 0 )
                        b=0;
                    
                    r -= 50;

                    if ( r < 0 )
                        r=0;
                    
                    g -= 50;

                    if ( g < 0 )
                        g=0;
                }
                                
                Color col = new Color(r, g, b);
           
                image.setRGB(x1, y1, col.getRGB());
            }
        }
    }
    
    public static ByteBuffer getSubImage(ByteBuffer imageContent,int x,int y,int width,int height) throws Exception
    {
        ByteArrayInputStream in = new ByteArrayInputStream(imageContent.array());
        BufferedImage image = ImageIO.read(in);
        
        BufferedImage subImage = image.getSubimage(x, y, width, height);
        
        ByteArrayOutputStream out=new ByteArrayOutputStream();
        ImageIO.write(subImage, "jpeg", out);
        
        return ByteBuffer.wrap(out.toByteArray());
    }
           
    public static int getObjectTypeId(List<VideoObjectType> videoObjectTypeList,String objectName)
    {
        try
        {
            for(VideoObjectType type : videoObjectTypeList)
            {
                if ( type.getName().trim().toLowerCase().equals(objectName.trim().toLowerCase()) )
                    return type.getId();
            }
            
            return 0;
        }
        catch(Exception e)
        {
            log.error("getObjectTypeId() failed! e="+e+" objectName="+objectName);
            throw e;
        }
    }
    
    public static int getObjectTypeIdByItem(List<VideoObjectFeatureValue> videoObjectFeatureValueList,String itemName,int modelId)
    {
        try
        {
            for(VideoObjectFeatureValue value : videoObjectFeatureValueList)
            {
                if ( value.getModel() != modelId  || value.getIsEnabled() == 0 )
                    continue;
                    
                String val = value.getFeatureValue().trim().toLowerCase();
                String val1 = itemName.trim().toLowerCase();
                
                if ( val.equals(val1) || val.equals(val1.replace("-", "_")) || val.equals(val1.replace(" ", "_")) )
                    return value.getObjectType();
            }
            
            return 0;
        }
        catch(Exception e)
        {
            log.error("getObjectTypeIdByItem() failed! e="+e+" itemName="+itemName);
            throw e;
        }
    }
    
    public static Map<String,String> getVideoProcessingWorkingDirectory(boolean thisNodeIsAVideoPrcoessingServer,ComputingNode computingNode,String currentWorkingDirKey,String videoServerHost,String videoServerUser,String videoServerPassword)
    {
        Map<String,String> map = new HashMap<>();
        
        String videoServerWorkingDirectoryBase = "/data/share";
        String edfServerWorkingDirectoryBase;

        if ( computingNode.getNodeOsType() == ComputingNodeOSType.Windows.getValue() )
        {
            edfServerWorkingDirectoryBase =  String.format("\\\\%s\\data\\share",videoServerHost);
        }
        else
        {
            if ( !thisNodeIsAVideoPrcoessingServer ) // this is not a video processing node
            {
                String videoServerId = videoServerHost.replace(".", "");
                edfServerWorkingDirectoryBase = String.format("/edf/video/%s",videoServerId);

                File folder = new File(edfServerWorkingDirectoryBase);

                if ( !folder.exists() ) // mount
                {
                    String command = String.format("mount -t cifs -o username='%s',password='%s' //%s/data/share %s",videoServerUser,videoServerPassword,videoServerHost,edfServerWorkingDirectoryBase);
                    RCUtil.executeCommand(command, null, false,null, null,false);
                } 
            }
            else
                edfServerWorkingDirectoryBase = videoServerWorkingDirectoryBase;
        }
    
        String videoServerWorkingDirectory = String.format("%s/%s",videoServerWorkingDirectoryBase,currentWorkingDirKey);
        String edfServerWorkingDirectory = String.format("%s/%s",edfServerWorkingDirectoryBase,currentWorkingDirKey);
        
        map.put("videoServerWorkingDirectory", videoServerWorkingDirectory);
        map.put("edfServerWorkingDirectory", edfServerWorkingDirectory);
        
        return map;
    }
    
    public static ByteBuffer drawPictureObjectFrame(String videoServerHost,String videoServerUser,String videoServerPassword,String videoServerWorkingDirectory,String edfServerWorkingDirectory,String pictureName, ByteBuffer pictureContent, List<Map<String,String>> frameInfo) throws Exception
    {                    
        String pictureFilepathInEdfServer = String.format("%s/%s",edfServerWorkingDirectory,pictureName);
        FileUtil.writeFileFromByteBuffer(pictureFilepathInEdfServer,pictureContent);
        
        String oldPictureFilepathInEdfServer = pictureFilepathInEdfServer;
        
        String pictureFilepathInVideoServer = String.format("%s/%s",videoServerWorkingDirectory,pictureName);
           
        for(Map<String,String> frame : frameInfo)
        {
            String topX = frame.get("top_x");
            String topY = frame.get("top_y");
            String bottomX = frame.get("bottom_x");
            String bottomY = frame.get("bottom_y");
         
            String newName = String.format("%d_%s",new Date().getTime(),pictureName);
            String newPictureFilepathInVideoServer = String.format("%s/%s",videoServerWorkingDirectory,newName);
         
            String command = String.format("python3 /home/edf/devel/drawrect.py %s -l %s -t %s -r %s -b %s -d %s \n",pictureFilepathInVideoServer,topX,topY,bottomX,bottomY,newPictureFilepathInVideoServer);
            Tool.executeSSHCommandChannelShell(videoServerHost,22,videoServerUser,videoServerPassword,command,3,"");    
            
            pictureFilepathInVideoServer = newPictureFilepathInVideoServer;
            pictureFilepathInEdfServer = String.format("%s/%s",edfServerWorkingDirectory,newName);
        }
     
        FileUtils.delete(new File(oldPictureFilepathInEdfServer));
          
        // get new picture file
        
        ByteBuffer buf = ByteBuffer.wrap(FileUtil.readFileToByteArray(new File(pictureFilepathInEdfServer)));
        
        FileUtils.delete(new File(pictureFilepathInEdfServer));
        
        return buf;
    }
    
    public static void saveOneRealtimeImageToRepository(ComputingNode computingNode,DataService.Client dataService,CommonService.Client commonService,int organizationId,int repositoryId,int cameraId,byte[] imageContentStream,long imageCapturedTime) throws Exception
    {
        int sourceApplicationId=0,datasourceType=1,datasourceId=0;

        String filename = String.format("%d.%s",cameraId,imageCapturedTime);        
        log.info("   contentstream="+imageContentStream);
 
        String key = String.format("%d.%d",cameraId,imageCapturedTime);

        String pictureDataobjectId = DataIdentifier.generateDataobjectId(organizationId,sourceApplicationId,datasourceType,datasourceId,key,repositoryId);
      
        Map<String,String> metadata = new HashMap<>();

        metadata.put("VIDEO_PICTURE_VIDEO_EDF_ID","0");
        metadata.put("VIDEO_PICTURE_TIME_IN_VIDEO","0");
        metadata.put("VIDEO_PICTURE_VIDEO_ID","0");

        //metadata.put("VIDEO_PICTURE_PEOPLE_NUMBER",String.valueOf(attributeNumber.get("personNumber"))); 
        //metadata.put("VIDEO_PICTURE_CAR_NUMBER",String.valueOf(attributeNumber.get("carNumber")));  
 
        metadata.put("VIDEO_PICTURE_CAPTURED_TIME",String.valueOf(imageCapturedTime));
        metadata.put("VIDEO_PICTURE_EXTRACTED_TIME",String.valueOf(new Date().getTime()));
        metadata.put("VIDEO_PICTURE_CAMERA_ID",String.format("%d",cameraId));
        //metadata.put("VIDEO_PICTURE_OBJECT_NUMBER",String.valueOf(pictureObjectData.getObjectTypes().size()));

        DataServiceUtil.storeVideoDataobject(CommonKeys.DATAOBJECT_TYPE_FOR_VIDEO_PICTURE,pictureDataobjectId,computingNode.getNodeOsType(),ByteBuffer.wrap(imageContentStream),dataService,commonService,organizationId,sourceApplicationId,datasourceType,datasourceId,filename,filename,repositoryId,false,metadata);
    }
        
    public static void processVideoAnalysisResult(List<VideoObjectType> videoObjectTypeList,List<PictureObjectData> videoObjectData,VideoAnalysisModel model,Client esClient,EntityManager em,EntityManager platformEm,Repository repository,UnstructuredFileProcessingTask task, UnstructuredFileProcessingJob job,String dataobjectId,DataobjectVO dataobjectVO) throws JSONException, Exception 
    {
        String videoId;
        String videoEDFId;
        String videoCaseId;
        String timeInVideoStr;
        String pictureDataobjectId;
        String videoObjectDataobjectTypeName = "video_object_type";
        Date today = new Date();
           
        // process result
        videoEDFId = dataobjectVO.getId().substring(0, 40);
        videoId = dataobjectVO.getMetadatas().get("VIDEO_ID");
        videoCaseId = dataobjectVO.getMetadatas().get("VIDEO_CASE_ID");
        
        String videoCapturedStartTimeStr = dataobjectVO.getMetadatas().get("VIDEO_START_TIME");
        Date videoCapturedStartTime = Tool.convertESDateStringToDate(videoCapturedStartTimeStr);
        Date pictureCapturedTime;
        
        int kkk=0;
        
        for(PictureObjectData pictureObjectData : videoObjectData) // each picture
        {
            kkk++;
            
            log.info(" processing kkk="+kkk+", total="+videoObjectData.size());
            
            // save picture, get video_object_image_id
            int sourceApplicationId = 0;
            int datasourceType = 1; // file server
            int datasourceId = 0;
            
            String filename = pictureObjectData.getPictureFilename();
            String originalSourcePath = pictureObjectData.getPictureFilepath();
        
            timeInVideoStr = Tool.secToTimeStr(pictureObjectData.getTimeInVideoInSeconds());
               
            if ( videoCapturedStartTime.getTime() > 10*3600*1000 ) // > 10小时
                pictureCapturedTime = Tool.dateAddDiff(videoCapturedStartTime, pictureObjectData.getTimeInVideoInSeconds(),Calendar.SECOND);       
            else
                pictureCapturedTime = new Date(0);
            
            pictureObjectData.setCapturedTime(pictureCapturedTime);
            
           /* Map<String,String> metadata = new HashMap<>();
            
            metadata.put("VIDEO_PICTURE_VIDEO_EDF_ID",videoEDFId);
            metadata.put("VIDEO_PICTURE_TIME_IN_VIDEO",timeInVideoStr);
            metadata.put("VIDEO_PICTURE_VIDEO_ID",videoId);
         
            metadata.put("VIDEO_PICTURE_CAPTURED_TIME",String.valueOf(pictureCapturedTime.getTime()));
            
            metadata.put("VIDEO_PICTURE_EXTRACTED_TIME",String.valueOf(new Date().getTime()));
            metadata.put("VIDEO_PICTURE_OBJECT_NUMBER",String.valueOf(pictureObjectData.getObjectTypes().size()));*/
                            
            //byte[] contentStream = FileUtil.readFileToByteArray(new File(pictureObjectData.getPictureFilepath()));
 
           // log.info(" filepath="+pictureObjectData.getPictureFilepath()+" contentstream="+contentStream);
            
            String key = String.format("%s.%d",videoId,pictureObjectData.getTimeInVideoInSeconds());
            
            pictureDataobjectId = DataIdentifier.generateDataobjectId(job.getOrganizationId(),sourceApplicationId,datasourceType,datasourceId,key,job.getRepositoryId());

         /*   Boolean alreadySavedPicture = alreadSavedPictureMap.get(key);
            
            if ( alreadySavedPicture == null )
            {
                DataServiceUtil.storeVideoDataobject(CommonKeys.DATAOBJECT_TYPE_FOR_VIDEO_PICTURE,pictureDataobjectId,computingNode.getNodeOsType(),ByteBuffer.wrap(contentStream),dataService,commonService,job.getOrganizationId(),sourceApplicationId,datasourceType,datasourceId,filename,originalSourcePath,job.getRepositoryId(),false,metadata);
                alreadSavedPictureMap.put(key,true);
            } 
            */
            
            for(int i=0;i<pictureObjectData.getObjectTypes().size();i++) // each object
            {            
                if ( pictureObjectData.getObjectTypes().get(i) == 0 )
                {
                    log.warn(" 666666 object type= 0");
                    continue;
                }
                        
                String cameraId = dataobjectVO.getMetadatas().get("VIDEO_CAMERA_ID");
                String cameraInfo = dataobjectVO.getMetadatas().get("VIDEO_CAMERA_INFO");
                
                //String videoTimeStr = Tool.convertDateToString(pictureObjectData.getVideoTime(),"yyyy-MM-dd HH:mm:ss.S");
              
                key = String.format("%d.%d.%s.%d.%d.%d", job.getOrganizationId(),repository.getId(),videoId,pictureObjectData.getTimeInVideoInSeconds(),model.getId(),i+1);
                
                String videoObjectId = DataIdentifier.generateHash(key);
              
                String indexName = Util.getTargetIndexName1(job.getOrganizationId(),repository.getId(),esClient,em,repository.getDefaultCatalogId(),today,"video_object_type",videoObjectId);
          
                log.debug("11111111111111 indexName="+indexName);
                
                Map<String, Object> jsonMap =  ESUtil.getRecord(esClient,indexName,videoObjectDataobjectTypeName,videoObjectId);
                
                if ( jsonMap == null )
                {
                    jsonMap = new HashMap<>();
                    jsonMap.put("dataobject_type",CommonKeys.DATAOBJECT_TYPE_FOR_VIDEO_OBJECT);
                    jsonMap.put("target_repository_id",repository.getId());
                    jsonMap.put("event_time",pictureObjectData.getVideoTime());
                    jsonMap.put("dataobject_name",key);
                    jsonMap.put("organization_id", job.getOrganizationId());
                    
                    jsonMap.put("VIDEO_OBJECT_VIDEO_ID",videoId);
                    //jsonMap.put("VIDEO_OBJECT_CAPTURED_TIME",pictureObjectData.getVideoTime());
                    jsonMap.put("VIDEO_OBJECT_RECOGNITION_MODEL_ID",model.getId());
                    jsonMap.put("VIDEO_OBJECT_INDEX",i+1);
                    
                    jsonMap.put("VIDEO_OBJECT_FLAGS","");
                }                                    
                
                jsonMap.put("VIDEO_OBJECT_TIME_IN_VIDEO",timeInVideoStr);
                jsonMap.put("VIDEO_OBJECT_TYPE",pictureObjectData.getObjectTypes().get(i));
                jsonMap.put("VIDEO_OBJECT_POSITION_TOP_X",pictureObjectData.getObjectPositions().get(i).get("top_x"));
                jsonMap.put("VIDEO_OBJECT_POSITION_TOP_Y",pictureObjectData.getObjectPositions().get(i).get("top_y"));
                jsonMap.put("VIDEO_OBJECT_POSITION_BOTTOM_X",pictureObjectData.getObjectPositions().get(i).get("bottom_x"));
                jsonMap.put("VIDEO_OBJECT_POSITION_BOTTOM_Y",pictureObjectData.getObjectPositions().get(i).get("bottom_y"));
               
                jsonMap.put("VIDEO_OBJECT_EXTRACTED_TIME",today);
                jsonMap.put("VIDEO_OBJECT_CAPTURED_TIME",pictureCapturedTime);
                jsonMap.put("VIDEO_OBJECT_CAMERA_ID",cameraId);
                jsonMap.put("VIDEO_OBJECT_CAMERA_INFO",cameraInfo);
                
                jsonMap.put("VIDEO_OBJECT_VIDEO_EDF_ID",videoEDFId);
                jsonMap.put("VIDEO_OBJECT_CASE_ID",videoCaseId);
                jsonMap.put("VIDEO_OBJECT_PICTURE_EDF_ID",pictureDataobjectId);
                //jsonMap.put("VIDEO_OBJECT_GEO_LOCATION",null);
                                                          
                jsonMap.put("object_status",DataobjectStatus.UNSYNCHRONIZED.getValue());    /////////////////////
                jsonMap.put("object_status_time",new Date());
                jsonMap.put("last_stored_time",new Date());
                
                Map<String,String> features = pictureObjectData.getObjectFeatures().get(i);
                               
                List<String> featureList = new ArrayList<>();
                
                VideoObjectType videoObjectType = getVideoObjectType(pictureObjectData.getObjectTypes().get(i),videoObjectTypeList);
                
                if ( videoObjectType.getIsSingleFeatureValue() == 1 )
                {
                    for( Map.Entry<String,String> entry : features.entrySet() )
                    {
                        String featureNameIndex = entry.getKey();
                        String featureValue = entry.getValue();
                        
                        if ( featureValue == null || featureValue.equals("null") )
                            featureValue = "0";

                        String featureStr = String.format("%s-%s~%s",pictureObjectData.getObjectTypes().get(i),featureNameIndex,featureValue);
                        featureList.add(featureStr);
                    }
                }
                else
                {
                    String featureStr = String.format("%s-",pictureObjectData.getObjectTypes().get(i));
                    
                    for( Map.Entry<String,String> entry : features.entrySet() )
                    {
                        String featureNameIndex = entry.getKey();
                        String featureValue = entry.getValue();

                        if ( featureValue == null || featureValue.equals("null") )
                            featureValue = "0";

                        featureStr += String.format("%s~%s-",featureNameIndex,featureValue);
                    }
                    
                    featureStr = Tool.removeLastChars(featureStr, 1);
                        
                    featureList.add(featureStr);
                }
                               
                jsonMap.put("VIDEO_OBJECT_FEATURES",featureList);
      
                int retry = 0;
                
                while(true)
                {
                    retry++;
                    
                    try
                    {
                        ESUtil.persistRecord(esClient,indexName,videoObjectDataobjectTypeName,videoObjectId,jsonMap,0);
                        break;
                    }
                    catch(Exception ee)
                    {
                        if ( retry > 10 )
                            throw ee;
                        
                        log.info(" persiste record failed! e="+ee+" stacktrace="+ExceptionUtils.getStackTrace(ee));
                        
                        if ( ee instanceof TypeMissingException || ee instanceof StrictDynamicMappingException )
                        {
                            DataobjectType dataobjectType = Util.getDataobjectTypeByIndexTypeName(platformEm, videoObjectDataobjectTypeName); // get new dataobjectType
                                        
                            XContentBuilder typeMapping = ESUtil.createTypeMapping(platformEm, dataobjectType, true);
                            ESUtil.putMappingToIndex(esClient, indexName, videoObjectDataobjectTypeName, typeMapping);
                            continue;
                        }
                        
                        throw ee;
                    }
                }
            }
        }
        //ESUtil.addOrupdateDocument(em,platformEm,job.getOrganizationId(),dataobjectId,0,jsonObject,job.getDataobjectType(),job.getRepositoryId());
    }
     
    public static VideoObjectType getVideoObjectType(int videoObjectTypeId,List<VideoObjectType> videoObjectTypeList)
    {
        for(VideoObjectType type : videoObjectTypeList)
        {
            if ( type.getId() == videoObjectTypeId)
                return type;
        }
        
        log.info(" 9999999 videoObjectTypeId not found ="+videoObjectTypeId);
        
        return null;
    }
      
    public static String preProcessVideo(DataService.Client dataService,CommonService.Client commonService,int organizationId,int repositoryId,boolean thisNodeIsAVideoPrcoessingServer,DataobjectVO dataobjectVO,ByteBuffer fileBuf,EntityManager platformEm, int videoProcessingServiceInstanceId,int videoToImageFrequencyType,ComputingNode computingNode,List<Lock> gpuLockList,int videoStartTimeInSecond,int videoLength) throws Exception
    {
        String command;
        String videoServerHost;
        int videoLengthInSeconds = 0;

        String framePerSecondStr = VideoToImageFrequencyType.findByValue(videoToImageFrequencyType).getFpsStr();
        float secondsBetweenTwoFrame = VideoToImageFrequencyType.findByValue(videoToImageFrequencyType).getSecondsBetweenTwoFrame();
                
        Map<String,String> map = Util.getEDFVideoProcessingService(platformEm,String.valueOf(videoProcessingServiceInstanceId));
 
        if ( thisNodeIsAVideoPrcoessingServer )
            videoServerHost = computingNode.getInternalIP();
        else
            videoServerHost = Util.pickOneServiceIP(map.get("ips"));
        
        String videoServerUser = map.get("user");
        String videoServerPassword = map.get("password");
                 
        String currentWorkingDirKey = UUID.randomUUID().toString();
        //String currentWorkingDirKey = "aaa";
        
        Map<String,String> map1 = VideoProcessUtil.getVideoProcessingWorkingDirectory(thisNodeIsAVideoPrcoessingServer,computingNode, currentWorkingDirKey,videoServerHost,videoServerUser,videoServerPassword);

        String videoServerWorkingDirectory = map1.get("videoServerWorkingDirectory");
        String edfServerWorkingDirectory =  map1.get("edfServerWorkingDirectory");
        
        // mkdir
        File folder = new File(edfServerWorkingDirectory);
        boolean created = folder.mkdir();
        log.info(" create folder "+edfServerWorkingDirectory+" created="+created);
        
        if ( thisNodeIsAVideoPrcoessingServer )
        {
            folder.setWritable(true,false);
            FileUtil.chmodFullPermission(edfServerWorkingDirectory);
        }
  
        String filename = dataobjectVO.getMetadatas().get("file_name");
        filename = filename.replace(" ", "_");
        filename = String.format("%d_%s",videoStartTimeInSecond,filename);
        
        // copy file to shared storage
        String videoFilepathInEDFServer = String.format("%s/%s",edfServerWorkingDirectory,filename);
        String videoFilepathInVideoServer = String.format("%s/%s",videoServerWorkingDirectory,filename); 
                    
        //String newVideoFileInEDFServer = String.format("%s/new_%s",edfServerWorkingDirectory,filename);
        //String newVideoFileInVideoServer = String.format("%s/new_%s",videoServerWorkingDirectory,filename);
        
        FileUtil.writeFileFromByteBuffer(videoFilepathInEDFServer,fileBuf);
        videoLengthInSeconds = VideoProcessUtil.getMediaDurationInSecond(videoFilepathInEDFServer);
        
        /*if ( videoLengthInSeconds >= videoLength )
        {            
            String commandStr = "nohup ffmpeg -y -ss "+videoStartTimeInSecond+" -t "+ videoLength+ " "+" -i " + videoFilepathInVideoServer +" -c:a aac -strict experimental -b:a 98k " + newVideoFileInVideoServer +" >output.out 2>error.out </dev/null& \n" ;
            Tool.executeSSHCommandChannelShell(videoServerHost,22,videoServerUser,videoServerPassword,commandStr,1,"");

            Tool.SleepAWhile(60, 0);
            
            int retry = 0;
            long oldLen = 0;
            
            while(true)
            {
                retry++;

                File file = new File(newVideoFileInEDFServer);

                if ( !file.exists() )
                {
                    log.info(" check new video file not exsits! file="+newVideoFileInEDFServer);
                    Tool.SleepAWhile(3, 0);

                    if ( retry > 30*20 ) // > 10 mins
                        throw new Exception(" check new video file file not exists! newVideoFileInEDFServer="+newVideoFileInEDFServer+" command="+commandStr);

                    continue;
                }
                
                long len = file.length();
                
                log.info("retry="+retry+" new file.length() = "+len+" old size="+oldLen+" newVideoFileInEDFServer="+newVideoFileInEDFServer);
                
                if ( oldLen == len )
                {
                    log.info(" no size change, done!");
                    Tool.SleepAWhile(1, 0);
                    break;
                }

                Tool.SleepAWhile(30, 0);
                
                oldLen = len;
            }
                        
            FileUtil.removeFile(videoFilepathInEDFServer);
            boolean isDone = new File(newVideoFileInEDFServer).renameTo(new File(videoFilepathInEDFServer));
            log.info(" isDone="+isDone);
            
            //fileBuf = FileUtil.readFileToByteBuffer(new File(newVideoFileInEDFServer));
            //FileUtil.writeFileFromByteBuffer(videoFilepathInEDFServer,fileBuf);
        }*/
   
        try
        {
            // extract object

            // convert video to image
            //command = String.format("python3 /home/edf/devel/video2images.py -fps %.2f -odir %s/img -oseq 4 -oname Image -oext jpg %s \n",framePerSecond,videoServerWorkingDirectory,videoFilepathInVideoServer);  
            //command = String.format("python /var/project/edf/utils/video2images.py -fps %.2f -odir %s/img -oseq 4 -oname Image -oext jpg %s \n",framePerSecond,videoServerWorkingDirectory,videoFilepathInVideoServer);          
            command = String.format("nohup python /var/project/edf/utils/video2images.py -ss %d -t %d -s %s -d %s/img %s >output.out 2>error.out </dev/null& \n",videoStartTimeInSecond,videoLength,framePerSecondStr,videoServerWorkingDirectory,videoFilepathInVideoServer);                     
            Tool.executeSSHCommandChannelShell(videoServerHost,22,videoServerUser,videoServerPassword,command,1,"");

            // object detect
            String imgFolder = String.format("%s/img",edfServerWorkingDirectory);
            
            int retry = 0;
            int oldSize = 0;
            while(true)
            {               
                retry++;
                Tool.SleepAWhile(3, 0);
                
                List<String> filepathList = new ArrayList<>();
                FileUtil.getFolderAllFiles(new File(imgFolder),filepathList);
                            
                log.info(" retry="+retry+",oldSize="+oldSize+", check folder file size="+filepathList.size());
                
                if ( oldSize == filepathList.size() )
                {
                    log.info(" no more new image!");
                    Tool.SleepAWhile(5, 0);
                    break;
                }
                
                oldSize = filepathList.size();
            }

            if ( oldSize == 0 )
                throw new DataProcessingException("video to image failed!"+command,"video_process_error.video_to_images_failed");

            // check if result csv file is there
            String videoObjectExtractionObjectDetectFilename = String.format("%s/object-detect-result.txt",edfServerWorkingDirectory);

            boolean needToReRun;
            
            int i = -1;
            
            while(true)
            {
                needToReRun = false;
            
                i++;
                
                if ( i == gpuLockList.size() )
                    i = 0;
                
                Lock lock = gpuLockList.get(i);
                
                if ( lock.tryLock(1,TimeUnit.SECONDS) )
                {
                    log.info(" get lock suceeded! gpu id ="+i);
                
                    try
                    {
                        int gpuId = i;
                        String gpuIndicator = String.format("CUDA_VISIBLE_DEVICES=%d",gpuId);
                        
                        //command = String.format("nohup python3 /home/edf/devel/detectobjects.py -i %s/img -threshold 0.8 -size %d -o %s/object-detect-result.txt >output.out 2>error.out </dev/null& \n",videoServerWorkingDirectory,minObjectSize,videoServerWorkingDirectory);           
                        //command = String.format("nohup python /var/project/edf/object_detect/detectobjects.py -i %s/img -threshold 0.8 -size %d -o %s/object-detect-result.txt >output.out 2>error.out </dev/null& \n",videoServerWorkingDirectory,minObjectSize,videoServerWorkingDirectory);
                        command = String.format("%s nohup python /var/project/edf/object_detect/detectobjects.py -i %s/img -o %s/object-detect-result.txt >output.out 2>error.out </dev/null& \n",gpuIndicator,videoServerWorkingDirectory,videoServerWorkingDirectory);
                        Tool.executeSSHCommandChannelShell(videoServerHost,22,videoServerUser,videoServerPassword,command,1,"");

                        retry = 0;
                        while(true)
                        {
                            retry++;

                            File file = new File(videoObjectExtractionObjectDetectFilename+".error");
                            if ( file.exists() )
                            {
                                log.info(" exist object-detect-result.txt.error file! ");
                                Tool.SleepAWhile(3, 0);
                                FileUtil.removeFile(videoObjectExtractionObjectDetectFilename+".error");
                                needToReRun = true;
                                break;
                            }
                            
                            file = new File(videoObjectExtractionObjectDetectFilename+".done");

                            if ( !file.exists() )
                            {
                                log.info(" check object detect result file! not exsits! file="+videoObjectExtractionObjectDetectFilename+".done");
                                Tool.SleepAWhile(3, 0);

                                //if ( retry > 30*20 ) // > 10 mins
                                //    throw new Exception("object detect result file not exists! videoObjectExtractionResultFilename="+videoObjectExtractionObjectDetectFilename+".done"+" command="+command);
                                
                                if ( retry > 30*20 )
                                    throw new DataProcessingException("object detect result file not exists! videoObjectExtractionResultFilename="+videoObjectExtractionObjectDetectFilename+".done"+" command="+command,"video_process_error.object_detection_failed");

                                continue;
                            }

                            Tool.SleepAWhile(1, 0);
                            break;
                        }

                        if ( needToReRun )
                            continue;
                    
                        break;
                    }
                    catch(Exception e)
                    {
                        log.info(" process objectdetect failed! e="+e);
                        throw new DataProcessingException(" process objectdetect failed! e="+e,"video_process_error.object_detection_failed");

                        //throw e;
                    }
                    finally
                    {
                        lock.unlock();
                    }
                }
                else
                {
                    log.info(" try to get lock failed! gpu id="+i);
                }
            }
            
            File file = new File(videoObjectExtractionObjectDetectFilename);

            if ( !file.exists() )
            {
                log.error(" check object detect result file! not exsits! file="+videoObjectExtractionObjectDetectFilename);             
                throw new Exception("object detect result file not exists! videoObjectExtractionResultFilename="+videoObjectExtractionObjectDetectFilename+"ssh command="+command);
            }
            
            List<String> filepathList = new ArrayList<>();
 
            FileUtil.getAllFilesWithSamePrefix(new File(imgFolder), "Image", filepathList);

            int sourceApplicationId = 0;
            int datasourceType = 1; // file server
            int datasourceId = 0;

            String videoEDFId = dataobjectVO.getId().substring(0, 40);
            String videoId = dataobjectVO.getMetadatas().get("VIDEO_ID");
            String cameraId = dataobjectVO.getMetadatas().get("VIDEO_CAMERA_ID");

            String videoCapturedTimeStr = dataobjectVO.getMetadatas().get("VIDEO_START_TIME");
            Date videoCapturedTime = Tool.convertESDateStringToDate(videoCapturedTimeStr);
            Date pictureCapturedTime;

            // save image to edf
            for(String filepath : filepathList)
            {
                byte[] contentStream = FileUtil.readFileToByteArray(new File(filepath));

                log.info(" filepath="+filepath+" contentstream="+contentStream);

                int timeInVideoInSeconds = videoStartTimeInSecond+VideoProcessUtil.getTimeInVideoFromPictureName(filepath,(int)secondsBetweenTwoFrame);

                String key = String.format("%s.%d",videoId,timeInVideoInSeconds);

                String pictureDataobjectId = DataIdentifier.generateDataobjectId(organizationId,sourceApplicationId,datasourceType,datasourceId,key,repositoryId);
                String fileName = filepath.substring(filepath.length()-14);
                Map<String,Object> attributeNumber = getAttributeNumber(edfServerWorkingDirectory,fileName);

                Map<String,String> metadata = new HashMap<>();

                metadata.put("VIDEO_PICTURE_VIDEO_EDF_ID",videoEDFId);
                metadata.put("VIDEO_PICTURE_TIME_IN_VIDEO",String.format("%d",timeInVideoInSeconds));
                metadata.put("VIDEO_PICTURE_VIDEO_ID",videoId);            

                metadata.put("VIDEO_PICTURE_PEOPLE_NUMBER",String.valueOf(attributeNumber.get("personNumber"))); 
                metadata.put("VIDEO_PICTURE_CAR_NUMBER",String.valueOf(attributeNumber.get("carNumber")));  

                if ( videoCapturedTime.getTime() > 10*3600*1000 ) // > 10小时
                    pictureCapturedTime = Tool.dateAddDiff(videoCapturedTime, timeInVideoInSeconds ,Calendar.SECOND);       
                else
                    pictureCapturedTime = new Date(0);

                metadata.put("VIDEO_PICTURE_CAPTURED_TIME",String.valueOf(pictureCapturedTime.getTime()));

                metadata.put("VIDEO_PICTURE_EXTRACTED_TIME",String.valueOf(new Date().getTime()));
                metadata.put("VIDEO_PICTURE_CAMERA_ID",cameraId);
                //metadata.put("VIDEO_PICTURE_OBJECT_NUMBER",String.valueOf(pictureObjectData.getObjectTypes().size()));

                DataServiceUtil.storeVideoDataobject(CommonKeys.DATAOBJECT_TYPE_FOR_VIDEO_PICTURE,pictureDataobjectId,computingNode.getNodeOsType(),ByteBuffer.wrap(contentStream),dataService,commonService,organizationId,sourceApplicationId,datasourceType,datasourceId,filename,filepath,repositoryId,false,metadata);
            }            

            //videoLengthInSeconds = ((int)secondsBetweenTwoFrame)*(filepathList.size()+1);

            return String.format("%s~%s~%s~%d~%d",videoServerHost,currentWorkingDirKey,edfServerWorkingDirectory,videoLengthInSeconds,filepathList.size());
        }
        catch(Exception e)
        {
            log.error(" preProcessVideo failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
            
            try
            {
                //org.apache.commons.io.FileUtils.deleteDirectory(folder);
            }
            catch(Exception ee)
            {
                log.warn(" delete "+edfServerWorkingDirectory+" failed!");
            }
            
            throw e;
        }    
    }
    
    public static Map<String,Object> getAttributeNumber(String filepath,String fileName) throws IOException{
        Map<String,Object> result = new HashMap();
        
        String featureResultFile = "object-detect-result.txt";
        String videoObjectExtractionAttributeFilename = String.format("%s/%s",filepath,featureResultFile);
        List<String> rows;
        try {
            
            rows = FileUtil.readTxtFileToStringList(videoObjectExtractionAttributeFilename, EncodingType.UTF_8.getValue(), 1, 0,true);
            if ( rows.isEmpty() )
            {
              result.put("carNumber", 0);
              result.put("personNumber", 0);
            }
            int personNumber = 0;
            int carNumber = 0;
            for(String row:rows)
            {
                String[] columns = row.split("\\,");
                String pictureFilename = columns[0];
                String object = columns[2];
                
                if ( pictureFilename.contains("/") )
                {
                    pictureFilename = pictureFilename.substring(pictureFilename.lastIndexOf("/")+1);
                }
                // 
                if ( fileName.equals(pictureFilename) && !pictureFilename.isEmpty() )
                {    
                     if(object.equals("person"))
                     {
                         personNumber++;
                     }
                     else if(object.equals("car"))
                     {
                        carNumber++;
                     }
                }
            }
            result.put("carNumber", carNumber);
            result.put("personNumber", personNumber);
        } catch (IOException ex) {
            throw ex;
        }
        return result;
    }
    public static String copyFileToVideoServer(String currentWorkingDirKey,String videoServerHost,Map<String,String> videoProcessingServicemap,boolean thisNodeIsAVideoPrcoessingServer,String filename,ByteBuffer fileBuf,EntityManager platformEm, int videoProcessingServiceInstanceId,ComputingNode computingNode) throws Exception
    {
        String videoServerUser = videoProcessingServicemap.get("user");
        String videoServerPassword = videoProcessingServicemap.get("password");
                 
        //String currentWorkingDirKey = c;
    
        Map<String,String> map1 = VideoProcessUtil.getVideoProcessingWorkingDirectory(thisNodeIsAVideoPrcoessingServer, computingNode, currentWorkingDirKey,videoServerHost,videoServerUser,videoServerPassword);

        String edfServerWorkingDirectory =  map1.get("edfServerWorkingDirectory");
     
        //FileUtil.chmodFullPermission(edfServerWorkingDirectory);
  
        // copy file to shared storage
        String videoFilepathInEDFServer = String.format("%s/%s",edfServerWorkingDirectory,filename);
        
        try
        {
            FileUtil.writeFileFromByteBuffer(videoFilepathInEDFServer,fileBuf);
        }
        catch(Exception e)
        {
            if ( e instanceof IOException )
            {
                // mkdir
                File folder = new File(edfServerWorkingDirectory);
                boolean created = folder.mkdir();
                log.info(" create folder "+edfServerWorkingDirectory+" created="+created);

                FileUtil.writeFileFromByteBuffer(videoFilepathInEDFServer,fileBuf);
            }
            else
                throw e;
        }
               
        return String.format("%s~%s",videoServerHost,currentWorkingDirKey);
    }
      
    public static int getTimeInVideoFromPictureName(String filename,int secondsBetweenTwoFrame)
    {
        int timeInVideoInSeconds = -1;
        
        try
        {            
            int indexOfPicture = Integer.parseInt(filename.substring(filename.lastIndexOf("-")+1,filename.lastIndexOf(".")));
            timeInVideoInSeconds = (indexOfPicture-1)*secondsBetweenTwoFrame;
        }
        catch(Exception e)
        {
        }
        
        return timeInVideoInSeconds;
    }
     
    public long getMediaDuration(String mediaFileUNCPath) 
    {
        String outputFilename = String.format("%s/%d.mediainfo",Tool.getSystemTmpdir(),new Date().getTime());
        String commandStr = String.format("ffmpeg -i %s",mediaFileUNCPath);

        RCUtil.executeCommand(commandStr, null, false,outputFilename, null,false);

        String fileInfo = FileUtil.readTextFileToString(outputFilename);
        
        int position = fileInfo.indexOf("Duration:"); 
        String time  =  fileInfo.substring(position+10,position+10+8);
        
        int hour = Integer.parseInt(time.substring(0,2));
        int minute = Integer.parseInt(time.substring(3,5));
        int second = Integer.parseInt(time.substring(6,8));
        
        return hour*3600+minute*60+second;
    }
  
    public static ByteBuffer mergeImages(List<byte[]> pics) 
    {    
        int len = pics.size();  //图片文件个数
        if (len < 1) 
            return null;  
      
        BufferedImage[] images = new BufferedImage[len];  
        int[][] ImageArrays = new int[len][];  
        
        for (int i = 0; i < len; i++) 
        {  
            try 
            {  
                ByteArrayInputStream in = new ByteArrayInputStream(pics.get(i));
                images[i] = ImageIO.read(in);  
            } 
            catch (Exception e) 
            {  
                return null;
            }  
            
            int width = images[i].getWidth();  
            int height = images[i].getHeight();  
            
            ImageArrays[i] = new int[width * height];// 从图片中读取RGB   
            ImageArrays[i] = images[i].getRGB(0, 0, width, height, ImageArrays[i], 0, width);  
        }  
  
        int dst_height = 0;  
        int dst_width = images[0].getWidth();  
        for (int i = 0; i < images.length; i++) 
        {  
            dst_width = dst_width > images[i].getWidth() ? dst_width  
                    : images[i].getWidth();  
  
            dst_height += images[i].getHeight();  
        }  
        
        dst_height+=200;
        dst_width +=200;
        
        System.out.println(dst_width);  
        System.out.println(dst_height);  
        
        if (dst_height < 1) {  
            System.out.println("dst_height < 1");  
            return null;  
        }  
  
        // 生成新图片   
        try 
        {  
            // dst_width = images[0].getWidth();   
            BufferedImage ImageNew = new BufferedImage(dst_width, dst_height,  
                    BufferedImage.TYPE_INT_RGB);  
            int height_i = 0;  
            for (int i = 0; i < images.length; i++) {  
                ImageNew.setRGB(0, height_i, dst_width, images[i].getHeight(),  
                        ImageArrays[i], 0, dst_width);  
                height_i += images[i].getHeight();  
            }  
   
            ByteArrayOutputStream out=new ByteArrayOutputStream();
            ImageIO.write(ImageNew, "jpeg", out);
                    
            return ByteBuffer.wrap(out.toByteArray());  
        
        } catch (Exception e) {  
            log.error(" mergeImages() failed! e="+e.toString()+" statctrace="+ExceptionUtils.getStackTrace(e));
            return null;  
        }  
    }  
}
