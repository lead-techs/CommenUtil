/*
 * GaitRecognitionProcessor.java
 *
 */

package com.broaddata.common.modelprocessor;
 
import com.broaddata.common.exception.DataProcessingException;
import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.Date;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.json.JSONObject;
 
import com.broaddata.common.model.organization.GaitData;
import com.broaddata.common.model.organization.GaitRepository;
import com.broaddata.common.model.organization.Repository;
import com.broaddata.common.model.platform.ComputingNode;
import com.broaddata.common.model.platform.VideoAnalysisModel;
import com.broaddata.common.model.vo.DataobjectVO;
import com.broaddata.common.model.vo.PictureObjectData;
import com.broaddata.common.util.DataIdentifier;
import com.broaddata.common.util.FileUtil;
import com.broaddata.common.util.RCUtil;
import com.broaddata.common.util.Tool;
import com.broaddata.common.util.Util;
import com.broaddata.common.util.VideoProcessUtil;

import static com.broaddata.common.util.VideoProcessUtil.getTimeInVideoFromPictureName;
import java.util.Iterator;
import org.json.JSONArray;

public class GaitRecognitionProcessor extends ModelProcessor
{
    static final Logger log = Logger.getLogger("GaitRecognitionProcessor");
 
    private int gaitVideoPeriodSeconds;
    
    private ComputingNode computingNode;
    private VideoAnalysisModel model;
    private EntityManager em;
    private EntityManager platformEm;
    private float secondsBetweenTwoFrame;
    private String videoServerHost;
    private String currentWorkingDirKey;
    private String videoProcessingServiceInstanceId;
    private boolean thisNodeIsAVideoPrcoessingServer;
    private Repository repository;
    private String[] gaitRepositoryNames;
    private int[] gaitRepositoryIds;
    private String videoId;
    private String timeInVideoStr;
    private DataobjectVO dataobjectVO;
    private String videoLengthInSeconds;
    private int videoStartTimeInSecond;
    private int videoLength;
    
    public void init(Map<String,Object> parameters) throws Exception
    {
        computingNode = (ComputingNode)parameters.get("computingNode");
        model = (VideoAnalysisModel)parameters.get("model");
        em = (EntityManager)parameters.get("em");
        platformEm = (EntityManager)parameters.get("platformEm");
        secondsBetweenTwoFrame = (Float)parameters.get("secondsBetweenTwoFrame");           
        currentWorkingDirKey = (String)parameters.get("currentWorkingDirKey"); 
        videoServerHost = (String)parameters.get("videoServerHost");
        videoProcessingServiceInstanceId = (String)parameters.get("videoProcessingServiceInstanceId");
        thisNodeIsAVideoPrcoessingServer = (Boolean)parameters.get("thisNodeIsAVideoPrcoessingServer");
        repository = (Repository)parameters.get("repository");
        dataobjectVO = (DataobjectVO)parameters.get("dataobjectVO");
         
        gaitRepositoryNames = (String[])parameters.get("gaitRepositoryNames");
        videoLengthInSeconds = (String)parameters.get("videoLengthInSeconds");
         
        videoStartTimeInSecond = (Integer)parameters.get("videoStartTimeInSecond");
        videoLength = (Integer)parameters.get("videoLength");
    }
    
    @Override
    public Map<String,Object> execute() throws Exception
    {
        log.info(" run gait recogonition model!   ");
        
        String restFulAPIPattern = model.getConfig();
        
        em.getTransaction().begin();
         
        Map<String,Object> result = new HashMap<>();
                
        Map<String,String> map = Util.getEDFVideoProcessingService(platformEm,videoProcessingServiceInstanceId);
 
        //String videoServerHost = Util.pickOneServiceIP(map.get("ips"));
        String videoServerUser = map.get("user");
        String videoServerPassword = map.get("password");
        
       /* map = Util.getEDFVideoProcessingService(platformEm,model.getObjectRecognitionServiceInstanceId());
 
        String host = Util.pickOneServiceIP(map.get("ips"));
        String keyId=map.get("keyId");
        String secretKey = map.get("secretKey");
        String version = map.get("version");*/
                   
        Map<String,String> videoProcessingServicemap = Util.getEDFVideoProcessingService(platformEm,videoProcessingServiceInstanceId); 
            
        Map<String,String> map1 = VideoProcessUtil.getVideoProcessingWorkingDirectory(thisNodeIsAVideoPrcoessingServer,computingNode, currentWorkingDirKey,videoServerHost,videoServerUser,videoServerPassword);
 
        String edfServerWorkingDirectory =  map1.get("edfServerWorkingDirectory");
        String videoServerWorkingDirectory = map1.get("videoServerWorkingDirectory");
        
        gaitRepositoryIds = new int[gaitRepositoryNames.length];
        for(int i=0;i<gaitRepositoryNames.length;i++)
        {
            String sql = String.format("from GaitRepository where name='%s'",gaitRepositoryNames[i]);
            GaitRepository gaitRepository = (GaitRepository)em.createQuery(sql).getSingleResult();
            gaitRepositoryIds[i] = gaitRepository.getId();
        }
        
        videoId = dataobjectVO.getMetadatas().get("VIDEO_ID");
         
        // 从视频生成 小段视频 按 10秒一段
        String filename = dataobjectVO.getMetadatas().get("file_name");
        filename = filename.replace(" ", "_");
       
        String videoFilepathInVideoServer = String.format("%s/%s",videoServerWorkingDirectory,filename); 
         
        int startTimeInSeconds = 0;
        
        File folder = new File(edfServerWorkingDirectory+"/video");
        folder.mkdir();
        
        if ( secondsBetweenTwoFrame >= 5 )
            gaitVideoPeriodSeconds = (int)secondsBetweenTwoFrame;
        else
            gaitVideoPeriodSeconds = 4; // min is 4 seconds
        
        for(;;)
        {
            int endTimeInSeconds = startTimeInSeconds + gaitVideoPeriodSeconds;
            
            String newVideoFile = String.format("%s/video/video_%04d.mp4",videoServerWorkingDirectory,startTimeInSeconds); 
       
            try
            {
                String startDate = Tool.secToTimeStr(startTimeInSeconds);
                //String commandStr = "ffmpeg -y  -ss "+startDate+" -t "+(endTimeInSeconds-startTimeInSeconds)+" "+" -i " + videoFilepathInVideoServer +" -vcodec copy -acodec copy -c:a aac -strict -2" + newVideoFile +" \n" ; 
                String commandStr = "ffmpeg -y  -ss "+startDate+" -t "+ gaitVideoPeriodSeconds+ " "+" -i " + videoFilepathInVideoServer +" -c:a aac -strict experimental -b:a 98k " + newVideoFile +" \n" ; 
                      
                Tool.executeSSHCommandChannelShell(videoServerHost,22,videoServerUser,videoServerPassword,commandStr,1,"");
            }
            catch(Exception e)
            {
                log.error(" create new file failed! newVideoFile="+newVideoFile);
                break;
            }
 
            startTimeInSeconds = endTimeInSeconds;
            
            if ( startTimeInSeconds >= Integer.parseInt(videoLengthInSeconds) )
                break;
        }
 
        List<String> filepathList = new ArrayList<>();
        String videoFolder = String.format("%s/video",edfServerWorkingDirectory);
        
        FileUtil.getAllFilesWithSamePrefix(new File(videoFolder), "video_", filepathList);
        
        List<PictureObjectData> videoObjectData = new ArrayList<>();
        Map<String,List<Map<String, String>>> objectListMap = new HashMap<>();
                 
        for(String videoFilepath : filepathList)
        {
            String videoFilename = videoFilepath.replaceAll("\\\\", "/");
            videoFilename = videoFilepath.substring(videoFilename.lastIndexOf("/")+1);
            String videoFilenameKey = videoFilename.substring(0, videoFilename.indexOf("."));
   
            log.info(" videoFilepath="+videoFilename+" videoFilepath="+videoFilepath);
            
            String videoFilepath1 = String.format("%s/video/%s",videoServerWorkingDirectory,videoFilename);

            // 取第一张图片
            String pictureFileName = String.format("p_%s",videoFilenameKey);
            String pictureFileName1 = String.format("%s/video/%s.jpg",videoServerWorkingDirectory,pictureFileName); 
            String commandStr = "ffmpeg -i " + videoFilepath1 +" -y -f image2 -ss 0:0:0 -vframes 1 "+ pictureFileName1+" \n"; 

            Tool.executeSSHCommandChannelShell(videoServerHost,22,videoServerUser,videoServerPassword,commandStr,1,"");
                   
            // 对象检测
            String pictureFileName2 = String.format("%s/video/%s.jpg",edfServerWorkingDirectory,pictureFileName); 
            ByteBuffer fileBuf2 = FileUtil.readFileToByteBuffer(new File(pictureFileName2));
                          
            if ( fileBuf2 == null )
            {
                log.warn(" wrong picture! pictureFileName2="+pictureFileName2+" videoFilepath1="+videoFilepath1); 
                continue;
            }
            
            List<Map<String, String>> objectList = detectObjects(videoServerHost,videoProcessingServicemap,"person",fileBuf2, null);
                
            objectListMap.put(videoFilepath,objectList);
        }
                    
        for(Map.Entry<String,List<Map<String, String>>> entry : objectListMap.entrySet() )
        {
            String videoFilepath = entry.getKey();
            String videoFilename = videoFilepath.replaceAll("\\\\", "/");
            videoFilename = videoFilepath.substring(videoFilename.lastIndexOf("/")+1);
            String videoFilenameKey = videoFilename.substring(0, videoFilename.indexOf("."));
          
            String videoFilepath1 = String.format("%s/video/%s.mp4",videoServerWorkingDirectory,videoFilenameKey);
            String videoFilepath2 = String.format("%s/video/%s.mp4",edfServerWorkingDirectory,videoFilenameKey);               
            
            File file = new File(videoFilepath2);

            if ( !file.exists() )
            {
                log.info(" videoFilepath2 not exist! videoFilepath2="+videoFilepath2);
                continue;
            }
            
            List<Map<String, String>> objectList = entry.getValue();
            List<Map<String, String>> gaitDataList = new ArrayList<>();
         
            int i=0;
            
            for(Map<String, String> objectData : objectList) // 每一段小视频的 每一个对象
            {
                String topX = objectData.get("topX");
                String topY = objectData.get("topY");
                String bottomX = objectData.get("bottomX");
                String bottomY = objectData.get("bottomY");

                int area = Util.getObjectArea(topX,topY,bottomX,bottomY);
                
                if ( area < 10000 )
                {   
                    log.warn(" object is too small! skip! area="+area);
                    continue;
                }
                
                i++;
                
                // 生成 一个 changed video period
                String outputFile = String.format("%s/video/%s_%d_gait.mp4",videoServerWorkingDirectory,videoFilenameKey,i); 
                String outputFileInEdf = String.format("%s/video/%s_%d_gait.mp4",edfServerWorkingDirectory,videoFilenameKey,i);
                
                String objcsvFile = String.format("%s/video/%s_objs_%d.csv",videoServerWorkingDirectory,videoFilenameKey,i);
                String objcsvFileInEdf = String.format("%s/video/%s_objs_%d.csv",edfServerWorkingDirectory,videoFilenameKey,i);
            
                String content = String.format("seektime,category,x1,y1,x2,y2\n0,person,%s,%s,%s,%s\n",objectData.get("topX"),objectData.get("topY"),objectData.get("bottomX"),objectData.get("bottomY"));
                FileUtil.saveToFile(objcsvFileInEdf,content.getBytes());
                
                String commandStr = String.format("nohup python /var/project/edf/utils/genvideo.py -i %s -o %s -objs %s >output.out 2>error.out </dev/null& \n",videoFilepath1,outputFile,objcsvFile); 
                Tool.executeSSHCommandChannelShell(videoServerHost,22,videoServerUser,videoServerPassword,commandStr,1,"");
      
                int retry = 0;
                while(true)
                {
                    retry++;

                    file = new File(outputFileInEdf+".done");

                    if ( !file.exists() )
                    {
                        log.info(" check genvideo.py result file! not exsits! file="+outputFileInEdf+".done");
                        Tool.SleepAWhile(3, 0);

                        if ( retry > 10*20 ) // > 10 mins  / ---1 mins
                            //throw new Exception("genvideo.py result file not exists! outputFileInEdf="+outputFileInEdf+" command="+commandStr);
                            throw new DataProcessingException("genvideo.py result file not exists! outputFileInEdf="+outputFileInEdf+" command="+commandStr,"video_process_error.generate_gait_short_video_failed");
       
                        continue;
                    }

                    Tool.SleepAWhile(3, 0);
                    break;
                }
                
                ByteBuffer fileBuf1 = FileUtil.readFileToByteBuffer(new File(outputFileInEdf));
                
                if ( fileBuf1 == null || fileBuf1.capacity() == 0 )
                {
                    log.info("1111111111 gen video empty!");
                    continue;
                }
                
                int len = VideoProcessUtil.getMediaDurationInSecond(outputFileInEdf);
                if ( len < 2 )
                {
                    log.info("1111111111 gen video < 2 second! len="+len);
                    continue;
                }
                
                String fileKey = UUID.randomUUID().toString();
                String filename1 = String.format("%s.mp4",fileKey);

                VideoProcessUtil.copyFileToVideoServer("gait-video",videoServerHost,videoProcessingServicemap,thisNodeIsAVideoPrcoessingServer,filename1,fileBuf1,platformEm,Integer.parseInt(videoProcessingServiceInstanceId),computingNode);

                String targetURL = String.format(restFulAPIPattern,videoServerHost,fileKey);

                String gaitDataStr = Tool.invokeRestfulService(targetURL,null,null,3);
                //log.info(" face data str="+faceDataStr);

                if ( gaitDataStr.isEmpty() )
                {
                    log.info("1111111111 no gait data!");
                    continue;
                }

                JSONObject faceDataJson = new JSONObject(gaitDataStr);

                String gaitVector = faceDataJson.optString("identification_vector");
            
                map = new HashMap<>();
             
                map.put("top_x",objectData.get("topX"));
                map.put("top_y",objectData.get("topY"));
                map.put("bottom_x",objectData.get("bottomX"));
                map.put("bottom_y",objectData.get("bottomY"));

                map.put("vector",gaitVector);

                gaitDataList.add(map);
            }
           
            if ( !gaitDataList.isEmpty() )
            {
                PictureObjectData pictureVideoObjectData = processOnePeriodVideoObjectData(model.getId(),videoFilename,videoFilepath,gaitDataList);
                videoObjectData.add(pictureVideoObjectData);
            }
        }
                 
        result.put("videoObjectData", videoObjectData);
        
        em.getTransaction().commit();
         
        return result;
    }            
    
    private PictureObjectData processOnePeriodVideoObjectData(int modelId,String videoFilename,String videoFilepath,List<Map<String,String>> gaitDataList) throws Exception
    {
        int topX,topY,bottomX,bottomY;
        
        PictureObjectData pictureObjectData = new PictureObjectData();
        pictureObjectData.setRecognitionModelId(modelId);
        
        Date videoTime = new Date();
        pictureObjectData.setVideoTime(videoTime);
                   
        List<Map<String, String>> objectFeatures = new ArrayList<>();
        List<Integer> objectTypes = new ArrayList<>();
        List<Map<String,Integer>> objectPositions = new ArrayList<>();
        
        pictureObjectData.setObjectFeatures(objectFeatures);
        pictureObjectData.setObjectTypes(objectTypes);
        pictureObjectData.setPictureFilename(videoFilename);
        pictureObjectData.setObjectPositions(objectPositions);
 
        pictureObjectData.setPictureFilepath(videoFilepath);

        int timeInVideoInSeconds = videoStartTimeInSecond+Integer.parseInt(videoFilename.substring(videoFilename.indexOf("_")+1, videoFilename.indexOf(".")));
        pictureObjectData.setTimeInVideoInSeconds(timeInVideoInSeconds);
   
        int i = 0;
        
        for(Map<String,String> gaitData : gaitDataList)  
        {
            i++;
            
            Map<String,Integer> position = new HashMap<>();
            
            try
            {
                String val = gaitData.get("top_x");
                val = val.contains(".")?val.substring(0, val.indexOf(".")):val;
                
                topX = Integer.parseInt(val);
                position.put("top_x", topX);

                val = gaitData.get("top_y");
                val = val.contains(".")?val.substring(0, val.indexOf(".")):val;
                
                topY = Integer.parseInt(val);
                position.put("top_y", topY);

                val = gaitData.get("bottom_x");
                val = val.contains(".")?val.substring(0, val.indexOf(".")):val;
                
                bottomX = Integer.parseInt(val);
                position.put("bottom_x", bottomX);

                val = gaitData.get("bottom_y");
                val = val.contains(".")?val.substring(0, val.indexOf(".")):val;
                
                bottomY = Integer.parseInt(val);
                position.put("bottom_y", bottomY);
            }
            catch(Exception e)
            {
                log.error(" position failed! e="+e);
                continue;
            }
            
            objectPositions.add(position);
          
            Map<String,String> features = new HashMap<>();
           
            String gaitVectorStr = gaitData.get("vector");
                              
            String vectorHash = saveToGaitRepository(timeInVideoInSeconds,i,gaitVectorStr);
                        
            features.put("1",vectorHash);
      
            objectFeatures.add(features);

            int objectTypeId = 9; // gait
            objectTypes.add(objectTypeId);
        }

        return pictureObjectData;
    }
         
    private String saveToGaitRepository(int timeInVideoInSeconds,int gaitIndex,String gaitVectorStr) throws Exception
    {
        String gaitVectorStrHash = DataIdentifier.generateHash(gaitVectorStr);
      
        String vidoeObjectId = String.format("%d.%d.%s.%d.%d.%d", repository.getOrganizationId(),repository.getId(),videoId,timeInVideoInSeconds,model.getId(),gaitIndex);
        vidoeObjectId = DataIdentifier.generateHash(vidoeObjectId);
        
        for(int gaitRepositoryId : gaitRepositoryIds)
        {
            GaitData gaitData = new GaitData();
            
            String key = String.format("%d-%s",repository.getId(),vidoeObjectId);
            String sql = String.format("from GaitData where gaitRepositoryId=%d and gaitKey='%s'",gaitRepositoryId,key);
             
            List<GaitData> list = em.createQuery(sql).getResultList();
            
            if ( list.isEmpty() )
            {
                gaitData.setGaitRepositoryId(gaitRepositoryId);
                gaitData.setGaitKey(key);
                gaitData.setVector(gaitVectorStr);
                gaitData.setVectorHash(gaitVectorStrHash);
                gaitData.setLastModifiedTime(new Date());
                gaitData.setGroups("");
                gaitData.setIsVideoInSystem((short)1);
                em.persist(gaitData);
            }
            else
            {
                gaitData = list.get(0);
                
                gaitData.setVector(gaitVectorStr);
                gaitData.setVectorHash(gaitVectorStrHash);
                gaitData.setLastModifiedTime(new Date());
                em.merge(gaitData);
            }
        }      
 
        return gaitVectorStrHash;
    }
    
    public List<Map<String, String>> detectObjects(String objectRecognitionServiceIP,Map<String,String> videoProcessingServicemap,String objectType,ByteBuffer pictureContent, Map<String, String> parameters) throws Exception
    {
        List<Map<String, String>> objectDatalist = new ArrayList<>();
     
        String fileKey = UUID.randomUUID().toString();
        String filename = String.format("%s.jpg",fileKey);
               
        String objectDataStr = "";
        String targetURL = "";
        
        try
        {
            String videoServerInfo = VideoProcessUtil.copyFileToVideoServer("object_detect_picture",objectRecognitionServiceIP,videoProcessingServicemap,thisNodeIsAVideoPrcoessingServer,filename,pictureContent,platformEm,Integer.parseInt(videoProcessingServiceInstanceId),computingNode);

            String[] val1s = videoServerInfo.split("\\~");
            String videoServerHost = val1s[0];
 
            targetURL = String.format("http://%s:43215/object_detect/detectron/%s",videoServerHost,fileKey);

            objectDataStr = Tool.invokeRestfulService(targetURL,null,null,3);
        }
        catch(Exception e)
        {
            log.error(" invokeRestfulService() failed! url="+targetURL);
            throw new DataProcessingException(" invokeRestfulService() failed! url="+targetURL+" e="+e,"video_process_error.restful_object_detection_failed");
        }
               
        JSONObject objectDataJson = new JSONObject(objectDataStr);
        
        JSONArray array = objectDataJson.optJSONArray("objects");
        Iterator<Object> it = array.iterator();
     
        int i = 0;
        while (it.hasNext()) 
        {
            i++;
            JSONObject obj = (JSONObject)it.next();
                         
            String box = obj.optString("box"); 
            String[] vals = box.split("\\,");

            Map<String,String> map = new HashMap<>();
            
            String type = obj.optString("category");
            
            if ( !objectType.equals(type) )
            {
                log.info(" skip other type="+type);
                continue;
            }
            
            map.put("objectType",type);
            
            map.put("objectIndex",String.valueOf(i));
            
            String val = String.valueOf((int)Float.parseFloat(vals[0]));
            map.put("topX",val);
            
            val = String.valueOf((int)Float.parseFloat(vals[1]));
            map.put("topY",val);
            
            val = String.valueOf((int)Float.parseFloat(vals[2]));
            map.put("bottomX",val);
            
            val = String.valueOf((int)Float.parseFloat(vals[3]));
            map.put("bottomY",val);

            objectDatalist.add(map);
        }      
 
        return objectDatalist;
    }
}
