/*
 * PeopleDressModelProcessor.java
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
import java.util.Iterator;
import java.util.UUID;
import org.json.JSONArray;
import org.json.JSONObject;
 
import com.broaddata.common.model.organization.FaceData;
import com.broaddata.common.model.organization.FaceRepository;
import com.broaddata.common.model.organization.Repository;
import com.broaddata.common.model.platform.ComputingNode;
import com.broaddata.common.model.platform.VideoAnalysisModel;
import com.broaddata.common.model.vo.DataobjectVO;
import com.broaddata.common.model.vo.PictureObjectData;
import com.broaddata.common.util.DataIdentifier;
import com.broaddata.common.util.FileUtil;
import com.broaddata.common.util.Tool;
import com.broaddata.common.util.Util;
import com.broaddata.common.util.VideoProcessUtil;

import static com.broaddata.common.util.VideoProcessUtil.getTimeInVideoFromPictureName;

public class FaceRecognitionProcessor extends ModelProcessor
{
    static final Logger log = Logger.getLogger("FaceRecognitionProcessor");
 
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
    private String[] faceRepositoryNames;
    private int[] faceRepositoryIds;
    private String videoId;
    private String timeInVideoStr;
    private DataobjectVO dataobjectVO;
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
         
        faceRepositoryNames = (String[])parameters.get("faceRepositoryNames");
       
        videoStartTimeInSecond = (Integer)parameters.get("videoStartTimeInSecond");
        videoLength = (Integer)parameters.get("videoLength");
    }
    
    @Override
    public Map<String,Object> execute() throws Exception
    {
        log.info(" run face recogonition model!   ");
        
        em.getTransaction().begin();
         
        Map<String,Object> result = new HashMap<>();
                
        Map<String,String> map = Util.getEDFVideoProcessingService(platformEm,videoProcessingServiceInstanceId);
 
        //String videoServerHost = Util.pickOneServiceIP(map.get("ips"));
        String videoServerUser = map.get("user");
        String videoServerPassword = map.get("password");
        
        /*map = Util.getEDFVideoProcessingService(platformEm,model.getServiceInstanceId());
 
        String host = Util.pickOneServiceIP(map.get("ips"));
        String keyId=map.get("keyId");
        String secretKey = map.get("secretKey");
        String version = map.get("version");*/
                   
        Map<String,String> videoProcessingServicemap = Util.getEDFVideoProcessingService(platformEm,String.valueOf(videoProcessingServiceInstanceId)); 
            
        Map<String,String> map1 = VideoProcessUtil.getVideoProcessingWorkingDirectory(thisNodeIsAVideoPrcoessingServer,computingNode, currentWorkingDirKey,videoServerHost,videoServerUser,videoServerPassword);

        //String videoServerWorkingDirectory = map1.get("videoServerWorkingDirectory");
        String edfServerWorkingDirectory =  map1.get("edfServerWorkingDirectory");
           
        faceRepositoryIds = new int[faceRepositoryNames.length];
        for(int i=0;i<faceRepositoryNames.length;i++)
        {
            String sql = String.format("from FaceRepository where name='%s'",faceRepositoryNames[i]);
            FaceRepository faceRepository = (FaceRepository)em.createQuery(sql).getSingleResult();
            faceRepositoryIds[i] = faceRepository.getId();
        }
        
        videoId = dataobjectVO.getMetadatas().get("VIDEO_ID");
            
        List<String> filepathList = new ArrayList<>();
        
        String imgFolder = String.format("%s/img",edfServerWorkingDirectory);
        
        FileUtil.getAllFilesWithSamePrefix(new File(imgFolder), "Image", filepathList);
        
        List<PictureObjectData> videoObjectData = new ArrayList<>();
         
        String restFulAPIPattern = model.getConfig();
        
        int i = 0;
        for(String imageFilepath : filepathList)
        {        
            i++;
            
            String imageFilename = imageFilepath.replaceAll("\\\\", "/");
            imageFilename = imageFilepath.substring(imageFilename.lastIndexOf("/")+1);

            log.info("i="+i+",total="+filepathList.size()+" imagefilename="+imageFilename);
                        
            ByteBuffer fileBuf = FileUtil.readFileToByteBuffer(new File(imageFilepath));
                    
            String fileKey = UUID.randomUUID().toString();
            String filename = String.format("%s.jpg",fileKey);
        
            VideoProcessUtil.copyFileToVideoServer("face-picture",videoServerHost,videoProcessingServicemap,thisNodeIsAVideoPrcoessingServer,filename,fileBuf,platformEm,Integer.parseInt(videoProcessingServiceInstanceId),computingNode);
              
            // cal face recognition rest api  5432/face_detect  http://%s:54321/face_recog/facenet/%
            //String targetURL = String.format("http://%s:54321/face_detect/facenet/%s",videoServerHost,fileKey);
            //String targetURL = String.format("http://%s:5432/face_detect/facenet/%s",videoServerHost,fileKey);
            
            String targetURL = String.format(restFulAPIPattern,videoServerHost,fileKey);
                                             
            List<Map<String, String>> faceDataList = new ArrayList<>();
             
            String faceDataStr = "";
            
            try
            {
                faceDataStr = Tool.invokeRestfulService(targetURL,null,null,3);
            }
            catch(Exception e)
            {
                throw new DataProcessingException("detect face failed! e="+e,"video_process_error.detect_faces_failed");
            }
            
            //log.info(" face data str="+faceDataStr);

            if ( faceDataStr.isEmpty() )
            {
                log.info(" no faces!");
                continue;
            }
            
            JSONObject faceDataJson = new JSONObject(faceDataStr);

            JSONArray array = faceDataJson.optJSONArray("faces");
            Iterator<Object> it = array.iterator();

            while (it.hasNext()) 
            {
                JSONObject obj = (JSONObject)it.next();

                String box = obj.optString("box"); 
                String[] vals = box.split("\\,");

                map = new HashMap<>();
                map.put("top_x",vals[0]);
                map.put("top_y",vals[1]);
                map.put("bottom_x",vals[2]);
                map.put("bottom_y",vals[3]);

                map.put("vector",obj.optString("embeddings"));

                faceDataList.add(map);
            }      
                   
            if ( !faceDataList.isEmpty() )
            {
                PictureObjectData pictureVideoObjectData = processOneImageVideoObjectData(model.getId(),imageFilename,imageFilepath,faceDataList);
                videoObjectData.add(pictureVideoObjectData);
            }
        }
                 
        result.put("videoObjectData", videoObjectData);
        
        em.getTransaction().commit();
         
        return result;
    }            
              
    private String saveToFaceRepository(int timeInVideoInSeconds,int faceIndex,String faceVectorStr,int width,int height) throws Exception
    {
        String faceVectorStrHash = DataIdentifier.generateHash(faceVectorStr);
      
        String vidoeObjectId = String.format("%d.%d.%s.%d.%d.%d", repository.getOrganizationId(),repository.getId(),videoId,timeInVideoInSeconds,model.getId(),faceIndex);
        vidoeObjectId = DataIdentifier.generateHash(vidoeObjectId);
        
        for(int faceRepositoryId : faceRepositoryIds)
        {
            FaceData faceData = new FaceData();
            
            String key = String.format("%d-%s",repository.getId(),vidoeObjectId);
            String sql = String.format("from FaceData where faceRepositoryId=%d and faceKey='%s'",faceRepositoryId,key);
             
            List<FaceData> list = em.createQuery(sql).getResultList();
            
            if ( list.isEmpty() )
            {
                faceData.setFaceRepositoryId(faceRepositoryId);
                faceData.setFaceKey(key);
                faceData.setVector(faceVectorStr);
                faceData.setVectorHash(faceVectorStrHash);
                faceData.setLastModifiedTime(new Date());
                faceData.setGroups("");
                faceData.setIsPictureInSystem((short)1);
                faceData.setFaceWidth(width);
                faceData.setFaceHeight(height);
                em.persist(faceData);
            }
            else
            {
                faceData = list.get(0);
                
                faceData.setVector(faceVectorStr);
                faceData.setVectorHash(faceVectorStrHash);
                faceData.setLastModifiedTime(new Date());
                em.merge(faceData);
            }
        }      
 
        return faceVectorStrHash;
    }
    
    private PictureObjectData processOneImageVideoObjectData(int modelId,String imageFilename,String imageFilepath,List<Map<String,String>> faceDataList) throws Exception
    {
        int topX,topY,bottomX,bottomY;
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
   
        int i = 0;
        
        for(Map<String,String> faceData : faceDataList)  
        {
            i++;
            
            Map<String,Integer> position = new HashMap<>();
            
            try
            {
                topX = Integer.parseInt(faceData.get("top_x"));
                position.put("top_x", topX);

                topY = Integer.parseInt(faceData.get("top_y"));
                position.put("top_y", topY);

                bottomX = Integer.parseInt(faceData.get("bottom_x"));
                position.put("bottom_x", bottomX);

                bottomY = Integer.parseInt(faceData.get("bottom_y"));
                position.put("bottom_y", bottomY);
            }
            catch(Exception e)
            {
                log.error(" position failed! e="+e);
                continue;
            }
            
            objectPositions.add(position);
          
            Map<String,String> features = new HashMap<>();
           
            String faceVectorStr = faceData.get("vector");
                              
            String vectorHash = saveToFaceRepository(timeInVideoInSeconds,i,faceVectorStr,bottomX-topX+1,bottomY-topY+1);
                        
            features.put("1",vectorHash);
      
            objectFeatures.add(features);

            int objectTypeId = 8; // human face
            objectTypes.add(objectTypeId);
        }

        return pictureObjectData;        
    }
}
