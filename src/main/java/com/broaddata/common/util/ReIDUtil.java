package com.broaddata.common.util;

import com.broaddata.common.manager.FaceRecognitionManager;
import com.broaddata.common.model.enumeration.CompareType;
import com.broaddata.common.model.organization.FaceData;
import com.broaddata.common.model.organization.GaitData;
import com.broaddata.common.model.organization.GaitRepository;
import com.broaddata.common.model.organization.TargetScreeningTask;
import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.thrift.dataservice.Dataset;
import com.broaddata.common.thrift.dataservice.FaceInfo;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import org.apache.log4j.Logger;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import org.apache.thrift.TException;
import org.json.JSONObject;

/**
 * 识别是不是同一对象
 */

public class ReIDUtil
{
    static final Logger log=Logger.getLogger("RCUtil");      
        
    private static double stdGaitDistance = -1;
     

    public static void searchSameObjects(int organizationId,int caseId,int repositoryId,int objectType,EntityManager platformEm,EntityManager em,int faceRepositoryId,List<Integer> targetCameraIds,String currentVideoObjectId,List<String> sameVideoObjectIdList,JSONObject currentObjectResultFields,DataService.Client dataService,boolean excludeItself,Date startTime,Date endTime) throws TException, Exception
    {
        Dataset videoObjectDataset;
        int needToBeProcessedObjectType;
                    
        String features = currentObjectResultFields.optString("VIDEO_OBJECT_FEATURES");
                    
        if ( objectType == 0 )
            needToBeProcessedObjectType = 1; //人
        else
            needToBeProcessedObjectType = objectType;
        
        if ( needToBeProcessedObjectType == 8 || needToBeProcessedObjectType == 1 )  // 是人和人脸的, 先处理人脸
        { 
            String faceVectorStrHash = "";

            try
            {
                if ( needToBeProcessedObjectType == 1 ) // if no face, continue
                {
                    String featuresStr = Tool.removeFirstAndLastChar(features);
                    String vals[] = featuresStr.split("\\,");

                    for(String val : vals)
                    {
                        String val1 = Tool.removeAroundAllQuotation(val);

                        String flag = "8-1~";

                        if ( val1.startsWith(flag) )
                        {
                            faceVectorStrHash = val1.substring(val1.indexOf(flag)+flag.length(),val1.indexOf(flag)+flag.length()+40);
                            break;
                        }
                    }
                }
                else
                    faceVectorStrHash = features.substring(features.indexOf("~")+1);
            }
            catch(Exception e)
            {
                log.warn(" get face vector hash failed! e="+e);
            }

            //faceVectorStrHash = ""; // skip face
            
            if ( faceVectorStrHash.isEmpty() )
            {
                //log.warn(" empty face vector hash, skip");
            }
            else
            {
                log.info("111111111 faceVectorStrHash="+faceVectorStrHash);

                List<FaceData> faceDataList = em.createQuery(String.format("from FaceData where faceKey like '%d-%%' and vectorHash='%s'",repositoryId,faceVectorStrHash)).getResultList();

                if ( !faceDataList.isEmpty() )
                {
                    if ( faceDataList.get(0).getFaceHeight()<FaceRecognitionManager.minHeightForComparing || faceDataList.get(0).getFaceWidth()<FaceRecognitionManager.minWidthForComparing )
                    {
                        log.info(" small face, skip");
                    }
                    else
                    {
                        String pictureVectorStr = faceDataList.get(0).getVector();

                        List<FaceInfo> faceInfoList = dataService.searchFacesByVector(organizationId, faceRepositoryId, "", pictureVectorStr,false); 

                        for(FaceInfo faceInfo : faceInfoList)
                        {
                            String[] vals = faceInfo.getFaceKey().split("\\-");

                            if ( excludeItself && vals[1].equals(currentVideoObjectId) )
                            {
                                log.info(" execlude itself! ");
                                continue;
                            }

                            if ( !sameVideoObjectIdList.contains(vals[1]) )
                                sameVideoObjectIdList.add(vals[1]);
                        }
                    } 
                }
            }
        }

        if ( needToBeProcessedObjectType != 8 )  // 不是人脸的,做以图搜图 和 步态
        {
            // 通过以图搜图查相同对象
            
            videoObjectDataset = null;
                   
            String videoObjectPictrueDataobjectId = currentObjectResultFields.optString("VIDEO_OBJECT_PICTURE_EDF_ID");

            String topX = currentObjectResultFields.optString("VIDEO_OBJECT_POSITION_TOP_X");
            String topY = currentObjectResultFields.optString("VIDEO_OBJECT_POSITION_TOP_Y");
            String bottomX = currentObjectResultFields.optString("VIDEO_OBJECT_POSITION_BOTTOM_X");
            String bottomY = currentObjectResultFields.optString("VIDEO_OBJECT_POSITION_BOTTOM_Y");
 
            int area = Util.getObjectArea(topX,topY,bottomX,bottomY);
            
            if ( area >= CommonKeys.MIN_SEARCHABLE_PICTURE_AREA )
            {                
                ByteBuffer objectImageBuf = VideoProcessUtil.retrieveVideoObjectPictureContent(em, platformEm,organizationId, repositoryId, videoObjectPictrueDataobjectId,Integer.parseInt(topX), Integer.parseInt(topY), Integer.parseInt(bottomX), Integer.parseInt(bottomY)); 

                int retry = 0;

                while(true)
                {
                    retry++;

                    try
                    {
                        Map<String, String> parameters = new HashMap<>();
                        parameters.put("caseId", String.valueOf(caseId));

                        if ( startTime!=null && endTime!=null )
                        {
                            String startTimeStr = Tool.convertDateToString(startTime, "yyyyMMddHHmmss");
                            String endTimeStr = Tool.convertDateToString(endTime, "yyyyMMddHHmmss");

                            parameters.put("startTime",startTimeStr);
                            parameters.put("endTime",endTimeStr);
                        }
                        else
                        if ( startTime!=null && endTime==null )
                        {
                            String startTimeStr = Tool.convertDateToString(startTime, "yyyyMMddHHmmss");
                            parameters.put("startTime",startTimeStr);
                        }
                        else
                        if ( startTime==null && endTime!=null )
                        {
                            String endTimeStr = Tool.convertDateToString(startTime, "yyyyMMddHHmmss");
                            parameters.put("endTime",endTimeStr);
                        }

                        String objectTypeId = currentObjectResultFields.optString("VIDEO_OBJECT_TYPE");

                        if ( objectTypeId != null )
                            parameters.put("objectTypeId",objectTypeId);

                        
                        if ( targetCameraIds != null && !targetCameraIds.isEmpty() )
                        {
                            String str = "";
                            for(Integer cameraId : targetCameraIds)
                                str += cameraId + "`";
                         
                            str = Tool.removeLastChars(str, 1);
                                                        
                            parameters.put("cameraId",String.format("%s",str));
                        }
                        
                        videoObjectDataset = null;
                        videoObjectDataset = dataService.searchPicturesByPicture(organizationId, repositoryId, 0, objectImageBuf, parameters, 100,3.0);
 
                        break;
                    }
                    catch(Exception e)
                    {
                        log.info(" dataService.searchPicturesByPicture() failed1 e="+e+" retry="+retry);

                        if ( e.getMessage().contains("ClientException: 1010") ) // dataset not ready
                            throw e;

                        Tool.SleepAWhile(1, 0);

                        if ( retry < 3 )
                            continue;

                        break;

                        //String tmpFilename = String.format("wrong.%s.jpg",UUID.randomUUID().toString());
                        //String imageFilepath = String.format("%s/%s",System.getProperty("java.io.tmpdir"),tmpFilename);

                        //FileUtil.writeFileFromByteBuffer(imageFilepath, objectImageBuf);
                    }
                }
            }
            else
                log.warn("skip <12000 pixel object");
 
            //log.info(" videoObjectDataset="+videoObjectDataset);

            if ( videoObjectDataset != null )
            {
                log.info(" search picture videoObjectDataset size="+videoObjectDataset.getRows().size());

                for (Map<String, String> row1 : videoObjectDataset.getRows()) 
                {
                    String videoObjectId = row1.get("dataobject_id");
                    videoObjectId = videoObjectId.substring(0, 40);

                    if (  excludeItself && videoObjectId.equals(currentVideoObjectId) )
                    {
                        log.warn(" same dataobject id, exclude itself, skip");
                        continue;
                    }
                    
                  /*  if ( targetCameraIds != null && !targetCameraIds.isEmpty() )
                    {
                        String cameraIdStr = row1.get("VIDEO_OBJECT_CAMERA_ID");
                            
                        boolean found = false;
                        
                        for(Integer cameraId : targetCameraIds)
                        {
                            if ( Integer.parseInt(cameraIdStr) ==  cameraId ) 
                            {
                                found = true;
                                break;
                            }
                        }
                        
                        if ( found == false )
                        {
                            log.warn(" not found cameraIdStr="+cameraIdStr);
                            continue;
                        }
                    }*/
                      
                    if ( !sameVideoObjectIdList.contains(videoObjectId) )
                        sameVideoObjectIdList.add(videoObjectId);
                }

                log.info("picture  sameObjectNumber="+videoObjectDataset.getRows().size());
            }
                        
            // 通过步态查相同对象
            String gaitVectorStrHash = "";
            int gaitRepositoryId = 0;

            try
            {
                if ( stdGaitDistance < 0 )
                {
                    String gaitDistanceStr = Util.getSystemConfigValue(platformEm,0,"video","gait_distance");

                    if( gaitDistanceStr.isEmpty() )
                        stdGaitDistance = 5;
                    else
                        stdGaitDistance = Double.parseDouble(gaitDistanceStr);

                    log.info(" stdGaitDistance ="+stdGaitDistance);
                }
                   
                if ( needToBeProcessedObjectType == 1 ) // if no face, continue
                {
                    String featuresStr = Tool.removeFirstAndLastChar(features);
                    String vals[] = featuresStr.split("\\,");

                    for(String val : vals)
                    {
                        String val1 = Tool.removeAroundAllQuotation(val);

                        String flag = "9-1~";

                        if ( val1.startsWith(flag) )
                        {
                            gaitVectorStrHash = val1.substring(val1.indexOf(flag)+flag.length(),val1.indexOf(flag)+flag.length()+40);
                            break;
                        }
                    }
                }
                else
                    gaitVectorStrHash = features.substring(features.indexOf("~")+1);
            }
            catch(Exception e)
            {
                log.warn(" get gait vector hash failed! e="+e);
            }

            if ( gaitVectorStrHash.isEmpty() )
            {
                //log.warn(" empty face vector hash, skip");
            }
            else
            {
                log.info("111111111 gaitVectorStrHash="+gaitVectorStrHash);

                String sql = String.format("from GaitRepository where name='case-%d'",caseId);
                GaitRepository gaitRepository = (GaitRepository)em.createQuery(sql).getSingleResult();   
                
                gaitRepositoryId = gaitRepository.getId();
            
                String[] checkVectorArray = gaitVectorStrHash.split("\\,");
             
                List<GaitData> list = em.createQuery(String.format("from GaitData where gaitRepositoryId=%d",gaitRepositoryId)).getResultList();

                for(GaitData gaitData : list)
                {
                    String[] vectorArray = gaitData.getVector().split("\\,");   

                    double dist = FaceRecognitionManager.compareTwoFaces(checkVectorArray,vectorArray);

                    if ( dist <= stdGaitDistance )
                    {
                        String[] vals = gaitData.getGaitKey().split("\\-");

                        String videoObjectId = vals[1];
                        
                        if (  excludeItself && videoObjectId.equals(currentVideoObjectId) )
                        {
                            log.warn(" same dataobject id, exclude itself, skip");
                            continue;
                        }

                        if ( targetCameraIds != null && !targetCameraIds.isEmpty() )
                        {
                            String[] selectedFields = new String[]{"VIDEO_OBJECT_CAMERA_ID"};
                            String[] sortFields = new String[]{};
                            List<Map<String,Object>> objectDataList = VideoProcessUtil.retrieveVideoObjectData(em,platformEm,organizationId,repositoryId,sameVideoObjectIdList,selectedFields,sortFields);

                            if ( objectDataList.isEmpty() )
                            {
                                log.info(" object data not found! videoObjectId="+videoObjectId);
                                continue;
                            }
                            
                            String cameraIdStr = (String)objectDataList.get(0).get("VIDEO_OBJECT_CAMERA_ID");

                            boolean found = false;

                            for(Integer cameraId : targetCameraIds)
                            {
                                if ( Integer.parseInt(cameraIdStr) ==  cameraId ) 
                                {
                                    found = true;
                                    break;
                                }
                            }

                            if ( found == false )
                            {
                                log.warn(" not found cameraIdStr="+cameraIdStr);
                                continue;
                            }
                        }
 
                        if ( !sameVideoObjectIdList.contains(videoObjectId) )
                            sameVideoObjectIdList.add(videoObjectId);
                    }
                }
            }
        }
        
        log.info("1111 found sameVideoObjectIdList size ="+sameVideoObjectIdList.size());
    }
}