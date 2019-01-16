/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.manager;

import org.apache.log4j.Logger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.persistence.EntityManager;
import org.json.JSONArray;
import org.json.JSONObject;

import com.broaddata.common.model.organization.FaceData;
import com.broaddata.common.model.organization.FaceGroup;
import com.broaddata.common.model.organization.FaceRepository;
import com.broaddata.common.model.organization.VideoCamera;
import com.broaddata.common.model.platform.ComputingNode;
import com.broaddata.common.util.DataIdentifier;
import com.broaddata.common.util.Tool;
import com.broaddata.common.util.Util;
import com.broaddata.common.util.VideoProcessUtil;

public class CameraInfoManager 
{
    static final Logger log = Logger.getLogger("cameraInfoManager");
    
    //private String faceRecognitionServiceIPs;
    private String cameraInfoManagerServiceIPs;
    private boolean thisNodeIsAVideoPrcoessingServer;
    private int videoProcessingServiceInstanceId;
    private EntityManager platformEm;
    private ComputingNode computingNode;
    private Map<String,String> videoProcessingServicemap;
       
    public CameraInfoManager(ComputingNode computingNode,String cameraInfoManagerServiceIPs,boolean thisNodeIsAVideoPrcoessingServer,int videoProcessingServiceInstanceId,EntityManager platformEm) throws Exception
    {
        this.cameraInfoManagerServiceIPs = cameraInfoManagerServiceIPs;
        this.thisNodeIsAVideoPrcoessingServer = thisNodeIsAVideoPrcoessingServer;
        this.videoProcessingServiceInstanceId = videoProcessingServiceInstanceId;
        this.platformEm = platformEm;
        this.computingNode = computingNode;
    }
    
    public void init()
    {
        videoProcessingServicemap = Util.getEDFVideoProcessingService(platformEm,String.valueOf(videoProcessingServiceInstanceId)); 
    }
     
    public static Map<String,String> addCameraInfo(EntityManager em,int organizationId,String cameraCode,String cameraDescription,Map<String,String> parameters) 
    {
        Map<String,String> resultMap;
        VideoCamera videoCamera;
  
        try
        {
            em.getTransaction().begin();
            
            videoCamera = new VideoCamera();
            resultMap = new HashMap();
            
            videoCamera.setOrganizationId(organizationId);
            videoCamera.setCode(cameraCode);
            videoCamera.setDescription(cameraDescription);
            videoCamera.setStatus(2);
            //em.merge(videoCamera);
            em.persist(videoCamera);
            em.getTransaction().commit();
            
            resultMap.put("cameraId", String.valueOf(videoCamera.getId()));
            resultMap.put("cameraCode", videoCamera.getCode());
            resultMap.put("cameraDescription", videoCamera.getDescription());
        }
         catch (Exception e) 
        {
            throw e;
        } 
        return resultMap;
    }
    public static Map<String,String> updateCameraInfo(EntityManager em,int cameraId,String cameraCode,String cameraDescription,Map<String,String> parameters) 
    {
        Map<String,String> resultMap;
        VideoCamera videoCamera;
        // FaceRepository faceRepository = (FaceRepository)em.find(FaceRepository.class,faceRepositoryId);
        try
        {
            em.getTransaction().begin();
            videoCamera = (VideoCamera)em.find(VideoCamera.class,cameraId);
            resultMap = new HashMap();
            
            videoCamera.setCode(cameraCode);
            videoCamera.setDescription(cameraDescription);
            videoCamera.setStatus(2);
            
            em.merge(videoCamera);
            em.getTransaction().commit();
            
            resultMap.put("cameraId", String.valueOf(videoCamera.getId()));
            resultMap.put("cameraCode", videoCamera.getCode());
            resultMap.put("cameraDescription", videoCamera.getDescription());
        }
        catch (Exception e) 
        {
            throw e;
        } 
        return resultMap;
   }
    public static void removeCameraInfo(EntityManager em,int cameraId) 
    {
        Map<String,String> resultMap;
        VideoCamera videoCamera;
        // FaceRepository faceRepository = (FaceRepository)em.find(FaceRepository.class,faceRepositoryId);
       try
        {
            em.getTransaction().begin();
            videoCamera = (VideoCamera)em.find(VideoCamera.class,cameraId);
            
            em.remove(videoCamera);
            
            em.getTransaction().commit();
        }
        catch (Exception e) 
        {
            throw e;
        } 
   }
   public static List<Map<String,String>> getCameraInfoList(EntityManager em,List<Integer> cameraIds,String filter) 
    {
        VideoCamera videoCamera;
        
        List<Map<String,String>> cameraInfoList = new ArrayList();
        
        
        for(int cameraId:cameraIds)
        {
            Map<String,String> map = new HashMap();
            videoCamera = (VideoCamera)em.find(VideoCamera.class,cameraId);
            
            map.put("cameraId", String.valueOf(videoCamera.getId()));
            map.put("cameraCode", videoCamera.getCode());
            map.put("cameraDescription", videoCamera.getDescription());
            
            cameraInfoList.add(map);
        }
        
        return cameraInfoList;
    }
}