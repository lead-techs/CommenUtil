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
import com.broaddata.common.model.organization.FaceRepository;
import com.broaddata.common.model.platform.ComputingNode;
import com.broaddata.common.util.FileUtil;
import com.broaddata.common.util.Tool;
import com.broaddata.common.util.Util;
import com.broaddata.common.util.VideoProcessUtil;

public class ObjectRecognitionManager 
{
    static final Logger log = Logger.getLogger("ObjectRecognitionManager");
    private String objectRecognitionServiceIPs;
    private boolean thisNodeIsAVideoPrcoessingServer;
    private int videoProcessingServiceInstanceId;
    private EntityManager platformEm;
    private ComputingNode computingNode;
    private Map<String,String> videoProcessingServicemap;
       
    public ObjectRecognitionManager(){
    }
    
    public ObjectRecognitionManager(ComputingNode computingNode,String objectRecognitionServiceIPs,boolean thisNodeIsAVideoPrcoessingServer,int videoProcessingServiceInstanceId,EntityManager platformEm) throws Exception
    {
        this.objectRecognitionServiceIPs = objectRecognitionServiceIPs;
        this.thisNodeIsAVideoPrcoessingServer = thisNodeIsAVideoPrcoessingServer;
        this.videoProcessingServiceInstanceId = videoProcessingServiceInstanceId;
        this.platformEm = platformEm;
        this.computingNode = computingNode;
    }
    
    public void init()
    {
        videoProcessingServicemap = Util.getEDFVideoProcessingService(platformEm,String.valueOf(videoProcessingServiceInstanceId)); 
    }
    
    public List<Map<String, String>> detectObjects(String objectType,ByteBuffer pictureContent, Map<String, String> parameters) throws Exception
    {
        List<Map<String, String>> objectDatalist = new ArrayList<>();
        
        String objectRecognitionServiceIP;
        
        // copy content to object recognition server
        
        if ( thisNodeIsAVideoPrcoessingServer )
            objectRecognitionServiceIP = computingNode.getInternalIP();
        else
            objectRecognitionServiceIP = Util.pickOneServiceIP(objectRecognitionServiceIPs);
         
        String fileKey = UUID.randomUUID().toString();
        String filename = String.format("%s.jpg",fileKey);
               
        String objectDataStr = "";
        String targetURL = "";
        
        try
        {
            String videoServerInfo = VideoProcessUtil.copyFileToVideoServer("object_detect_picture",objectRecognitionServiceIP,videoProcessingServicemap,thisNodeIsAVideoPrcoessingServer,filename,pictureContent,platformEm,videoProcessingServiceInstanceId,computingNode);

            String[] val1s = videoServerInfo.split("\\~");
            String videoServerHost = val1s[0];
 
            targetURL = String.format("http://%s:43215/object_detect/detectron/%s",videoServerHost,fileKey);

            objectDataStr = Tool.invokeRestfulService(targetURL,null,null,3);
        }
        catch(Exception e)
        {
            log.error(" invokeRestfulService() failed! url="+targetURL);
            throw e;
        }
               
        if ( objectDataStr == null || objectDataStr.trim().isEmpty() )
        {
            log.info(" empty result targetURL="+targetURL);
            return objectDatalist;
        }
        
        JSONObject objectDataJson = new JSONObject(objectDataStr);
        
        JSONArray array = objectDataJson.optJSONArray("objects");
        
        if ( array == null )
        {
            log.info(" no object found!");
            return objectDatalist;
        }
        
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
                //log.info(" skip other type="+type);
                continue;
            }
            
            map.put("objectType",type);
            map.put("objectIndex",String.valueOf(i));
         /*   map.put("x",Double.vals[0]);
            map.put("y",vals[1]);
            map.put("width",String.valueOf(Integer.parseInt(vals[2])-Integer.parseInt(vals[0])));
            map.put("height",String.valueOf(Integer.parseInt(vals[3])-Integer.parseInt(vals[1])));*/

            objectDatalist.add(map);
        }      
 
        return objectDatalist;
    }

    public List<Map<String, String>> detectObjects1(String objectRecognitionServiceIPs,String objectType,ByteBuffer pictureContent) throws Exception
    {
        List<Map<String, String>> objectDatalist = new ArrayList<>();
        
        String objectRecognitionServiceIP = Util.pickOneServiceIP(objectRecognitionServiceIPs);
         
        String fileKey = UUID.randomUUID().toString();
         
        String objectDataStr = "";
        String targetURL = "";
        String videoFilepathInEDFServer = "";
        
        try
        {
            videoFilepathInEDFServer = String.format("\\\\%s\\data\\share\\object_detect_picture\\%s.jpg",objectRecognitionServiceIP,fileKey);
            FileUtil.writeFileFromByteBuffer(videoFilepathInEDFServer,pictureContent);
               
            targetURL = String.format("http://%s:43215/object_detect/detectron/%s",objectRecognitionServiceIP,fileKey);

            objectDataStr = Tool.invokeRestfulService(targetURL,null,null,1000);
        }
        catch(Exception e)
        {
            log.error(" invokeRestfulService() failed! url="+targetURL);
            throw e;
        }
        finally
        {
           FileUtil.removeFile(videoFilepathInEDFServer);
        }
               
        if ( objectDataStr == null || objectDataStr.trim().isEmpty() )
        {
            log.info(" empty result targetURL="+targetURL);
            return objectDatalist;
        }
        
        JSONObject objectDataJson = new JSONObject(objectDataStr);
        
        JSONArray array = objectDataJson.optJSONArray("objects");
        
        if ( array == null )
        {
            log.info(" no object found!");
            return objectDatalist;
        }
        
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
                continue;
            
            map.put("objectType",type);
            map.put("objectIndex", String.valueOf(i));
            map.put("x",String.valueOf(Integer.parseInt(vals[0].substring(0,vals[0].indexOf(".")))));
            map.put("y",String.valueOf(Integer.parseInt(vals[1].substring(0,vals[1].indexOf(".")))));
            map.put("width",String.valueOf(Integer.parseInt(vals[2].substring(0,vals[2].indexOf(".")))-Integer.parseInt(vals[0].substring(0,vals[0].indexOf(".")))));
            map.put("height",String.valueOf(Integer.parseInt(vals[3].substring(0,vals[3].indexOf(".")))-Integer.parseInt(vals[1].substring(0,vals[1].indexOf(".")))));
            
            objectDatalist.add(map);
        }      
 
        return objectDatalist;
    }

}