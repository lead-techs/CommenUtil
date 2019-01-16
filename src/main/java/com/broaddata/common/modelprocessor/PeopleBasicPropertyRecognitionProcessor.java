/*
 * PeopleBasicPropertyModelProcessor.java
 *
 */

package com.broaddata.common.modelprocessor;
 
import org.apache.log4j.Logger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import javax.persistence.EntityManager;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import com.broaddata.common.model.enumeration.EncodingType;
import com.broaddata.common.model.platform.ComputingNode;
import com.broaddata.common.model.platform.VideoAnalysisModel;
import com.broaddata.common.model.vo.PictureObjectData;
import com.broaddata.common.util.FileUtil;
import com.broaddata.common.util.Tool;
import com.broaddata.common.util.Util;
import com.broaddata.common.util.VideoProcessUtil;
import com.broaddata.common.exception.DataProcessingException;

public class PeopleBasicPropertyRecognitionProcessor extends ModelProcessor
{
    static final Logger log = Logger.getLogger("PeopleBasicPropertyModelProcessor");
     
    private ComputingNode computingNode;
    private VideoAnalysisModel model;
    private EntityManager platformEm;
    private float secondsBetweenTwoFrame;
    private String videoServerHost;
    private String currentWorkingDirKey;
    private String videoProcessingServiceInstanceId;
    private boolean thisNodeIsAVideoPrcoessingServer;
    private int gpuId;
    private String gpuIndicator;
    private List<Lock> gpuLockList;
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
        videoStartTimeInSecond = (Integer)parameters.get("videoStartTimeInSecond");
        videoLength = (Integer)parameters.get("videoLength");
        
        gpuLockList = (List<Lock>)parameters.get("gpuLockList");
    }
    
    @Override
    public Map<String,Object> execute() throws Exception
    {
        Date videoTime;
        String command;
        String featureResultFile = "attributes_result.csv";
        
        log.info(" run people basic propery recogonition model!   ");
        
        List<PictureObjectData> dataList = new ArrayList<>();
        Map<String,Object> result = new HashMap<>();
        
        Map<String,String> map = Util.getEDFVideoProcessingService(platformEm,videoProcessingServiceInstanceId);
 
        //String videoServerHost = Util.pickOneServiceIP(map.get("ips"));
        String videoServerUser = map.get("user");
        String videoServerPassword = map.get("password");
        
        Map<String,String> map1 = VideoProcessUtil.getVideoProcessingWorkingDirectory(thisNodeIsAVideoPrcoessingServer, computingNode, currentWorkingDirKey,videoServerHost,videoServerUser,videoServerPassword);

        String videoServerWorkingDirectory = map1.get("videoServerWorkingDirectory");
        String edfServerWorkingDirectory =  map1.get("edfServerWorkingDirectory");
        
        String videoObjectExtractionAttributeFilename = String.format("%s/%s",edfServerWorkingDirectory,featureResultFile);

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
                    gpuId = i;
                    gpuIndicator = String.format("CUDA_VISIBLE_DEVICES=%d",gpuId);

                    // feature extraction
                    //command = String.format("nohup python3 /home/edf/devel/detectattrs.py -i %s/img -objs %s/object-detect-result.txt -o %s/%s >output.out 2>error.out </dev/null& \n",videoServerWorkingDirectory,videoServerWorkingDirectory,videoServerWorkingDirectory,featureResultFile);        
                    command = String.format("%s nohup python /var/project/edf/utils/detectattrs.py -i %s/img -objs %s/object-detect-result.txt -o %s/%s >output.out 2>error.out </dev/null& \n",gpuIndicator,videoServerWorkingDirectory,videoServerWorkingDirectory,videoServerWorkingDirectory,featureResultFile);

                    Tool.executeSSHCommandChannelShell(videoServerHost,22,videoServerUser,videoServerPassword,command,1,null);

                    // check if result csv file is there

                    int retry = 0;
                    while(true)
                    {
                        retry++;

                        File file = new File(videoObjectExtractionAttributeFilename+".error");
                        if ( file.exists() )
                        {
                            log.info(" exist attributes_result.csv.error file! ");
                            Tool.SleepAWhile(3, 0);
                            
                            FileUtil.removeFile(videoObjectExtractionAttributeFilename+".error");
                            needToReRun = true;
                            break;
                        }
                        
                        file = new File(videoObjectExtractionAttributeFilename+".done");

                        if ( !file.exists() )
                        {
                            log.info(" check attributes result file! not exsits! file="+videoObjectExtractionAttributeFilename);
                            Tool.SleepAWhile(3, 0);

                            if ( retry > 30*20 ) // > 10 mins  / ---1 mins
                                //throw new Exception("attribute result file not exists! videoObjectExtractionResultFilename="+videoObjectExtractionAttributeFilename+" command="+command);
                                throw new DataProcessingException("attribute result file not exists! videoObjectExtractionResultFilename="+videoObjectExtractionAttributeFilename+" command="+command,"video_process_error.detect_people_attributes_failed");
 
                            continue;
                        }

                        Tool.SleepAWhile(3, 0);
                        break;
                    }

                    if ( needToReRun )
                        continue;
                    
                    break;
                }
                catch(Exception e)
                {
                    log.info(" process extract people attributes failed! e="+e);
                    throw new DataProcessingException("process extract people attributes failed! e="+e,"video_process_error.detect_people_attributes_failed");
                }
                finally
                {
                    lock.unlock();
                }
            }
            else
            {
                log.info(" try to get lock failed! i="+i);
            }
        }       
       
        // get result csv file, process csv file   
        File fr=new File(videoObjectExtractionAttributeFilename);
        if ( !fr.exists() )
        {
            log.warn(" attribute result file not exist!  videoObjectExtractionAttributeFilename="+videoObjectExtractionAttributeFilename);
            //result.put("videoObjectData", dataList);
            throw new Exception("attribute result file not exists! videoObjectExtractionResultFilename="+videoObjectExtractionAttributeFilename+" command="+command);
            //return result;
        }
        
        String lastPictureFilename = "";
        String pictureFilepath = "";
        List<String> rows = FileUtil.readTxtFileToStringList(videoObjectExtractionAttributeFilename, EncodingType.UTF_8.getValue(), 2, 0,true);
       
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
            log.info("111111111 row = "+row);
            
            String[] columns = row.split("\\,");
            
            int objectTypeId = columns[2].toLowerCase().equals("person")?1:0;
            
            if ( objectTypeId != 1)
            {
                log.info(" objectType ="+columns[2]+" skip");
                continue;
            }
                       
            String pictureFilename = columns[0];
            
            if ( pictureFilename == null || pictureFilename.trim().isEmpty() )
            {
                continue;
            }
            
            if ( !lastPictureFilename.equals(pictureFilename) && !lastPictureFilename.isEmpty() )
            {          
                pictureObjectData.setObjectFeatures(objectFeatures);
                pictureObjectData.setObjectTypes(objectTypes);
                pictureObjectData.setPictureFilename(lastPictureFilename);
                pictureObjectData.setObjectPositions(objectPositions);
                
                pictureFilepath = String.format("%s/img/%s",edfServerWorkingDirectory,lastPictureFilename);  log.info(" pictureFilepath="+pictureFilepath);
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
                String p = Tool.removeLastString(columns[3],".");
                position.put("top_x", Integer.parseInt(p));

                p = Tool.removeLastString(columns[4],".");
                position.put("top_y", Integer.parseInt(p));

                p = Tool.removeLastString(columns[5],".");
                position.put("bottom_x", Integer.parseInt(p));

                p = Tool.removeLastString(columns[6],".");
                position.put("bottom_y", Integer.parseInt(p));
            }
            catch(Exception e)
            {
                log.error(" position failed! e="+e+" row="+row);
                continue;
            }
            
            objectPositions.add(position);
                                        
            Map<String,String> features = new HashMap<>();
            
            Map<Integer,String> positionIndexMapping = new HashMap<>();
            positionIndexMapping.put(1, "1");
            positionIndexMapping.put(2, "2");
            positionIndexMapping.put(3, "3");
            positionIndexMapping.put(4, "3");
            positionIndexMapping.put(5, "4");
            positionIndexMapping.put(6, "4");
            positionIndexMapping.put(7, "5");
            positionIndexMapping.put(8, "5");
            positionIndexMapping.put(9, "5");

            int featureStartColumn = 7; //
            int featureNumber = 9;
            
            for(int j=featureStartColumn;j<featureStartColumn+featureNumber;j++)
            {
                int ii = j-featureStartColumn+1;
                
                String featureIndex = positionIndexMapping.get(ii);
                
                if ( featureIndex == null )
                   featureIndex = "0";
                
                features.put(String.format("%s",featureIndex),columns[j]);
            }
                                         
            objectFeatures.add(features);
 
            objectTypes.add(objectTypeId);      
           
            lastPictureFilename = pictureFilename;
        }
        
        if ( !lastPictureFilename.isEmpty() )
        {
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
        }
                                  
        result.put("videoObjectData", dataList);
        
        return result;
    }            
}
