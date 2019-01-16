/*
 * KafkaProcessor.java
 */

package com.broaddata.common.processor;

import com.broaddata.common.model.enumeration.VideoAnalysisAlertType;
import org.apache.log4j.Logger;

import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte; 
import org.opencv.videoio.VideoCapture;
import org.opencv.imgcodecs.Imgcodecs;
import org.apache.commons.lang3.exception.ExceptionUtils;
import java.util.Base64;
import java.util.Date;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.json.JSONObject;
import org.opencv.videoio.Videoio;

import com.broaddata.common.util.Tool;
import com.broaddata.common.util.DataServiceConnector;
import com.broaddata.common.model.platform.ComputingNode;
import com.broaddata.common.thrift.dataservice.CreateVideoConnectionJob;
import com.broaddata.common.util.Counter;
import com.broaddata.common.util.KafkaUtil;
import com.broaddata.common.util.RCUtil;

public class VideoProcessor implements Runnable
{
    static final Logger log = Logger.getLogger("VideoProcessor");
 
    private String dataServiceIps;
    private volatile DataServiceConnector dsConn;
    private double framePerSecond = 1;
      
    private CreateVideoConnectionJob job;
    private ComputingNode computingNode;
    
    private volatile String errorInfo = "";
    private Counter serviceCounter;
    private int organizationId;
   
    public VideoProcessor(int organizationId,String dataServiceIps,CreateVideoConnectionJob job,ComputingNode computingNode,Counter serviceCounter) 
    {
        this.organizationId = organizationId;
        this.dataServiceIps = dataServiceIps;
        this.job = job;
        this.computingNode = computingNode;
        this.serviceCounter = serviceCounter;
    }
  
    @Override
    public void run()
    {
        String videoUrl = job.getStreamAddress();
        double framePerSecond = job.getFramePerSecond(); // 每秒取几帧
        KafkaProducer<String, String> producer = null;
        
        //framePerSecond = 2;
   
        try
        {
            System.loadLibrary( Core.NATIVE_LIBRARY_NAME );
        }
        catch(Exception e)
        {
            log.warn(" loadlibary failed! e="+e);
        }
        
        /*
        for(int alertTypeId : job.getAlertTypeList())
        {
            if ( alertTypeId == VideoAnalysisAlertType.DROWSINESS_CHECK.getValue() )
            {
                try
                {
                    String cameraStreamAddress = "";

                    String tmpOutputDir = "/tmp/drowsiness-detetion";
                    String errorOutFile = "/tmp/startDrowsinessCheck.error";
                    String commandStr= String.format("python /home/edf/devel/drowsiness-detection/detect_drowsiness.py --id %s -d %s -s %s \n",job.cameraId,tmpOutputDir,cameraStreamAddress);

                    RCUtil.executeCommand(commandStr, null, true, errorOutFile, null, true);    
                }
                catch (Exception e) 
                {
                    String errorInfo = "getVideoAnalysisAlertList() failed! e="+e+ " stacktrace="+ExceptionUtils.getStackTrace(e);
                    log.error(errorInfo);
                    return;
                }  

                while(true)
                {
                    // check output folder
                    
                    
                    
                    // send to kafka
                    
                    Tool.SleepAWhile(1, 0);
                } 
            }
        }
        */
        // no drowsiness check
        
        while(true)
        {
            try
            {
                log.info(" init kafka! ip="+job.getKafkaConfig());
                
                producer = KafkaUtil.getKafkaProducer(job.getKafkaConfig());
 
                VideoCapture capture = new VideoCapture(); 

                if( videoUrl.equals("0") )
                {
                    if(!capture.open(0)) 
                    {
                        String errorInfo = String.format(" can not open local camera");
                        throw new Exception(errorInfo);
                    }
                }
                else            
                if(!capture.open(videoUrl)) 
                {
                    String errorInfo = String.format(" can not open video stream! url="+videoUrl);
                    throw new Exception(errorInfo);
                }

                dsConn = new DataServiceConnector();
               
                log.info("Begin to read video from camera: " + videoUrl+" job.getKafkaConfig()="+job.getKafkaConfig());

                int nextFramePosition = 0;            

                double increase = 24/framePerSecond;
                 
                int i = 0;
                while (true)
                {
                    i++;
                    
                    Date startTime = new Date();
                    
                    log.info(" read video stream .... frame="+nextFramePosition);

                    Mat frame = new Mat();
                      
                    capture.set(Videoio.CV_CAP_PROP_POS_FRAMES, (int)nextFramePosition);

                    capture.read(frame);

                    MatOfByte mob = new MatOfByte();

                    try
                    {
                        Imgcodecs.imencode(".jpg", frame, mob);
                    }
                    catch(Exception e)
                    {
                        log.info(" imagcodecs failed! e="+e);
                        mob = null;
                    }
                    
                    if ( mob != null )
                    {
                        byte[] frameByteArray = mob.toArray();

                        JSONObject obj = new JSONObject();

                        obj.put("clientNodeId",computingNode.getId());
                        obj.put("organizationId",organizationId);
                        obj.put("cameraId",job.getCameraId());
                        obj.put("frameCapturedTime", new Date().getTime());
                        obj.put("frameData", Base64.getEncoder().encodeToString(frameByteArray));
                        obj.put("framePerSecond",framePerSecond);

                        String json = obj.toString();
 
                        KafkaUtil.sendMessage(producer, "video-frames", "frame", json);

                        KafkaUtil.sendMessage(producer, "video-frames-for-alert", "frame", json);
                        
                        log.info(" finish sending ....");
                    }
                    
                    nextFramePosition += increase;
                        
                    if ( i%120 == 0 )
                    {
                        log.info("99999999999999999999999999999999999 updateVideoConnectionLatestStatus() .......... i="+i);
                        dsConn.getClient(dataServiceIps);
                        dsConn.getClient().updateVideoConnectionLatestStatus(organizationId, job.getCameraId(), computingNode.getId(), "");
                        dsConn.close();
                    }
                                        
                    double needToSleepTime = 1000.00/framePerSecond - (new Date().getTime()-startTime.getTime());
                       
                    if ( needToSleepTime > 0 )
                        Tool.SleepAWhileInMS((int)needToSleepTime);
                    
                    Tool.SleepAWhile(3, 0);
                }
            }
            catch(Exception e)
            {
                log.error(" VideoProcessor.run() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));    
                Tool.SleepAWhile(3, 0);
            }
            finally
            {
                dsConn.close();
                
                if( producer != null )
                 producer.close();
            }
        }
    }
}
