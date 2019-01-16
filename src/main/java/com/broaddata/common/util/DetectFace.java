//package com.broaddata.common.util;
//
//import com.broaddata.common.thrift.dataservice.DataServiceException;
//import java.nio.ByteBuffer;
//import org.apache.log4j.Logger;
//import org.opencv.core.*;
//import org.opencv.highgui.Highgui;
//import org.opencv.objdetect.CascadeClassifier;
//import sun.misc.BASE64Decoder;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.UUID;
//import org.apache.commons.lang.exception.ExceptionUtils;
//import org.apache.thrift.TException;
//
///**
// * Created by Duenan on 2018/4/13.
// */
//public class DetectFace {
//
//    static final Logger log = Logger.getLogger("DocumentSimilarityUtil");
//    private static List<Map<String, String>> resultMsg = null;
//    private static boolean isLoaded = false;
//
//    public List<Map<String, String>> getResultMsg() {
//        return resultMsg;
//    }
//
//    public void setResultMsg(List<Map<String, String>> resultMsg) {
//        this.resultMsg = resultMsg;
//    }
//
//    public static List<CoordinatConfig> detectFaces(String sourceFilename, String outputFilename, String faceFilePath) {
//
//        log.info("begin to detectFaces");
//        Mat image = null;
//        CascadeClassifier faceDetector = null;
//        String xmlfilePath = DetectFace.class.getClassLoader().getResource("lbpcascade_frontalface.xml").getPath().substring(1);
//        try {
//            faceDetector = new CascadeClassifier(xmlfilePath);
//            image = Highgui.imread(sourceFilename);
//        } catch (Throwable e) {
//            if (e instanceof UnsatisfiedLinkError) {
//                System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
//                faceDetector = new CascadeClassifier(xmlfilePath);
//                image = Highgui.imread(sourceFilename);
//            }
//            if (e instanceof Exception) {
//                e.printStackTrace();
//            }
//        }
//        MatOfRect faceDetections = new MatOfRect();
//        faceDetector.detectMultiScale(image, faceDetections);
//        log.info("how many faces=" + faceDetections.toArray().length);
//        List<CoordinatConfig> coordinateList = new ArrayList<>();
//        Rect rect = new Rect();
//        CoordinatConfig coordinatConfig = null;
//        for (int i = 0; i < faceDetections.toArray().length; i++) {
//            coordinatConfig = new CoordinatConfig();
//            rect = faceDetections.toArray()[i];
//            coordinatConfig.setX(rect.x);
//            coordinatConfig.setY(rect.y);
//            coordinatConfig.setWidth(rect.width);
//            coordinatConfig.setHeight(rect.height);
//            Core.rectangle(image, new Point(rect.x, rect.y), new Point(rect.x + rect.width, rect.y + rect.height), new Scalar(0, 255, 0));
//            coordinateList.add(coordinatConfig);
//        }
//
//        System.out.println(String.format("Writing %s", outputFilename));
//        Highgui.imwrite(outputFilename, image);
//        log.info("coordinateList=" + coordinateList);
//        return coordinateList;
//    }
//
//    public static List<Map<String, String>> detectPictureFacess(ByteBuffer pictureContent) throws DataServiceException, TException {
//        try {
//            String fileName = UUID.randomUUID().toString();
//
//            String sourceFile = String.format("%s%s.jpg", System.getProperty("java.io.tmpdir"), fileName);
//            FileUtil.writeFileFromByteBuffer(sourceFile, pictureContent);
//
//            //String outputFile = String.format("%s/recog_%s.jpg",System.getProperty("java.io.tmpdir"),fileName);
//            log.info("begin to detectFaces");
//            Mat image = null;
//            CascadeClassifier faceDetector = null;
//            String xmlfilePath = DetectFace.class.getClassLoader().getResource("lbpcascade_frontalface.xml").getPath();
//            log.info(" xmlfilePath1=" + xmlfilePath);
//            xmlfilePath = System.getProperty("user.dir") + "\\lbpcascade_frontalface.xml";
//            /*if ( xmlfilePath.startsWith("/") )
//                xmlfilePath = xmlfilePath.substring(1);
//            else
//                xmlfilePath = xmlfilePath.replace("file:/", "");*/
//
//            log.info(" xmlfilePath2=" + xmlfilePath);
//            try {
//                faceDetector = new CascadeClassifier(xmlfilePath);
//                image = Highgui.imread(sourceFile);
//            } catch (Throwable e) {
//                log.error(" failed!  e=" + e);
//                /* System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
//                 faceDetector = new CascadeClassifier(xmlfilePath);
//                 image = Highgui.imread(sourceFile); */
//                if (e instanceof UnsatisfiedLinkError) {
//                    System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
//                    faceDetector = new CascadeClassifier(xmlfilePath);
//                    image = Highgui.imread(sourceFile);
//                }
//            }
//            MatOfRect faceDetections = new MatOfRect();
//            faceDetector.detectMultiScale(image, faceDetections);
//            log.info("how many faces=" + faceDetections.toArray().length);
//            List<Map<String, String>> faceCoordinateList = new ArrayList<>();
//            Rect rect = new Rect();
//            for (int i = 0; i < faceDetections.toArray().length; i++) {
//                rect = faceDetections.toArray()[i];
//                Map<String, String> map = new HashMap<>();
//                map.put("label", String.valueOf((i + 1)));
//                map.put("x", String.valueOf((rect.x - 15) > 0 ? rect.x - 15 : 0));
//                map.put("y", String.valueOf((rect.y - 15) > 0 ? rect.y - 15 : 0));
//                map.put("width", String.valueOf(rect.width + 10));
//                map.put("height", String.valueOf(rect.height + 18));
//                faceCoordinateList.add(map);
//            }
//            log.info("faceCoordinateList=" + faceCoordinateList);
//            return faceCoordinateList;
//        } catch (Exception e) {
//            String errorInfo = "detectPictureFaces() failed! e=" + e + " stacktrace=" + ExceptionUtils.getStackTrace(e);
//            log.error(errorInfo);
//
//            throw new DataServiceException(errorInfo);
//
//        }
//
//    }
//
//    public static double calcArea(Rect rect) {
//        return rect.width * rect.height;
//    }
//}
