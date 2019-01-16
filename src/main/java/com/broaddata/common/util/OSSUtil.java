/*
 * OSSUtil.java
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.ByteArrayInputStream;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.aliyun.openservices.ClientException;
import com.aliyun.openservices.ClientConfiguration;
import com.aliyun.openservices.oss.OSSException;
import com.aliyun.openservices.oss.OSSClient;
import com.aliyun.openservices.oss.model.GetObjectRequest;
import com.aliyun.openservices.oss.model.ObjectMetadata;
import com.aliyun.openservices.oss.model.PartETag;
import com.aliyun.openservices.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.openservices.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.openservices.oss.model.InitiateMultipartUploadResult;
import com.aliyun.openservices.oss.model.OSSObject;
import com.aliyun.openservices.oss.model.OSSObjectSummary;
import com.aliyun.openservices.oss.model.ObjectListing;
import com.aliyun.openservices.oss.model.UploadPartRequest;
import com.aliyun.openservices.oss.model.UploadPartResult;


public class OSSUtil 
{
    static final Logger log=Logger.getLogger("OSSUtil");
        
    private static final long PART_SIZE = 5 * 1024 * 1024L; // 每个Part的大小，最小为5MB
    private static final int CONCURRENCIES = 3; // 上传Part的并发线程数。

    public static byte[] getContent(String contentId)
    {
        try 
        {
            OSSClient client = getClient(CommonKeys.OSS_ENDPOINT, CommonKeys.OSS_ACCESS_ID, CommonKeys.OSS_ACCESS_KEY);            
            return getDataobject(client, CommonKeys.OSS_BUCKETNAME,contentId);
        } 
        catch (Exception e) 
        {
            log.error("getContentFromOSS failed! e="+e);
            return null;
        }
    }
    
    public static OSSClient getClient(String OSS_ENDPOINT,String access_id,String access_key) throws Exception
    {
        OSSClient client = null;
        ClientConfiguration config = null;
        
        try 
        {
            config = new ClientConfiguration();
            client = new OSSClient(OSS_ENDPOINT,access_id,access_key,config);
        }
        catch (Exception e) 
        {
            log.error("Connect OSS failed! e="+e);
            throw e;
        }
        return client;
    }
    
    public static void storeDataobject(OSSClient client, String bucketName, String key, String filename, ObjectMetadata objectMetadata) throws Exception
    {
        try 
        {
            File file = new File(filename);
            objectMetadata.setContentLength(file.length());
           
            InputStream input = new FileInputStream(file);
            client.putObject(bucketName, key, input, objectMetadata);
        }
        catch (Exception e) 
        {
            log.error("storeDocument() failed! e="+e);
            throw e;
        }
    }
    
    public static void storeDataobject(OSSClient client, String bucketName, String key, byte[] fileStream, ObjectMetadata objectMetadata) throws Exception
    {
        try 
        {          
            objectMetadata.setContentLength(fileStream.length);
           
            InputStream input = new ByteArrayInputStream(fileStream);
            client.putObject(bucketName, key, input, objectMetadata);
        } 
        catch (Exception e) 
        {
            log.error("backupDocument() failed! e="+e);
            throw e;
        }
    }
    
    public static void getDataobject(OSSClient client, String bucketName, String key, String filename) throws Exception 
    {
        try 
        {
            client.getObject(new GetObjectRequest(bucketName, key),new File(filename));
        }
        catch (Exception e) 
        {
            log.error("getDocument() failed! e="+e);
            throw e;
        }
    }
    
    public static byte[] getDataobject(OSSClient client, String bucketName, String key) throws Exception 
    {
        try 
        {
            OSSObject obj=client.getObject(new GetObjectRequest(bucketName, key));        
            return FileUtil.readInputStreamToByteArray(obj.getObjectContent());
        }
        catch (Exception e) 
        {
            log.error("getDocument() failed! e="+e);
            throw e;
        }
    }
    
    public static void storeBigDataobject(OSSClient client, String bucketName, String key,File uploadFile) throws Exception 
    {
        try 
        {
            int partCount = calPartCount(uploadFile);
            if (partCount <= 1) {
                throw new Exception("no need to use multiple part upload");
            }
            
            String uploadId = initMultipartUpload(client, bucketName, key);
            
            ExecutorService pool = Executors.newFixedThreadPool(CONCURRENCIES);
            
            List<PartETag> eTags = Collections.synchronizedList(new ArrayList<PartETag>());
            
            for (int i = 0; i < partCount; i++) 
            {
                long start = PART_SIZE * i;
                long curPartSize = PART_SIZE < uploadFile.length() - start
                        ? PART_SIZE : uploadFile.length() - start;
                
                pool.execute(new UploadPartThread(client, bucketName, key,
                        uploadFile, uploadId, i + 1,
                        PART_SIZE * i, curPartSize, eTags));
            }
            
            pool.shutdown();
            while (!pool.isTerminated()) {
                pool.awaitTermination(5, TimeUnit.SECONDS);
            }
            
            if (eTags.size() != partCount) {
                throw new IllegalStateException("Multipart上传失败，有Part未上传成功。");
            }
            
            completeMultipartUpload(client, bucketName, key, uploadId, eTags);
        }         
        catch (Exception e) 
        {
            log.error("upload big file failed! e="+e);
            throw e;
        }
    }

    // 根据文件的大小和每个Part的大小计算需要划分的Part个数。
    private static int calPartCount(File f) 
    {
        int partCount = (int) (f.length() / PART_SIZE);
        if (f.length() % PART_SIZE != 0){
            partCount++;
        }
        return partCount;
    }

    // 初始化一个Multi-part upload请求。
    private static String initMultipartUpload(OSSClient client, String bucketName, String key) throws OSSException, ClientException 
    {
        InitiateMultipartUploadRequest initUploadRequest =
                new InitiateMultipartUploadRequest(bucketName, key);
        InitiateMultipartUploadResult initResult =
                client.initiateMultipartUpload(initUploadRequest);
        String uploadId = initResult.getUploadId();
        return uploadId;
    }

    // 完成一个multi-part请求。
    private static void completeMultipartUpload(OSSClient client,String bucketName, String key, String uploadId, List<PartETag> eTags)
            throws OSSException, ClientException 
    {
        //为part按partnumber排序
        Collections.sort(eTags, new Comparator<PartETag>()
        {
            public int compare(PartETag arg0, PartETag arg1) 
            {
                PartETag part1= arg0;
                PartETag part2= arg1;

                return part1.getPartNumber() - part2.getPartNumber();
            }  
        });

        CompleteMultipartUploadRequest completeMultipartUploadRequest =
                new CompleteMultipartUploadRequest(bucketName, key, uploadId, eTags);

        client.completeMultipartUpload(completeMultipartUploadRequest);
    }

    private static class UploadPartThread implements Runnable 
    {
        private File uploadFile;
        private String bucket;
        private String object;
        private long start;
        private long size;
        private List<PartETag> eTags;
        private int partId;
        private OSSClient client;
        private String uploadId;

        UploadPartThread(OSSClient client,String bucket, String object,
                File uploadFile,String uploadId, int partId,
                long start, long partSize, List<PartETag> eTags) 
        {
            this.uploadFile = uploadFile;
            this.bucket = bucket;
            this.object = object;
            this.start = start;
            this.size = partSize;
            this.eTags = eTags;
            this.partId = partId;
            this.client = client;
            this.uploadId = uploadId;
        }

        @Override
        public void run() 
        {
            InputStream in = null;
            try 
            {
                in = new FileInputStream(uploadFile);
                in.skip(start);

                UploadPartRequest uploadPartRequest = new UploadPartRequest();
                uploadPartRequest.setBucketName(bucket);
                uploadPartRequest.setKey(object);
                uploadPartRequest.setUploadId(uploadId);
                uploadPartRequest.setInputStream(in);
                uploadPartRequest.setPartSize(size);
                uploadPartRequest.setPartNumber(partId);

                UploadPartResult uploadPartResult = client.uploadPart(uploadPartRequest);

                eTags.add(uploadPartResult.getPartETag());

            } catch (Exception e) {
            } finally {
                if (in != null) try { in.close(); } catch (Exception e) {}
            }
        }
    }
 
    public static void createBucket(OSSClient client, String bucketName) throws Exception
    {
        try 
        {
            if (client.doesBucketExist(bucketName)) {
                return;
            }
            //创建bucket
            client.createBucket(bucketName);
        }         
        catch (Exception e) 
        {
            log.error("createBucket() failed! e="+e);
            throw e;
        }
    }

    public static void deleteBucket(OSSClient client, String bucketName) throws Exception 
    {
        try
        {
            ObjectListing ObjectListing = client.listObjects(bucketName);
            List<OSSObjectSummary> listDeletes = ObjectListing.getObjectSummaries();
            for (int i = 0; i < listDeletes.size(); i++) {
                String objectName = listDeletes.get(i).getKey();
                // 如果不为空，先删除bucket下的文件
                client.deleteObject(bucketName, objectName);
            }
            client.deleteBucket(bucketName);
        }         
        catch (Exception e) 
        {
            log.error("deleteBucket() failed! e="+e);
            throw e;
        }
    }
}
