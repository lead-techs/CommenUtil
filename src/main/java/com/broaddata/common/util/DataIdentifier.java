/*
 * SystemSecurity.java
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

public class DataIdentifier 
{
    static final Logger log=Logger.getLogger("DataIdentifier");
    private static final String SYSTEM_DIGEST_ALGORITHM = "SHA";
    
    public static String generateContentId(ByteBuffer documentContent)
    {
        try
        {
            Integer in = 0;
            MessageDigest sha = MessageDigest.getInstance(SYSTEM_DIGEST_ALGORITHM);
            sha.update(documentContent);
           
            return FileUtil.byteArrayToHex(sha.digest());
        } 
        catch (Exception e)
        {
            log.error("failed to generateContentId! e="+e);       
            return null;
        }       
    }
    
    public static String generateContentId(byte[] documentContent)
    {
        try
        {
            MessageDigest sha = MessageDigest.getInstance(SYSTEM_DIGEST_ALGORITHM);
            sha.update(documentContent);
           
            return FileUtil.byteArrayToHex(sha.digest());
        } 
        catch (Exception e)
        {
            log.error("failed to generateContentId! e="+e);       
            return null;
        }       
    }
        
    public static String generateDataobjectId(int organizationId, int sourceApplicationId, int datasourceType, int datasourceId, String fullpathName,int repositoryId)
    {
        try 
        {
            String documentIdStr=String.format("%011d%011d%011d%d%d",organizationId,sourceApplicationId,datasourceType,datasourceId,repositoryId);
            byte[] keyBytes = byteMerger(documentIdStr.getBytes("utf-8"),fullpathName.getBytes("utf-8"));
                     
            MessageDigest sha = MessageDigest.getInstance(SYSTEM_DIGEST_ALGORITHM);
            sha.update(keyBytes);
           
            return FileUtil.byteArrayToHex(sha.digest());
        } 
        catch (Exception e) 
        {
            log.error("failed to generateDocumentId! e="+e+"-"+fullpathName);           
            return null;
        }
    }
      
    public static String generateHash(String inputStr) throws Exception
    {
        try 
        {
            byte[] keyBytes = inputStr.getBytes("utf-8");
                     
            MessageDigest sha = MessageDigest.getInstance(SYSTEM_DIGEST_ALGORITHM);
            sha.update(keyBytes);
           
            return FileUtil.byteArrayToHex(sha.digest());
        } 
        catch (Exception e) 
        {
            log.error("failed to generateHash() ! e="+e+"-"+inputStr);           
            throw e;
        }
    }
    
    public static String generateDataobjectChecksum(int organizationId, String dataobjectId, String dataobjectMetadataStr)
    {
        try 
        {
            String checkmsumStr=String.format("%011d%s",organizationId,dataobjectId);
            byte[] keyBytes = byteMerger(checkmsumStr.getBytes("utf-8"),dataobjectMetadataStr.getBytes("utf-8"));
             
            MessageDigest sha = MessageDigest.getInstance(SYSTEM_DIGEST_ALGORITHM);
            sha.update(keyBytes);
           
            return FileUtil.byteArrayToHex(sha.digest());
        } 
        catch (Exception e) 
        {
            log.error("failed to generateDataobjectChecksum()! e="+e+" dataobjectId="+dataobjectId+" dataobjectMetadataStr="+dataobjectMetadataStr);           
            return null;
        }
    }
    
    public static byte[] byteMerger(byte[] byte_1, byte[] byte_2){  
        byte[] byte_3 = new byte[byte_1.length+byte_2.length];  
        System.arraycopy(byte_1, 0, byte_3, 0, byte_1.length);  
        System.arraycopy(byte_2, 0, byte_3, byte_1.length, byte_2.length);
        
        return byte_3;
    } 
}
