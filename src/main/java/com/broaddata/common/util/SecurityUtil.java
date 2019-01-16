
package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.persistence.EntityManager;
 
import com.broaddata.common.model.enumeration.UserType;
import com.broaddata.common.model.organization.ResourceAcl;
import com.broaddata.common.model.platform.UserInfo;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.log4j.Logger;

public class SecurityUtil  
{
    static final Logger log = Logger.getLogger("SecurityUtil");
    private static final String SYSTEM_DIGEST_ALGORITHM = "SHA";
     
    public static String generateHashKey(String value)
    {
        try 
        {
            byte[] keyBytes = value.getBytes("utf-8");
             
            MessageDigest sha = MessageDigest.getInstance(SYSTEM_DIGEST_ALGORITHM);
            sha.update(keyBytes);
           
            return FileUtil.byteArrayToHex(sha.digest());
        } 
        catch (Exception e) 
        {
            log.error("failed to generateHashKey()! e="+e+" value="+value);           
            return value;
        }
    }
        
    public static String generateUserInitalPassword(int organizationId, UserInfo userInfo)
    {
        return userInfo.getName().trim();
    }
    
    public static String generatePasswordInSHA(String userName, String password)
    {
        String str = null;
         
        try 
        {
            str = String.format("edf%s%s",userName.trim(),password.trim()); log.info("generatePasswordInSHA str="+str);
            
            MessageDigest sha = MessageDigest.getInstance(SYSTEM_DIGEST_ALGORITHM);
            sha.update(str.getBytes("utf-8"));
           
            return FileUtil.byteArrayToHex(sha.digest());
        } 
        catch (NoSuchAlgorithmException | UnsupportedEncodingException e) 
        {
            log.error("failed to generateDocumentId! e="+e+" str = "+str);           
            return null;
        }
    }
         
    public static boolean checkPermission(EntityManager platformEm, EntityManager em, int organizationId, int userId, int resourceTypeId, String resourceId, int resourcePermissionTypeId) throws Exception 
     {
        Map<String, Object> map;
        String sql;
        ResourceAcl resourceAcl;
        List<Map<String,Object>> userInfo = new ArrayList<>();
                
        // user
        map = new HashMap<>();
        map.put("userType", UserType.USER.getValue());
        map.put("userIds", new Integer[]{userId});
        userInfo.add(map);
        
        map = Util.getGroupRoleUserForASingleUser(platformEm,UserType.USER_GROUP,userId);
        
        userInfo.add(map);
        map = Util.getGroupRoleUserForASingleUser(platformEm,UserType.ROLE,userId);
        userInfo.add(map);
        
        for(Map<String,Object> user : userInfo)
        {
            int userTypeId = (int)user.get("userType");
            Integer[] userIds = (Integer[])user.get("userIds");
            
            for(int i=0;i<userIds.length;i++)
            {
                // user
                if ( resourceId == null || resourceId.trim().isEmpty() )
                    sql = String.format("from ResourceAcl where organizationId=%d and resourceType=%d and userType=%d and userId=%d and ra.resourceId='%s'",organizationId,resourceTypeId,userTypeId,userIds[i],"0");
                else
                    sql = String.format("from ResourceAcl where organizationId=%d and resourceType=%d and userType=%d and userId=%d and ( resourceId='%s' or resourceId='0')",organizationId,resourceTypeId,userTypeId,userIds[i],resourceId);
                
                //log.info(" hasPermission sql="+sql);
                try {
                    resourceAcl = (ResourceAcl)em.createQuery(sql).getSingleResult();
                }
                catch(Exception e)
                {
                    continue;
                }
                
                String[] aclArray = resourceAcl.getResourceAcl().split("\\,");
                for(String acl : aclArray)
                {
                    if ( Integer.parseInt(acl) == resourcePermissionTypeId )
                        return true;
                }
            }
        }
        
        return false;
    }
     
    public static String processEncyption(String content ,String key)
    {
        String[] values = content.split(" ");
        
        StringBuilder builder = new StringBuilder();
        for(String value : values)
        {
            String encrypted = encrypt(value, key);
            builder.append(encrypted).append(" ");
        }
        
        return builder.toString();
    }
    

    public static String encrypt(String content, String password) 
    {
        try {           
                KeyGenerator kgen = KeyGenerator.getInstance("AES");
                kgen.init(128, new SecureRandom(password.getBytes()));
                SecretKey secretKey = kgen.generateKey();
                byte[] enCodeFormat = secretKey.getEncoded();
                SecretKeySpec key = new SecretKeySpec(enCodeFormat, "AES");
                Cipher cipher = Cipher.getInstance("AES");// 创建密码器
                byte[] byteContent = content.getBytes("utf-8");
                cipher.init(Cipher.ENCRYPT_MODE, key);// 初始化
                byte[] result = cipher.doFinal(byteContent);

                return parseByte2HexStr(result);
        } catch (Exception e) {
                e.printStackTrace();
        } 
        return null;
    }

    public static String decrypt(String contentStr, String password) 
    {
        try {
                byte[] content = parseHexStr2Byte(contentStr);
                 KeyGenerator kgen = KeyGenerator.getInstance("AES");
                 kgen.init(128, new SecureRandom(password.getBytes()));
                 SecretKey secretKey = kgen.generateKey();
                 byte[] enCodeFormat = secretKey.getEncoded();
                 SecretKeySpec key = new SecretKeySpec(enCodeFormat, "AES");            
                 Cipher cipher = Cipher.getInstance("AES");// 创建密码器
                cipher.init(Cipher.DECRYPT_MODE, key);// 初始化
                byte[] result = cipher.doFinal(content);
                return new String(result,"UTF-8");
        } catch (Exception e) {
                e.printStackTrace();
        }  
        return null;
    }


    public static String parseByte2HexStr(byte buf[]) 
    {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < buf.length; i++) {
                String hex = Integer.toHexString(buf[i] & 0xFF);
                if (hex.length() == 1) {
                        hex = '0' + hex;
                }
                sb.append(hex.toUpperCase());
        }
        return sb.toString();
    }

    public static byte[] parseHexStr2Byte(String hexStr) 
    {
        if (hexStr.length() < 1)
                return null;
        byte[] result = new byte[hexStr.length()/2];
        for (int i = 0;i< hexStr.length()/2; i++) {
                int high = Integer.parseInt(hexStr.substring(i*2, i*2+1), 16);
                int low = Integer.parseInt(hexStr.substring(i*2+1, i*2+2), 16);
                result[i] = (byte) (high * 16 + low);
        }
        return result;
    }
}