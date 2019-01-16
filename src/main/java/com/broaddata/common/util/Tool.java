/*
 * Tool.java
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.Normalizer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import net.sourceforge.pinyin4j.PinyinHelper;
import net.sourceforge.pinyin4j.format.HanyuPinyinCaseType;
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat;
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType;
import org.dom4j.Document;
import org.dom4j.io.SAXReader;
import org.json.JSONArray;
import org.json.JSONObject;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import java.io.InputStream;
import java.util.Locale;
import java.util.Properties;
import com.jcraft.jsch.Channel;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import com.broaddata.common.exception.ParsingDatetimeException;
import com.broaddata.common.model.enumeration.CompareType;
import com.broaddata.common.model.enumeration.ContentType;
import com.broaddata.common.model.enumeration.TimeFolderType;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Tool 
{
    static final Logger log = Logger.getLogger("Tool");
    static String SPACE = "   "; 
           
    public static String extractValueByRE(String content,String regularExpression,int numberOfMatch)
    {
        String fieldValue = "";
        
        try
        {
            Pattern pattern = Pattern.compile(regularExpression);

            Matcher m = pattern.matcher(content);
            boolean found = false;

            for(int i=0;i<numberOfMatch;i++)
            {
                found = m.find();
                if ( !found )
                    break;
            }

            if ( found )
                fieldValue = m.group(); 
        }
        catch(Exception e)
        {
            log.error("extractValueByRE() failed! e="+e);
        }
                
        return fieldValue;
    }
    
    public static String getSystemTmpdir()
    {
        String tmpdir = System.getProperty("java.io.tmpdir");
        
        if ( tmpdir.endsWith("/") || tmpdir.endsWith("\\") )
            tmpdir = removeLastChars(tmpdir, 1);
        
        return tmpdir;
    }
    
    public static boolean isComparingMatched(CompareType comparingType,int value,int targetValue)
    {
        boolean matched = false;
          
        if ( comparingType == null || comparingType == comparingType.EQ )
        {
            if ( value == targetValue )
                matched = true;
        }
        else
        if ( comparingType == comparingType.LT )
        {
            if ( value < targetValue )
               matched = true;
        }              
        else
        if ( comparingType == comparingType.LTE )
        {
            if ( value <= targetValue  )
               matched = true;
        }   
        else
        if ( comparingType == comparingType.GT )
        {
            if ( value > targetValue )
               matched = true;
        }   
        else
        if ( comparingType == comparingType.GTE )
        {
            if ( value >= targetValue )
               matched = true;
        }   

        return matched;
    }
    
    public static boolean isComparingMatched1(CompareType comparingType,String value,String targetValue,String targetValue1)
    {
        boolean matched = false;
          
        if ( comparingType == null || comparingType == comparingType.EQ )
        {
            if ( value.equals(targetValue) )
                matched = true;
        }
        else
        if ( comparingType == comparingType.LIKE )
        {
            if ( value.startsWith(targetValue) )
                matched = true;
        }
        else
        if ( comparingType == comparingType.LT )
        {
            if ( value.compareTo(targetValue) > 0 )
               matched = true;
        }              
        else
        if ( comparingType == comparingType.LTE )
        {
            if ( value.compareTo(targetValue) <= 0 )
               matched = true;
        }   
        else
        if ( comparingType == comparingType.GT )
        {
            if ( value.compareTo(targetValue) > 0 )
               matched = true;
        }   
        else
        if ( comparingType == comparingType.GTE )
        {
            if ( value.compareTo(targetValue) >= 0 )
               matched = true;
        }   
        else
        if ( comparingType == comparingType.EBT )
        {
            if ( value.compareTo(targetValue)>0 && value.compareTo(targetValue1)<0 )
                 matched = true;
        }
        else
        if ( comparingType == comparingType.IBT )
        {
            if ( value.compareTo(targetValue)>=0 && value.compareTo(targetValue1)<=0 )
                 matched = true;
        }
        else
        if ( comparingType == comparingType.IN )
        {
            String[] vals = targetValue.split("\\`");
            
            for(String val : vals)
            {
                if ( value.equals(val) )
                {
                    matched = true;
                    break;
                }
            }
        }    

        return matched;
    }
    
    public static String getNotNumberInStrAtBeginning(String str)
    {
        String val = "";
        
        if ( str == null || str.isEmpty() )
            return val;
        
        for(int i=0;i<str.length();i++)
        {
            if ( str.charAt(i)>='0' && str.charAt(i)<='9' )
            {
                return val;
            }
            
            val += str.charAt(i);
        }
        
        return val;
    }
    
    public static String invokeRestfulService(String targetURL,String method,String requestBody,int retryTimes) throws Exception 
    {
        String result = "";

        int retry=0;
        
        while(true)
        {
            retry++;
            
            try 
            {
                log.info(" invokeRestfulService url= "+targetURL+",method="+method+" retry="+retry);
                
                URL restServiceURL = new URL(targetURL);

                HttpURLConnection httpConnection = (HttpURLConnection) restServiceURL.openConnection();
                
                if ( method == null )
                    httpConnection.setRequestMethod("GET");
                else
                    httpConnection.setRequestMethod(method);
                
                httpConnection.setRequestProperty("Accept", "application/json");
                
                if ( requestBody != null && !requestBody.trim().isEmpty() )
                {
                    httpConnection.setRequestProperty("Content-Type", "application/json");
                    httpConnection.setDoOutput(true);
                    OutputStream os = httpConnection.getOutputStream();
                    OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");    
                    osw.write(requestBody);
                    osw.flush();
                    osw.close();
                    os.close();  //don't forget to close the OutputStream
                }
                
                if (httpConnection.getResponseCode() != 200) 
                   throw new RuntimeException("HTTP GET Request Failed with Error code : " + httpConnection.getResponseCode());

                BufferedReader responseBuffer = new BufferedReader(new InputStreamReader(
                       (httpConnection.getInputStream())));

                String output;

                while ((output = responseBuffer.readLine()) != null) 
                {
                    //log.info(" output="+output);
                    result += output;
                }

                httpConnection.disconnect();
                 
                if ( result.isEmpty() && retry <= retryTimes )
                    continue;
                
                break;
            } 
            catch(Exception e)
            {
                //log.error(" invokeRestfulService() failed! e="+e);
                
                //if ( e.toString().contains("Error code : 500") )
                //    break;
                
                if ( retry > retryTimes )
                    throw e;
                
                Tool.SleepAWhileInMS(200);
            }
        }
        
        return result;
    }
     
    public static String secToTimeStr(int timeInSecond) 
    {  
        String timeStr = null;  
        int hour = 0;  
        int minute = 0;  
        int second = 0; 
        
        if (timeInSecond <= 0)  
            return "00:00";  
        else 
        {  
            minute = timeInSecond / 60;  
            
            if (minute < 60) 
            {  
                second = timeInSecond % 60;  
                timeStr = unitFormat(minute) + ":" + unitFormat(second);  
            }
            else 
            {  
                hour = minute / 60;  
                if (hour > 99)  
                    return "99:59:59";
                
                minute = minute % 60;  
                second = timeInSecond - hour * 3600 - minute * 60;  
                timeStr = unitFormat(hour) + ":" + unitFormat(minute) + ":" + unitFormat(second);  
            }  
        }  
        
        return timeStr;  
    }  
  
    public static String unitFormat(int i) 
    {  
        String retStr = null;  
        if (i >= 0 && i < 10)  
            retStr = "0" + Integer.toString(i);  
        else  
            retStr = "" + i;  
        
        return retStr;  
    }  
    
    public static void executeSSHCommandChannelExec(String host,int port,String user,String password,String command,int retryNumber) throws Exception
    {
        Date startTime = new Date();
        
        int retry = 0;
        
        while(true)
        {
            retry++;
            
            try
            {
                Properties config = new Properties();
                config.put("StrictHostKeyChecking", "no");

                log.info("executeSSHCommandChannelExec: ssh Connecting... host="+host+",port="+port+",user="+user+",password="+password+",command="+command);

                JSch jsch = new JSch();
                // Create a JSch session to connect to the server
                Session session = jsch.getSession(user, host, port);
                session.setPassword(password);
                session.setConfig(config);
                // Establish the connection
                session.connect();

                log.info("ssh Connected...");

                ChannelExec channel = (ChannelExec) session.openChannel("exec"); 
                channel.setCommand(command);
                channel.setErrStream(System.err);

                InputStream in = channel.getInputStream();
                channel.connect();
                byte[] tmp = new byte[1024];
                
                while (true) 
                {
                    while (in.available() > 0) 
                    {
                        int i = in.read(tmp, 0, 1024);
                        if (i < 0) {
                                break;
                        }
                        
                        log.info(new String(tmp, 0, i));
                    }

                    if (channel.isClosed()) 
                    {
                        log.error("Exit Status: " + channel.getExitStatus());
                        break;
                    }

                    Tool.SleepAWhile(1, 0);
                }

                channel.disconnect();
                session.disconnect();

                log.info("DONE!!! took "+(new Date().getTime() - startTime.getTime())+" ms");
            }
            catch(Exception e)
            {
                log.error(" executeSSHCommandChannelExec() failed! e="+e+",retry="+retry);
                
                if ( retry > retryNumber )
                    throw e;
                
                continue;
            }
                        
            break;
        } 
    }
    
    public static void executeSSHCommandChannelShell(String host,int port,String user,String password,String command,int retryNumber,String waitingStringToQuit) throws Exception
    {
        Date startTime = new Date();
        
        int retry = 0;
        
        while(true)
        {
            retry++;
            
            try
            {
                Properties config = new Properties();
                config.put("StrictHostKeyChecking", "no");

                log.info("executeSSHCommandChannelShell:ssh Connecting... host="+host+",port="+port+",user="+user+",password="+password+",command="+command);

                JSch jsch = new JSch();
                // Create a JSch session to connect to the server
                Session session = jsch.getSession(user, host, port);
                session.setPassword(password);
                session.setConfig(config);
                // Establish the connection
                session.connect();

                log.info("ssh Connected...");

                Channel channel = session.openChannel("shell"); 
                channel.connect(1000);
                
                InputStream instream = channel.getInputStream();
		OutputStream outstream = channel.getOutputStream();
                
                outstream.write(command.getBytes());
                //outstream.write("ls \n".getBytes());
		outstream.flush();
    
                Tool.SleepAWhileInMS(200);
                
                if ( waitingStringToQuit == null || waitingStringToQuit.isEmpty() )
                {
                    while(instream.available() > 0) 
                    {
                        byte[] data = new byte[instream.available()];
                        int nLen = instream.read(data);

                        if (nLen < 0) {
                            throw new Exception("network error.");
                        }

                        String temp = new String(data, 0, nLen,"iso8859-1");
                        log.info("recieve from ssh session:"+temp);

                        Tool.SleepAWhile(1, 0);
                    }
                }
                else
                {
                    while(true) 
                    {
                        byte[] data = new byte[instream.available()];
                        int nLen = instream.read(data);

                        if (nLen < 0) {
                            throw new Exception("network error.");
                        }

                        String temp = new String(data, 0, nLen,"iso8859-1");
                        log.info(temp);

                        if ( temp.contains(waitingStringToQuit) )
                        {
                            log.info(" recieve "+temp+" quit");
                            break;
                        }
                        
                        Tool.SleepAWhile(1, 0);
                    }
                }
	    
                outstream.close();
	        instream.close();

                channel.disconnect();
                session.disconnect();

                log.info("DONE!!! took "+(new Date().getTime() - startTime.getTime())+" ms");
            }
            catch(Exception e)
            {
                log.error(" executeSSHCommandChannelShell() failed! e="+e+",retry="+retry);
                
                if ( retry > retryNumber )
                    throw e;
                
                continue;
            }
                        
            break;
        } 
    }
    
    public static String convertForRegularExpression(String regularExpression)
    {
        return regularExpression.replace("\\", "\\\\").replace("*", "\\*")
                .replace("+", "\\+").replace("|", "\\|")
                .replace("{", "\\{").replace("}", "\\}")
                .replace("(", "\\(").replace(")", "\\)")
                .replace("^", "\\^").replace("$", "\\$")
                .replace("[", "\\[").replace("]", "\\]")
                .replace("?", "\\?").replace(",", "\\,")
                .replace(".", "\\.").replace("&", "\\&");
    }
    
    /** 
     * 返回格式化JSON字符串。 
     *  
     * @param json 未格式化的JSON字符串。 
     * @return 格式化的JSON字符串。 
     */  
    public static String formatJson(String json)  
    {  
        StringBuilder result = new StringBuilder();  
          
        int length = json.length();  
        int number = 0;  
        char key = 0;  
        boolean inQuatation = false;
          
        //遍历输入字符串。  
        for (int i = 0; i < length; i++)  
        {  
            //1、获取当前字符。  
            key = json.charAt(i);
            
            if ( key == '\"' )
            {
                if ( inQuatation )
                    inQuatation = false;
                else
                    inQuatation = true;
            }
            
            if ( inQuatation )
            {               
                result.append(key);  
                continue;
            }
 
            //2、如果当前字符是前方括号、前花括号做如下处理：  
            if((key == '[') || (key == '{') )  
            {  
                //（1）如果前面还有字符，并且字符为“：”，打印：换行和缩进字符字符串。  
                if((i - 1 > 0) && (json.charAt(i - 1) == ':'))  
                {  
                    result.append('\n');  
                    result.append(indent(number));  
                }  
                  
                //（2）打印：当前字符。  
                result.append(key);  
                  
                //（3）前方括号、前花括号，的后面必须换行。打印：换行。  
                result.append('\n');  
                  
                //（4）每出现一次前方括号、前花括号；缩进次数增加一次。打印：新行缩进。  
                number++;  
                result.append(indent(number));  
                  
                //（5）进行下一次循环。  
                continue;  
            }  
              
            //3、如果当前字符是后方括号、后花括号做如下处理：  
            if((key == ']') || (key == '}') )  
            {  
                //（1）后方括号、后花括号，的前面必须换行。打印：换行。  
                result.append('\n');  
                  
                //（2）每出现一次后方括号、后花括号；缩进次数减少一次。打印：缩进。  
                number--;  
                result.append(indent(number));  
                  
                //（3）打印：当前字符。  
                result.append(key);  
                  
                //（4）如果当前字符后面还有字符，并且字符不为“，”，打印：换行。  
                //if(((i + 1) < length) && (json.charAt(i + 1) != ','))  
                //{  
                //    result.append('\n');  
                //}  
                  
                //（5）继续下一次循环。  
                continue;  
            }  
              
            //4、如果当前字符是逗号。逗号后面换行，并缩进，不改变缩进次数。  
            if((key == ','))  
            {  
                result.append(key);  
                result.append('\n');  
                result.append(indent(number));  
                continue;  
            }  
              
            //5、打印：当前字符。  
            result.append(key);  
        }  
          
        return result.toString();  
    }  
    
      /** 
     * 返回指定次数的缩进字符串。每一次缩进三个空格，即SPACE。 
     *  
     * @param number 缩进次数。 
     * @return 指定缩进次数的字符串。 
     */  
    private static String indent(int number)  
    {  
        StringBuilder result = new StringBuilder();  
        for(int i = 0; i < number; i++)  
        {  
            result.append(SPACE);  
        }  
        return result.toString();  
    }  
    
    public static String getESDateTimeCondition(String fieldName,String operatorName,Date startDatetime,Date endDatetime)
    {
        String newEndDatetimeStr;
        String datetimeCondition = "";
        String fromDatetimeStr;
        
        String startDatetimeStr = Tool.convertDateToDateStringForES(startDatetime);    
        String endDatetimeStr = Tool.convertDateToDateStringForES(endDatetime);
                
        switch(operatorName)
        {
            case "eq":
                newEndDatetimeStr = Tool.getEndDatetime(startDatetimeStr);
                datetimeCondition=String.format("%s:[%s TO %s]", fieldName, startDatetimeStr,newEndDatetimeStr);
                break;
            case "ibt":
                newEndDatetimeStr = Tool.getEndDatetime(endDatetimeStr);
                datetimeCondition=String.format("%s:[%s TO %s]", fieldName, startDatetimeStr, newEndDatetimeStr);
                break;
            case "ebt":
                fromDatetimeStr = Tool.changeMilliSecondTo999((String)startDatetimeStr);
                datetimeCondition=String.format("%s:{%s TO %s}", fieldName, fromDatetimeStr, endDatetimeStr);
                break;                        
            case "gt":
                fromDatetimeStr = Tool.changeMilliSecondTo999((String)startDatetimeStr);
                datetimeCondition=String.format("%s:{%s TO *]", fieldName, fromDatetimeStr);
                break;      
            case "gte":
                datetimeCondition=String.format("%s:[%s TO *]", fieldName, startDatetimeStr);
                break;                           
            case "lt":
                fromDatetimeStr = Tool.changeMilliSecondTo0((String)startDatetimeStr);
                datetimeCondition=String.format("%s:[* TO %s}", fieldName, fromDatetimeStr);
                break;                           
            case "lte":
                datetimeCondition=String.format("%s:[* TO %s]", fieldName, startDatetimeStr);
        }
        
        return datetimeCondition;
    } 
      
    public static String getTableNameFromSQLStr(String sqlStr)
    {
        //  updateSql = String.format("UPDATE %s SET ",tableName);"INSERT INTO %s ( %s ) VALUES (",tableName,columnNameStr);
        String tableName = "";
        sqlStr = sqlStr.toUpperCase();
        
        if ( sqlStr.startsWith("INSERT INTO") )
            tableName = sqlStr.substring(sqlStr.indexOf("INSERT INTO")+"INSERT INTO".length(), sqlStr.indexOf(" ("));
        else
        if ( sqlStr.startsWith("UPDATE") ) 
            tableName = sqlStr.substring(sqlStr.indexOf("UPDATE")+"UPDATE".length(), sqlStr.indexOf(" SET")).trim();
        
        return tableName.trim();
    }
    
    public static String convertMetadataName(String metadataName)
    {
        if ( metadataName == null || metadataName.trim().isEmpty() )
            return "";
        
        if ( metadataName.equals("count(*)") )
            return "count_all";
        else
        if ( metadataName.contains("(") )
        {
            metadataName = metadataName.replace("(", "_");
            metadataName = metadataName.replace(")", "");
        }
        else
        if ( metadataName.contains("-") )
            metadataName = metadataName.substring(0, metadataName.indexOf("-"));
        
        return metadataName;
    }
    
    public static boolean ifListContainsStr(List<String> list,String str)
    {
       for(String s : list)
       {
           if ( s.equals(str) )
               return true;
       }
       
       return false;
    }
    
    public static String convertColumnName(String columnName)
    {
        if ( columnName == null )
            return "";
        
        String str = columnName.replace("(*)", "_all");
        str = str.replace("(", "_").replace(")", "");
        return str;
    }
    
    public static String getOralceDatetimeFormat(String timeStr,String format)
    {
        if ( format.contains("hh24") )
            return String.format("to_date('%s','%s')",timeStr.substring(0, format.length()-2),format);
        else
            return String.format("to_date('%s','%s')",timeStr.substring(0, format.length()),format);
    }
    
    public static double getDoubleValueFromString(String str)
    {
        try
        {
            return Double.parseDouble(str);
        }
        catch(Exception e)
        {
            return 0.0;
        }
    }
    
    public static int getColumnDisplayLen(int lenPx)
    {    
        if ( lenPx < CommonKeys.MIN_DISPLAY_COLUMN_LEN )
            lenPx = CommonKeys.MIN_DISPLAY_COLUMN_LEN;

        if ( lenPx > CommonKeys.MAX_DISPLAY_COLUMN_LEN )
            lenPx = CommonKeys.MAX_DISPLAY_COLUMN_LEN;

        return lenPx*12;
    }

    public static String normalizeUnicode(String str) 
    {
        Normalizer.Form form = Normalizer.Form.NFD;
     
        if (!Normalizer.isNormalized(str, form))
            return Normalizer.normalize(str, form);
        else        
            return str;
    }
        public static String normalizeUnicode1(String str) 
    {
        Normalizer.Form form = Normalizer.Form.NFC;
     
        if (!Normalizer.isNormalized(str, form))
            return Normalizer.normalize(str, form);
        else        
            return str;
    }
            public static String normalizeUnicode2(String str) 
    {
        Normalizer.Form form = Normalizer.Form.NFKD;
     
        if (!Normalizer.isNormalized(str, form))
            return Normalizer.normalize(str, form);
        else        
            return str;
    }    
            public static String normalizeUnicode3(String str) 
    {
        Normalizer.Form form = Normalizer.Form.NFKC;
     
        if (!Normalizer.isNormalized(str, form))
            return Normalizer.normalize(str, form);
        else        
            return str;
    }
        
    public static String changeTableColumnName(String tableName,String metadataName,boolean ifDataobjectTypeWithChangedColumnName) // CORE_AAA, AAA_BBB AAA_1_BBB
    {
        String tableColumnName = metadataName;
        
        try
        {
            if ( ifDataobjectTypeWithChangedColumnName == false ) // no need to change
                return metadataName;
            
            if ( tableName == null || tableName.trim().isEmpty() )
                return metadataName;
            
            tableName = tableName.trim();
            if ( metadataName.toLowerCase().startsWith(tableName.toLowerCase()) ) // CORE_AAA
                tableColumnName = metadataName.substring(tableName.length()+1);      
        }
        catch(Exception e)
        {
            //log.error(" changeTableColumnName() failed! tableName="+tableName+" metadataName="+metadataName);
        }
       
        return tableColumnName;
    }
    
    public static String removeKeyValueSpace(String keyValue)
    {
        String ret = keyValue.trim();
        
        try
        {
            if ( !ret.contains(" ") )
                return ret;
            
            String[] vals = ret.split("\\.");
            
            String str = "";
            
            for(String val : vals)
                str += val.trim()+".";
            
            if ( vals.length > 0 )
                str = str.substring(0, str.length()-1);
                       
            if ( ret.endsWith(".") )
                str += ".";
            
            ret = str;
        }
        catch(Exception e)
        {
            log.error("removeKeySpace() failed! e="+e+" keyValue="+keyValue);
        }
                
        return ret;
    }
    
    public static String getDefaultNullVal(String format) // 12,2
    {
        try
        {
            String val = "0.";
            String[] strs = format.split("\\,");
   
            if ( strs.length == 1 )
                return "0";
      
            for(int i=0;i<Integer.parseInt(strs[1]);i++)
                val += "0";

            if ( val.equals("0.") )
                val = "0";
            
            return val; 
        }
        catch(Exception e)
        {
            log.debug(" getDefaultNullValue failed! e="+e+" format="+format);
        }

        return "0";
    }
    
    public static String extractStr(String str,String startStr,String endStr)
    {
        String newStr = "";
        
        try
        {
            if ( endStr == null || endStr.trim().isEmpty() )
                newStr = str.substring(str.indexOf(startStr)+startStr.length());
            else
                newStr = str.substring(str.indexOf(startStr)+startStr.length(), str.indexOf(endStr));
        }
        catch(Exception e)
        {
            log.error("extractStr() faild! e="+e+" str="+str+" startStr="+startStr+" endStr="+endStr);
        }
        
        newStr = newStr.replace(',', ';');
        
        return newStr.trim();
    }
    
    public static double convertDouble(double d, int scale,int roundingMode)
    {
        BigDecimal bg = new BigDecimal(d);
        return  bg.setScale(scale, roundingMode).doubleValue();
    }
        
    public static float convertFloat(float d, int scale,int roundingMode)
    {
        BigDecimal bg = new BigDecimal(d);
        return  bg.setScale(scale, roundingMode).floatValue();
    }
        
    public static String newNormalizeNumber(String str)
    {
        if ( str == null )
            return null;
        
        String newStr = str.trim();
        
        if ( newStr.isEmpty() || !Tool.isNumber(newStr) )
            return str;
         
        String val = str;
        
        if ( str.contains(".") )
        {
            if ( str.contains("E") )
            {
                BigDecimal bd = new BigDecimal(str);  
                val = bd.toPlainString();
            }
            else
            {
                int i= str.indexOf(".");
                String j = str.substring(i+1);
                long k = Long.parseLong(j);
                if ( k == 0 )
                {
                    val = String.valueOf(Long.parseLong(str.substring(0,i)));
                }
            }
        }
        
        return val;
    }
    
    public static String normalizeNumber(String str)
    {
        String val = str;
        
        if ( val == null || val.isEmpty() )
            return "";
        
        try
        {     
            if ( str.contains(".") )
            {
                if ( str.contains("E") )
                {
                    BigDecimal bd = new BigDecimal(str);  
                    val = bd.toPlainString();
                }
                else
                {
                    int i= str.indexOf(".");
                    String j = str.substring(i+1);
                    long k = Long.parseLong(j);
                    if ( k == 0 )
                    {
                        val = String.valueOf(Long.parseLong(str.substring(0,i)));
                    }
                }
            }
        }
        catch(Exception e)
        {
            //log.error(" normalizeNumber() failed! e="+e+" str="+str);
        }
               
        return val;
    }
    
    public static boolean hasMeaning(String str) // has chinese char,number, letter
    {
        if (str == null || str.trim().isEmpty())
            return false;
        
        for(int i=0;i<str.length();i++)
        {
            char c = str.charAt(i);
            
            if ( isChineseChar(c) || c > '0' && c <= '9' || c>='A' && c <= 'Z' || c>='a' && c<='z' )
                return true;
        }
        
        return false;
    }
    
    public static boolean hasMeaningNumbers(String str) // has char between 1-9
    {
        if (str == null || str.trim().isEmpty())
            return false;
        
        for(int i=0;i<str.length();i++)
        {
            char c = str.charAt(i);
            
            if ( c > '0' && c <= '9' )
                return true;
        }
        
        return false;
    }
    
    public static boolean onlyContainsChinese(String str)
    {
        if (str == null || str.isEmpty())
            return false;
        
        for(int i=0;i<str.length();i++)
        {
            char c = str.charAt(i);
            
            if ( !isChineseChar(c) )
                return false;
        }
        
        return true;
    }
    
    public static String[] shuffleArray(String[] array)
    {
        List list = Arrays.asList(array);
        Collections.shuffle(list);
        
        return (String[])list.toArray(new String[1]);
    }
    
    public static Object getServiceData(String serviceIP,int jmxPort,String jmxDomain,String jmxType,String methodName,Object... args) throws Exception
    {
        Object ret = null;
        
        try
        {                   
            String url = String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", serviceIP,jmxPort);
            JMXServiceURL jmxServiceUrl = new JMXServiceURL(url);  
            JMXConnector connector = JMXConnectorFactory.connect(jmxServiceUrl);  
            MBeanServerConnection connection = connector.getMBeanServerConnection();  

            // remote operation  
            ObjectName name = new ObjectName(String.format("%s:type=%s",jmxDomain,jmxType));  
            EdfMBean mbean = JMX.newMBeanProxy(connection, name, EdfMBean.class);  

            Method[] allMethods = EdfMBean.class.getDeclaredMethods();
 
            for (Method m : allMethods) 
            {
		String mname = m.getName().toLowerCase();
                if ( mname.equals(methodName.toLowerCase()) )
                {
                    Class[] paraList = m.getParameterTypes();
                    
                    boolean sameMethod = true;
                    for(int i=0;i<paraList.length;i++)
                    {
                        String typeName = paraList[i].getName().toLowerCase();
                        String argTypeName = args[i].getClass().getName().toLowerCase();
                        
                        if (  !argTypeName.contains(typeName) )
                        {
                            sameMethod = false;
                            break;
                        }
                    }
                    
                    if ( sameMethod == false )
                        continue;
                    
                    ret = m.invoke(mbean,args);
                    break;
                }         
            }
            // close connection  
            connector.close();
        }
        catch(Exception e)
        {
            log.error("getServiceData() failed! e="+e);
            throw e;
        }
       
        return ret;
    }
    
    public static Object getServiceData(String serviceIP,int jmxPort,String jmxDomain,String jmxType,String fieldName) throws Exception
    {
        String url = null;
        Object ret = null;
        
        try
        {                   
            url = String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", serviceIP,jmxPort);
            JMXServiceURL jmxServiceUrl = new JMXServiceURL(url);  
            JMXConnector connector = JMXConnectorFactory.connect(jmxServiceUrl);  
            MBeanServerConnection connection = connector.getMBeanServerConnection();  

            // remote operation  
            ObjectName name = new ObjectName(String.format("%s:type=%s",jmxDomain,jmxType));  
            EdfMBean mbean = JMX.newMBeanProxy(connection, name, EdfMBean.class);  

            Method[] allMethods = EdfMBean.class.getDeclaredMethods();
            
            for (Method m : allMethods) 
            {
		String mname = m.getName().toLowerCase();
                if ( mname.contains("get"+fieldName.toLowerCase()) )
                {
                    ret = m.invoke(mbean);
                    break;
                }         
            }
            // close connection  
            connector.close();
        }
        catch(Exception e)
        {
            log.error("getServiceData() failed! e="+e+" url="+url+" jmxDomain="+jmxDomain+" jmxType="+jmxType+" fieldName="+fieldName);
            throw e;
        }
       
        return ret;
    }
    
    public static void SleepAWhile(int minSeconds,int maxSeconds)
    {
        try 
        {
            Thread.sleep(minSeconds*1000); // sleep 0-30 second 

            if ( maxSeconds <= minSeconds )
                return;
            
            Thread.sleep(Tool.getRandomMilliSeconds((maxSeconds-minSeconds)*1000));
        }
        catch(Exception e) {
            log.debug("SleepAWhile() exception!  e="+e);
        }
    }
    
    public static void SleepAWhileInMS(int mSeconds)
    {
        try 
        {
            Thread.sleep(mSeconds); 
        }
        catch(Exception e) {
            log.debug("SleepAWhile() exception!  e="+e);
        }
    }
    
    public static String getStringWithMaxLen(String str,int maxLen)
    {
        return str.substring(0,maxLen<str.length()?maxLen:str.length());
    }
    
    public static String removeQuotation(String str)
    {
        String str1 = str.replace("'", " ");
        return str1.replace("\"", " ");
    }
    
    public static String replaceComma(String str)
    {
        return str.replace(",", " ");
    }
       
    public static String removeSpecificCharForNameAndDescription(String line)
    {
        String needToRemoveStr = ".,:;-";
        
        if ( line == null || line.isEmpty() )
            return "";
        
        StringBuilder builder = new StringBuilder();
         
        for(int i=0;i<line.length();i++)
        {
            char c = line.charAt(i);
            
            if ( !needToRemoveStr.contains(String.format("%c", c)) )
                builder.append(c);    
            else
                builder.append(" ");
        }
        
        return builder.toString();
    }
    
    public static String removeSpecialChar(String line)
    {       
        if ( line == null || line.isEmpty() )
            return "";
        
        StringBuilder builder = new StringBuilder();
         
        for(int i=0;i<line.length();i++)
        {
            char c = line.charAt(i);
            
            if ( isChineseChar(c) || c >= '0' && c <= '9' || c>='A' && c <= 'Z' || c>='a' && c<='z' || c=='_' || c=='-' || c=='#' || c=='*' ||
                c=='~' || c == '@' || c == '$' || c == '%' || c == '^' || c == '&' || c == '(' || c == ')' || c == '+' || c == '=' ||
                c == '[' || c == ']'|| c == '{'|| c == '}' || c == '/' || c == ';' || c == ',' || c == '.' || c == '>' || c == '<' || c == '?' || 
                c == ':' || c == '\'' || c == '\"' || c == ' ')
                builder.append(c);                 
        }
        
        return builder.toString();
    }    
  
    public static boolean onlyContainsBlank(String str)
    {
        if (str == null || str.isEmpty())
            return false;
        
        for(int i=0;i<str.length();i++)
        {
            char c = str.charAt(i);
            
            if ( c != ' ' )
                return false;
        }
        
        return true;
    }
    
    public static String onlyKeepLetterOrNumber(String line)
    {
        if ( line == null || line.isEmpty() )
            return "";
        
        StringBuilder builder = new StringBuilder();
         
        for(int i=0;i<line.length();i++)
        {
            char c = line.charAt(i);
            
            if ( isLetterOrNumber1(c) )
                builder.append(c);                 
        }
        
        return builder.toString();
    }
    
    public static boolean isLetterOrNumber1(char ch)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(ch);
       
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("[a-zA-Z0-9]+"); 
        java.util.regex.Matcher m = pattern.matcher(builder.toString());
        return m.matches();
    }
        
    public static boolean isLetterOrNumber(char ch)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(ch);
       
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("[a-zA-Z0-9_]+"); 
        java.util.regex.Matcher m = pattern.matcher(builder.toString());
        return m.matches();
    }
    
    public static boolean onlyContainsLetter(String str)
    {
        if ( str.startsWith("_") || str.startsWith("0") || str.startsWith("1") || str.startsWith("2") || str.startsWith("3") || str.startsWith("4") || 
                str.startsWith("5") || str.startsWith("6") || str.startsWith("7") || str.startsWith("8") || str.startsWith("9") )
            return false;
        
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("[a-zA-Z0-9_]+"); 
        java.util.regex.Matcher m = pattern.matcher(str);
        return m.matches();
    }
     
    // GENERAL_PUNCTUATION 判断中文的“号  
    // CJK_SYMBOLS_AND_PUNCTUATION 判断中文的。号  
    // HALFWIDTH_AND_FULLWIDTH_FORMS 判断中文的，号  
  /*  public static boolean isChineseChar(char c) 
    {  
        Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);  
        return ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS  
                || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
                || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
                || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION
                || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
                || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS
                || ub == Character.UnicodeBlock.SPECIALS;  
    }  */
  
    public static boolean isChineseChar(char c) {  
        Character.UnicodeScript sc = Character.UnicodeScript.of(c);  
        if (sc == Character.UnicodeScript.HAN) 
            return true;  
        else
            return false;  
    }  
    
    public static boolean hasChineseChar(String strName) {  
        char[] ch = strName.toCharArray();  
        for (int i = 0; i < ch.length; i++) 
        {
            char c = ch[i];  
            if (isChineseChar(c))
                return true;
        }  
        return false;  
    }  
    
    public static boolean isNumber(String str)
    {
        try  
        {  
            double d = Double.parseDouble(str);  
        }  
        catch(NumberFormatException nfe)  
        {  
            return false;  
        } 
        
        return true; 
    }
    
    public static String removeItemsFromString(String str,String removeStr)
    {
        String newStr = str;
        
        if ( removeStr == null || removeStr.trim().isEmpty() )
            return newStr;
        
        String[] removeArray = removeStr.split("\\ ");
        
        for(String rStr : removeArray)
        {
            newStr = newStr.replace(rStr, "");
        }
        
        return newStr;
    }
    
    public static String removeLastString(String str,String c)
    {
        if ( str == null || str.isEmpty() )
            return "";
        
        if ( !str.endsWith(c) )
            return str;
         
        return str.substring(0,str.length()-c.length());
    }
     
    public static String removeLastChar(StringBuilder builder)
    {
        String str = builder.toString();
        if ( str.isEmpty() )
            return "";
        else
            return str.substring(0,str.length()-1);
    }
    
    public static String removeLastChars(String str,int k)
    {
        if ( str == null )
            return null;
        
        if ( str.length() <= k )
            return "";
        else
            return str.substring(0,str.length()-k);
    }
    
    public static String getTimeDiffStr(Date date1, Date date2)
    {
        long l = date2.getTime()-date1.getTime();
        
        if ( l < 0)
            return "";
        
        long day=l/(24*60*60*1000);
        long hour=(l/(60*60*1000)-day*24);
        long min=((l/(60*1000))-day*24*60-hour*60);
        long s=(l/1000-day*24*60*60-hour*60*60-min*60);
        
        return String.format("%s%s%s%s",
                day==0?"":day+"天",
                hour==0?"":hour+"小时",
                min==0?"":min+"分",
                s==0?"0":s+"秒");     
    }
    
    public static List<String> getSplitedWords(String searchKeyword)
    {
        String[] wordsArray;
        List<String> words = new ArrayList<>();
                
        if ( !searchKeyword.contains("\"") )
        {
            wordsArray = searchKeyword.split("\\ ");
            words = Arrays.asList(wordsArray);
            return words;
        }
        
        searchKeyword = searchKeyword.trim();
        
        char[] charArray = searchKeyword.toCharArray();
        boolean inQuotation = false;
        String word = "";
        
        for(int i=0;i<charArray.length;i++)
        {
            if ( inQuotation ) // 在引号里
            {
                if ( charArray[i] == '"' )
                {
                    if ( !word.isEmpty() )
                    {
                        words.add(word);                   
                        word = "";
                    }
                    
                    inQuotation = false;
                }
                else    
                {
                    if ( charArray[i-1]==' ' && charArray[i]==' ' )
                        continue;
                    
                    word += charArray[i];
                }
            }
            else
            {
                if ( charArray[i] == '"' )
                {
                    inQuotation = true;
                    word = "";
                }
                else
                if (charArray[i] == ' ' )
                {
                    if ( !word.isEmpty() )
                    {
                        words.add(word);                   
                        word = "";
                    }
                }
                else
                {
                    word += charArray[i];
                }
            }
        }
        
        if ( !word.isEmpty() )
            words.add(word);
        
        return words;        
    }
        
/*    public static List<String> tokenizeString(String string) 
    {
        Analyzer analyzer = new IKAnalyzer();
        
        List<String> result = new ArrayList<>();
        try 
        {
            TokenStream stream  = analyzer.tokenStream(null, new StringReader(string));
            stream.reset();
            while (stream.incrementToken()) {
                result.add(stream.getAttribute(CharTermAttribute.class).toString());
            }
        } catch (IOException e) {
          // not thrown b/c we're using a string reader...
          throw new RuntimeException(e);
        }
        return result;
    }*/
     
    public static Date getChangedDate(int field, int change,Date initDate)
    {
        Calendar cal = Calendar.getInstance();
        
        if ( initDate != null )
            cal.setTime(initDate);
        
        cal.set(field,cal.get(field)+change);
        
        return cal.getTime();
    }
    
    public static Date changeToGmt( Date date )
    {
        TimeZone tz = TimeZone.getDefault();
        Date ret = new Date( date.getTime() - tz.getRawOffset() );

        //Date ret = new Date( date.getTime() - 28800000 ); // 8 hours difference
        //return ret;
        
        // if we are now in DST, back off by the delta.  Note that we are checking the GMT date, this is the KEY.
        if ( tz.inDaylightTime( ret )){
            Date dstDate = new Date( ret.getTime() - tz.getDSTSavings() );

            // check to make sure we have not crossed back into standard time
            // this happens when we are on the cusp of DST (7pm the day before the change for PDT)
            if ( tz.inDaylightTime( dstDate )){
                ret = dstDate;
            }
         }
         return ret; 
    }
    
    public static Date changeFromGmt( Date date )
    {
        if ( date == null )
            return null;
        
        TimeZone tz = TimeZone.getDefault();
        Date ret = new Date( date.getTime() + tz.getRawOffset() );

        //Date ret = new Date( date.getTime() - 28800000 ); // 8 hours difference
        //return ret;
        
        // if we are now in DST, back off by the delta.  Note that we are checking the GMT date, this is the KEY.
        if ( tz.inDaylightTime( ret )){
            Date dstDate = new Date( ret.getTime() - tz.getDSTSavings() );

            // check to make sure we have not crossed back into standard time
            // this happens when we are on the cusp of DST (7pm the day before the change for PDT)
            if ( tz.inDaylightTime( dstDate )){
                ret = dstDate;
            }
         }
         return ret; 
    }
    
    public static String removeInvisiableASIIC(String str)
    {
       // String tmp =  str.replaceAll("[\\u0001-\\u001F]", " ");
       // return tmp.replaceAll("[\\][\\u0001-\\u001F]", " ");
        StringBuilder builder = new StringBuilder();
        
        char[] strArray = str.toCharArray();
        
        for(char c : strArray)
        {
        //    log.info(" c=["+c+"]+("+(int)c+")");
                    
            if ( (int)c <= 31 )
                builder.append(' ');
            else
                builder.append(c);
        }
        
        return builder.toString();
    }
    
    public static String convertStringArrayToSingleString(List<String> strList)
    {
        StringBuilder builder = new StringBuilder();
        
        if ( strList == null || strList.isEmpty() )
            return "";
        
        for(String str : strList)
            builder.append(str).append(";");
        
        String ret = builder.toString();
        return ret.substring(0,ret.length()-1);
    }
    
    public static String convertStringArrayToSingleString(List<String> strList,String separator)
    {
        StringBuilder builder = new StringBuilder();
        
        if ( strList == null || strList.isEmpty() )
            return "";
        
        for(String str : strList)
            builder.append(str).append(separator);
        
        String ret = builder.toString();
        return ret.substring(0,ret.length()-1);
    }
    
    public static String getDataExportFileName(String filename)
    {
        int k = filename.lastIndexOf(".");       
        k = filename.lastIndexOf(".", k-1);

        return filename.substring(k+1);
    }
    
    public static String convertIfItIsTimeFolder(String path, Date businessDate) ///$(yyyy-MM:this_month)
    {
        if ( path == null || path.isEmpty() )
            return "";
        
        if ( businessDate == null )
            businessDate = new Date();
        
        String str = path.trim();
        
        if ( str.contains("$(") && str.endsWith(")"))
        {
            String timeFolderTypeStr = str.substring(str.indexOf(":")+1,str.indexOf(")"));
            String timeStr;
            Date date;
            
            TimeFolderType type = TimeFolderType.findByName(timeFolderTypeStr);
            
            if ( type == TimeFolderType.YESTERDAY )
                date = Tool.dateAddDiff(businessDate, -24, Calendar.HOUR);
            else
                date = businessDate;
                
            timeStr = convertDateToString(date,type.getFormat());
            
            str = String.format("%s%s",path.substring(0,path.indexOf("$")),timeStr);
        }
        
        return str;
    }
     
    public static String removeFirstAndLastChar(String source)
    {
        return source.substring(1,source.length()-1);
    }
    
    public static String converStringListToSingleString(String[] inputStr)
    {
        StringBuilder builder = new StringBuilder();
        
        if ( inputStr == null || inputStr.length == 0 )
            return "";
        
        for(String str : inputStr)
            builder.append(str).append(";");
        
        String newStr = builder.toString();
        
        return newStr.substring(0, newStr.length()-1);
    }
    
    public static String converStringListToSingleString1(String[] inputStr)
    {
        StringBuilder builder = new StringBuilder();
        
        if ( inputStr == null || inputStr.length == 0 )
            return "";
        
        for(String str : inputStr)
            builder.append(str).append(",");
        
        String newStr = builder.toString();
        
        return newStr.substring(0, newStr.length()-1);
    }
    
    public static String converStringListToSingleString(List<String> inputStr)
    {
        StringBuilder builder = new StringBuilder();
        
        if ( inputStr==null || inputStr.isEmpty() )
            return "";
        
        for(String str : inputStr)
            builder.append(str).append(";");
        
        String newStr = builder.toString();
        
        return newStr.substring(0, newStr.length()-1);
    }
    
    public static String convertToWindowsPathFormat(String path)
    {
        return path.replace("/", "\\");
    }
    
    public static String convertToNonWindowsPathFormat(String path)
    {
        return path.replace("\\", "/");
    }
    
    public static String formalizeIntLongString(String value)
    {
        if ( value.equals("t") )
            value = "1";
        
        if ( value.equals("f") )
            value = "0";
        
        String val = value.trim();
        
        int k = val.lastIndexOf("."); 
        if ( k==-1) 
            return val;
        else
            return val.substring(0, k);
    }
    
    public static String removeAroundQuotation(String str)
    {
        String newStr = str;
        
        if ( str == null )
            return "";
        
        if ( str.startsWith("\"") )
            newStr = str.substring(1);
        
        if ( str.endsWith("\"") && str.length()>=1 )
            newStr = newStr.substring(0, newStr.length()-1);
        
        return newStr;
    }
    
    public static String removeAroundAllQuotation(String str)
    {
        String newStr = str;
        
        if ( str == null )
            return "";
        
        if ( str.startsWith("'") || str.startsWith("\"") )
            newStr = str.substring(1);
        
        if ( (str.endsWith("'") || str.endsWith("\"")) && str.length()>=1 )
            newStr = newStr.substring(0, newStr.length()-1);
        
        return newStr;
    }
    
    public static int getRandomMilliSeconds(int maxMilliSeconds)
    {
        Random random = new Random();
        int ret = Math.abs(random.nextInt()%maxMilliSeconds);
        return ret;
    }
    
    public static String getDurationString(int seconds) 
    {
        int hours = seconds / 3600;
        int minutes = (seconds % 3600) / 60;
        seconds = seconds % 60;

        return twoDigitString(hours) + ":" + twoDigitString(minutes) + ":" + twoDigitString(seconds);
    }

    public static String twoDigitString(int number) 
    {
        if (number == 0) 
            return "00";

        if (number / 10 == 0) 
            return "0" + number;

        return String.valueOf(number);
    }
    
    public static String getStringFromByteBuffer(ByteBuffer byteBuffer,String encoding) throws UnsupportedEncodingException
    {
        if ( byteBuffer.capacity() < CommonKeys.INTRANET_BIG_FILE_SIZE )
            return new String(byteBuffer.array(),encoding);
          
        StringBuilder str = new StringBuilder();
        
        int bufSize = 8*1024;
        byte[] buf = new byte[bufSize];
                
        while(byteBuffer.hasRemaining())
        {
            int len = Math.min(bufSize, byteBuffer.remaining());
            byteBuffer.get(buf,0,len);
            
            str.append(new String(buf,encoding));
        }
        
        return str.toString();
    }
    
    public static String converterToPinyinAcronym(String chineseStr)
    {
        String pinyinName = "";
        char[] nameChar = chineseStr.toCharArray();
    
        HanyuPinyinOutputFormat defaultFormat = new HanyuPinyinOutputFormat();
        defaultFormat.setCaseType(HanyuPinyinCaseType.LOWERCASE);
        defaultFormat.setToneType(HanyuPinyinToneType.WITHOUT_TONE);
        
        for (int i = 0; i < nameChar.length; i++)
        {
            if ( Tool.isChineseChar(nameChar[i]) )
            {
                try {
                    pinyinName += PinyinHelper.toHanyuPinyinStringArray(nameChar[i], defaultFormat)[0].charAt(0);
                } catch (Exception e) {
                    //log.info(" converterToPinyinAcronym() failed! e="+e+" ");
                }
            }
           /* else
            {
                if ( !isLetterOrNumber(nameChar[i]) )
                    pinyinName += "_";
                else
                    pinyinName += nameChar[i];
            }*/
        }
        
        if ( pinyinName.endsWith("_") )
            pinyinName = pinyinName.substring(0, pinyinName.length()-1);
        
        return pinyinName;
    }

    public static String converterToPinyin(String chineseStr)
    {
        String pinyinName = "";
        char[] nameChar = chineseStr.toCharArray();
        
        HanyuPinyinOutputFormat defaultFormat = new HanyuPinyinOutputFormat();
        defaultFormat.setCaseType(HanyuPinyinCaseType.LOWERCASE);
        defaultFormat.setToneType(HanyuPinyinToneType.WITHOUT_TONE);
        
        for (int i = 0; i < nameChar.length; i++) 
        {
            if ( Tool.isChineseChar(nameChar[i]) )
            {
                try {
                    pinyinName += PinyinHelper.toHanyuPinyinStringArray(nameChar[i], defaultFormat)[0];
                } catch (Exception e) {
                }
            }else{
                pinyinName += nameChar[i];
            }
        }
        return pinyinName;
    }
    
    public static Map<String,String> parseJson(JSONObject json) throws Exception
    {
        JSONObject object;
        JSONArray array;
        String key;
        String val;
        
        Map<String,String> out = new HashMap<>();
        
        Iterator<String> keys = json.keys();
        
        while(keys.hasNext())
        {
            key = keys.next();

            try 
            {
                object = json.optJSONObject(key);
                if ( object != null )
                    continue;

                array = json.getJSONArray(key);
                if ( array != null )
                {
                    val = json.optString(key);
                    out.put(key,val);
                }
            }
            catch(Exception e)
            {
                val = json.optString(key);
                
                if( val != null )
                {
                    val = val.trim();
                    
                    if ( !val.isEmpty() && isNumber(val)  )
                        val = normalizeNumber(val);
                           
                    out.put(key,val);
                }
            }
        }
        
        return out;
    }

    public static long convertESDateStringToLong(String dateStr)
    {
        SimpleDateFormat formatter;
        
        try 
        {
            formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S");
            return formatter.parse(dateStr).getTime();
        }
        catch(Exception e)
        {
            return 0;
        }
    }
    
    public static Date convertESDateStringToDate(String dateStr)
    {
        SimpleDateFormat formatter;
        
        try 
        {
            formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S");
            formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
            
            Date date = new Date();
            date.setTime(formatter.parse(dateStr).getTime());
            return date;
        }
        catch(Exception e)
        {
            return null;
        }
    }
     
    public static Date convertDateStringToDateAccordingToPattern(String dateStr, String pattern) throws ParsingDatetimeException
    {
        SimpleDateFormat formatter;
        
        try
        {
            if ( dateStr == null || dateStr.trim().isEmpty() )
                return null;
                        
            if ( dateStr.length()>23 )
                dateStr =dateStr.substring(0, 23);
                        
            formatter = new SimpleDateFormat(pattern);
            Date date = new Date();
            date.setTime(formatter.parse(dateStr).getTime());
            return date;
        }
        catch(Exception e)
        {
            log.error("convertDateStringToDateAccordingToPattern() failed! dateStr="+dateStr+" pattern="+pattern+" error="+e.getMessage());
            throw new ParsingDatetimeException(Util.getBundleMessage("parsing_datetime_failed",dateStr,pattern));
        }
    }
     
    public static Date convertTimestampStringToDate(String dateStr)
    {
        SimpleDateFormat formatter;
        
        try 
        {
            formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");           
            Date date = new Date();
            date.setTime(formatter.parse(dateStr).getTime());
            return date;
        }
        catch(Exception e)
        {
            return null;
        }
    }
     
    public static Date changeToLocal(Date date)
    {
        SimpleDateFormat formatter;
        
        try 
        {
            String dateStr = Tool.convertDateToTimestampStringWithMilliSecond(date);
            formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
            formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
            
            Date newDate = new Date();
            newDate.setTime(formatter.parse(dateStr).getTime());
            return newDate;
        }
        catch(Exception e)
        {
            return null;
        }
    }
        
    public static java.util.Date convertStringToDate(String dateStr)
    {
        SimpleDateFormat formatter;
        
        try 
        {
            if ( dateStr.length()==10)
                formatter = new SimpleDateFormat("yyyy-MM-dd");
            else
                formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            
            return formatter.parse(dateStr);   
        }
        catch(Exception e)
        {
            
            return null;
        }
    }
     
    public static java.util.Date convertStringToDate(String dateStr,String format)
    {
        SimpleDateFormat formatter;
        
        try 
        {
            formatter = new SimpleDateFormat(format);           
            return formatter.parse(dateStr);   
        }
        catch(Exception e)
        {
            return null;
        }
    }
     
    public static java.util.Date convertStringToDateForUSLocal(String dateStr,String format)
    {
        SimpleDateFormat formatter;
        
        try 
        {
            formatter = new SimpleDateFormat(format,Locale.US);           
            return formatter.parse(dateStr);   
        }
        catch(Exception e)
        {
            return null;
        }
    }
     
    public static String convertDateToString(java.util.Date date,String format)
    {
        if ( date == null )
            return "";
        
        SimpleDateFormat formatter = new SimpleDateFormat(format);
        return formatter.format(date);
    }
        
    public static String convertDateToDateString(java.util.Date date)
    {
        if ( date == null )
            return "";
                
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        return formatter.format(date);
    }
    
    public static String convertDateToTimestampString(java.util.Date date)
    {
        if ( date == null )
            return "";
                
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        formatter.setTimeZone(TimeZone.getDefault());
        return formatter.format(date);
    }    
    
    public static String convertDateToTimestampStringInChinese(java.util.Date date)
    {
        if ( date == null )
            return "";
                
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss");
        formatter.setTimeZone(TimeZone.getDefault());
        return formatter.format(date);
    }    
    
    public static String convertDateToTimestampStringInChinese1(java.util.Date date)
    {
        if ( date == null )
            return "";
                
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy年MM月dd日HH时mm分ss秒");
        formatter.setTimeZone(TimeZone.getDefault());
        return formatter.format(date);
    }    
    
     public static String convertDateToDateString1(java.util.Date date)
    {
        if ( date == null )
            date = new Date(0);
                
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        return formatter.format(date);
    }
    
    public static String convertDateToTimestampString1(java.util.Date date)
    {
        if ( date == null )
            date = new Date(0);
                
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        formatter.setTimeZone(TimeZone.getDefault());
        return formatter.format(date);
    }    
    
    public static String convertDateToTimestampString2(java.util.Date date)
    {
        if ( date == null )
            date = new Date(0);
                
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss-SSS");
        formatter.setTimeZone(TimeZone.getDefault());
        return formatter.format(date);
    }    
    
    public static String convertDateToTimestampStringWithMilliSecond(java.util.Date date)
    {
        if ( date == null )
            return "";
        
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
        formatter.setTimeZone(TimeZone.getDefault());
        return formatter.format(date);
    }  
    
    public static String convertDateToTimestampStringWithMilliSecond1(java.util.Date date)
    {
        if ( date == null )
            date = new Date(0);
        
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        formatter.setTimeZone(TimeZone.getDefault());
        return formatter.format(date)+"000";
    }
    
    public static String changeMilliSecondTo999(String datetimeStr)
    {
        String mSecond = datetimeStr.substring(20);  // get milli second
        
        if ( mSecond.equals("0"))
            return String.format("%s999",datetimeStr.substring(0, 20));
        else
            return datetimeStr;
    }
        
    public static String changeMilliSecondTo0(String datetimeStr)
    {
        String mSecond = datetimeStr.substring(20);  // get milli second
        
        if ( mSecond.equals("999") || mSecond.equals("0")  )
            return String.format("%s0",datetimeStr.substring(0, 20));
        else
            return datetimeStr;
    }
        
    public static String getEndDatetime(String datetimeStr)
    {
        if ( datetimeStr == null || datetimeStr.isEmpty() )
            return "";
        
        //log.info(" datatimeStr="+datetimeStr);
        
        String endStr=datetimeStr;
        
        String mSecond = datetimeStr.substring(20);  // get milli second
        if ( mSecond.equals("0") )
        {
            endStr = String.format("%s999",datetimeStr.substring(0, 20));
        }
        else
            return endStr;            

        String second = datetimeStr.substring(17);  // get second
        if ( second.equals("00") )
        {
            endStr = String.format("%s59.999",datetimeStr.substring(0, 17));
        }
        else
            return endStr;
        
        String minute = datetimeStr.substring(14,16);  // get minute
        if ( minute.equals("00") ) 
        {
            endStr = String.format("%s59:59.999",datetimeStr.substring(0, 14));
        }
        else
            return endStr;
        
        String hour = datetimeStr.substring(11,13);  // get hour
        if ( hour.equals("00") )
        {
            endStr = String.format("%s23:59:59.999",datetimeStr.substring(0, 11));
        }

        return endStr;
    }
          
    public static String convertDateToDateStringForES(java.util.Date date)
    {
        if(date == null)
            return "";

        Date newDate = changeToGmt(date);
         
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S");
        
        return formatter.format(newDate);
    }
    
    public static String convertDateToDateEndStringForES(java.util.Date date)
    {
        if(date == null)
            return "";

        Date newDate =  changeToGmt(date);
         
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        
        return formatter.format(newDate)+"T23:59:59";
    }    
    
    public static String convertDateToTimestampStringForES(java.util.Date date)
    {
        if(date == null)
            return "";

        Date newDate = changeToGmt(date);
        
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S");
        
        return formatter.format(newDate);
    }    
        
    public static ContentType getContentType(String mimeType)
    {
        if ( mimeType.startsWith("text/html") )
            return ContentType.HTML;
                
        if ( mimeType.startsWith("text") || mimeType.startsWith("application/xml") )
            return ContentType.TEXT;
                
        if ( mimeType.startsWith("image"))
            return ContentType.IMAGE;
        
        if ( mimeType.startsWith("audio"))
            return ContentType.AUDIO;
        
        if ( mimeType.startsWith("video") || mimeType.startsWith("application/x-shockwave-flash"))
            return ContentType.VIDEO;

        if ( mimeType.startsWith("application/pdf"))
            return ContentType.PDF;
        
        return ContentType.OTHERS;
    }
    
    public static String getMediaPlayer(String mimeType)
    {
        if ( mimeType.startsWith("audio/mpeg") || mimeType.startsWith("application/x-shockwave-flash") || mimeType.startsWith("video/x-flv")
                || mimeType.startsWith("video/mp4") )
            return "flash";
        
        if ( mimeType.startsWith("application/pdf") )
            return "pdf";
        
        if ( mimeType.startsWith("video/x-ms-wmv") || mimeType.startsWith("video/x-msvideo") )
            return "windows";                                              
         
        if ( mimeType.startsWith("video/quicktime") )
            return "quicktime";

        if ( mimeType.startsWith(" video/vnd.rn-realvideo") || mimeType.startsWith("audio/vnd.rn-realaudio") )
            return "real";
        
        return "";
    }
           
    public static Document getXmlDocument(String xml)
    { 
        try
        {  
            ByteArrayInputStream in = new ByteArrayInputStream(xml.getBytes("UTF-8"));
            Reader reader = new InputStreamReader(in,"UTF-8");
            SAXReader saxReader = new SAXReader();
            
            return saxReader.read(reader);
        } 
        catch (Exception e) 
        {
            log.error("getXmlDocument() failed! e="+e);
            log.error("xml="+xml);
            return null;
        }
    }
        
    public static String getYearStr(Date date)
    {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        
        return String.format("%04d", cal.get(Calendar.YEAR));
    }
    
    public static int getTimeHours(Date date)
    {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        
        return cal.get(Calendar.HOUR_OF_DAY);
    }
    
    public static String getYearMonthStr(Date date)
    {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        
        return String.format("%04d%02d", cal.get(Calendar.YEAR), cal.get(Calendar.MONTH)+1);
    }
    
    public static String getYearWeekStr(Date date)
    {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        
        return String.format("%04d%02d", cal.get(Calendar.YEAR), cal.get(Calendar.WEEK_OF_YEAR));
    }
    
    public static String getYearMonthDayStr(Date date)
    {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        
        return String.format("%04d%02d%02d", cal.get(Calendar.YEAR), cal.get(Calendar.MONTH)+1,cal.get(Calendar.DAY_OF_MONTH));
    }    
            
    public static String getYearMonthDayHourStr(Date date)
    {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        
        return String.format("%04d%02d%02d%02d", cal.get(Calendar.YEAR), cal.get(Calendar.MONTH)+1,cal.get(Calendar.DAY_OF_MONTH),cal.get(Calendar.HOUR_OF_DAY));
    }    
    
    public static String getYearMonthDayHourMinuteStr(Date date)
    {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        
        return String.format("%04d%02d%02d%02d%02d", cal.get(Calendar.YEAR), cal.get(Calendar.MONTH)+1,cal.get(Calendar.DAY_OF_MONTH),cal.get(Calendar.HOUR_OF_DAY),cal.get(Calendar.MINUTE));
    }   
         
    public static byte[] serializeObject(Object object) throws IOException
    {
        ByteArrayOutputStream saos=new ByteArrayOutputStream();
        ObjectOutputStream oos=new ObjectOutputStream(saos);
        oos.writeObject(object);
        oos.flush();
        saos.close();
        oos.close();
        return saos.toByteArray();
    }
 
    public static Object deserializeObject(byte[]buf) throws IOException, ClassNotFoundException
    {
        ByteArrayInputStream sais=new ByteArrayInputStream(buf);
        ObjectInputStream ois = new ObjectInputStream(sais);
        Object object = ois.readObject();
        sais.close();
        ois.close();
        return object;
    }
    
    public static String getStrFromList(List<String> list) 
    {
        if ( list == null )
            return "";
        
        String str = "";
        
        for (int i=0;i<list.size();i++)
        {
            if ( i==0 )
                str = str.concat(list.get(i));
            else
                str = str.concat(",").concat(list.get(i));
        }
        
        return str;
    }   
    
    public static List<String> getListFromString(String str) 
    {
        List<String> list = new ArrayList<>();
        
        if ( str == null || str.trim().isEmpty() )
            return list;
        
        String[] vals = str.split("\\,");
        
        for(String val : vals)
        {
            list.add(val);    
        }
        
        return list;
    }   
    
    public static String getStrFromListWithQuotation(List<String> list) 
    {
        if ( list == null )
            return "";
        
        String str = "";
        
        for (int i=0;i<list.size();i++)
        {
            if ( i==0 )
                str = "'"+str.concat(list.get(i))+"'";
            else
                str = str.concat(",'").concat(list.get(i)).concat("'");
        }
        
        return str;
    }   
    
    public static Date dateWithNewTime(Date date, Date time)
    {
        Calendar timeCal = Calendar.getInstance();
        timeCal.setTime(time);
        
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        
        cal.set(Calendar.HOUR_OF_DAY, timeCal.get(Calendar.HOUR_OF_DAY));
        cal.set(Calendar.MINUTE, timeCal.get(Calendar.MINUTE));
        cal.set(Calendar.SECOND, timeCal.get(Calendar.SECOND));
        cal.set(Calendar.MILLISECOND, timeCal.get(Calendar.MILLISECOND));
        
        return cal.getTime(); 
    }
        
    public static Date dateAddDiff(Date date,int diff, int type)
    {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(type,diff);//把日期往后增加 second, minute
 
        return cal.getTime(); 
    }
        
    public static Calendar getFirstDayOfMonth(Calendar day) 
    {
        Calendar cal = (Calendar) day.clone();
        cal.set(Calendar.DAY_OF_MONTH, 1);
             
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        
        return cal;
    }
    
    public static Calendar getFirstDayOfYear(Calendar day) 
    {
        Calendar cal = (Calendar) day.clone();
        cal.set(Calendar.DAY_OF_YEAR, 1);
        
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        
        return cal;
    }
    
    public static Calendar getFirstDayOfLastMonth(Calendar day) 
    {
        Calendar cal = (Calendar) day.clone();
        
        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH);
        
        if ( month == 0 )  // Jan
        {
            year--;
            month = 12;
        }
        else
            month--;
        
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.MONTH, month);
        cal.set(Calendar.DAY_OF_MONTH, 1);
                
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        
        return cal;
    }    
    
    public static Calendar getLastDayOfLastMonth(Calendar day) 
    {
        Calendar cal = (Calendar) day.clone();
        
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.add(Calendar.DAY_OF_MONTH, -1);
        
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        
        return cal;
    }    
    
    public static Calendar getFirstDayOfLastYear(Calendar day) 
    {
        Calendar cal = (Calendar) day.clone();
        
        int year = cal.get(Calendar.YEAR);
        
        cal.set(Calendar.YEAR, year-1);
        cal.set(Calendar.DAY_OF_YEAR, 1);
        
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
                
        return cal;
    }    
    
    public static Calendar getLastDayOfLastYear(Calendar day) 
    {
        Calendar cal = (Calendar) day.clone();
        
        cal.set(Calendar.DAY_OF_YEAR, 1);
        cal.add(Calendar.DAY_OF_YEAR, -1);
        
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        
        return cal;
    }
        
    public static Date startOfSecond(Date date)
    {
        Calendar cal = Calendar.getInstance();

        cal.setTime(date);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTime();
    }
     
    public static Date endOfSecond(Date date)
    {
        Calendar cal = Calendar.getInstance();

        cal.setTime(date);
        cal.set(Calendar.MILLISECOND, 999);

        return cal.getTime();
    } 
    
    public static Date startOfMinute(Date date)
    {
        Calendar cal = Calendar.getInstance();

        cal.setTime(date);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTime();
    }
     
    public static Date endOfMinute(Date date)
    {
        Calendar cal = Calendar.getInstance();

        cal.setTime(date);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 999);

        return cal.getTime();
    }    
    
    public static Date startOfHour(Date date)
    {
        Calendar cal = Calendar.getInstance();

        cal.setTime(date);

        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTime();
    }
     
    public static Date endOfHour(Date date)
    {
        Calendar cal = Calendar.getInstance();

        cal.setTime(date);

        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 999);

        return cal.getTime();
    }    
    
    public static Date startOfDay(Date date)
    {
        Calendar cal = Calendar.getInstance();

        cal.setTime(date);

        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTime();
    }
     
    public static Date endOfDay(Date date)
    {
        Calendar cal = Calendar.getInstance();

        cal.setTime(date);

        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 999);

        return cal.getTime();
    }    
        
    public static Date startOfMonth(Date date)
    {
        Calendar cal = Calendar.getInstance();

        cal.setTime(date);
        
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTime();
    }
     
    public static Date endOfMonth(Date date)
    {
        Calendar cal = Calendar.getInstance();

        cal.setTime(date);
        
        int lastDay = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
              
        cal.set(Calendar.DAY_OF_MONTH, lastDay);
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 999);

        return cal.getTime();
    }
    
    public static Date startOfWeek(Date date)
    {
        Calendar cal = Calendar.getInstance();

        cal.setTime(date);
        
        cal.set(Calendar.DAY_OF_WEEK, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTime();
    }
     
    public static Date endOfWeek(Date date)
    {
        Calendar cal = Calendar.getInstance();

        cal.setTime(date);
        
        int lastDay = cal.getActualMaximum(Calendar.DAY_OF_WEEK);
              
        cal.set(Calendar.DAY_OF_MONTH, lastDay);
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 999);

        return cal.getTime();
    }
    
    
    public static Date startOfYear(Date date)
    {
        Calendar cal = Calendar.getInstance();

        cal.setTime(date);
        
        cal.set(Calendar.DAY_OF_YEAR, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTime();
    }    
        
    public static Date endOfYear(Date date)
    {
        Calendar cal = Calendar.getInstance();

        cal.setTime(date);
        
        int lastDay = cal.getActualMaximum(Calendar.DAY_OF_YEAR);
              
        cal.set(Calendar.DAY_OF_YEAR, lastDay);
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 999);

        return cal.getTime();
    }
   
    public static Calendar getFirstDayOfWeek(Calendar day) 
    {
        Calendar monday = (Calendar) day.clone();
           
        monday.set(Calendar.HOUR_OF_DAY, 0);
        monday.set(Calendar.MINUTE, 0);
        monday.set(Calendar.SECOND, 0);
        monday.set(Calendar.MILLISECOND, 0);

        return getADayOfWeek(monday, Calendar.MONDAY);
    }

    public static Calendar getLastDayOfWeek(Calendar day) 
    { 
        Calendar sunday = (Calendar) day.clone();      
        return getADayOfWeek(sunday, Calendar.SUNDAY);  
    }

    private static Calendar getADayOfWeek(Calendar day, int dayOfWeek) 
    {
        int week = day.get(Calendar.DAY_OF_WEEK);     
        if (week == dayOfWeek)          
            return day;

        int diffDay = dayOfWeek - week;      

        if (week == Calendar.SUNDAY) 
        {          
            diffDay -= 7;      
        }
        else if (dayOfWeek == Calendar.SUNDAY) 
        {
              diffDay += 7;      
        }

        day.add(Calendar.DATE, diffDay);      
        return day;  
    }      
    
 
}
