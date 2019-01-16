/*
 * FileUtil.java
 *
 */

package com.broaddata.common.util;

import java.util.Date;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.thrift.TException;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.io.FileReader;
import java.util.List;
import java.util.Map;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileInputStream;
import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import com.pff.PSTFile;
import com.pff.PSTFolder;
import com.pff.PSTMessage;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.UUID;
import java.util.Vector;

import com.broaddata.common.model.enumeration.ColumnSeparatorType;
import com.broaddata.common.model.enumeration.EncodingType;
import com.broaddata.common.model.enumeration.JobTimeType;
import com.broaddata.common.processor.Processor;
import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.thrift.dataservice.DiscoveredDataobjectInfo;
import com.broaddata.common.thrift.dataservice.Job;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Set;
import org.apache.poi.ss.usermodel.DataFormatter;
 
public class FileUtil 
{
    static final Logger log=Logger.getLogger("FileUtil");
    
    public static void createFolders(File folder)
    {
        File parentFile = folder.getParentFile();
        
        if(!parentFile.exists())
            createFolders(parentFile);
     
        folder.mkdir();
    }
    
    public static void chmodFullPermission(String filepath) throws IOException
    {
        Set<PosixFilePermission> perms = new HashSet<PosixFilePermission>();
          //add owners permission
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        perms.add(PosixFilePermission.OWNER_EXECUTE);
          //add group permissions
        perms.add(PosixFilePermission.GROUP_READ);
        perms.add(PosixFilePermission.GROUP_WRITE);
        perms.add(PosixFilePermission.GROUP_EXECUTE);
          //add others permissions
        perms.add(PosixFilePermission.OTHERS_READ);
        perms.add(PosixFilePermission.OTHERS_WRITE);
        perms.add(PosixFilePermission.OTHERS_EXECUTE);

        Files.setPosixFilePermissions(Paths.get(filepath), perms);
    }
    
    public static void sendFileToWebServer(String url,String filename,String contentTypeStr) throws Exception
    {
        //String url = "http://example.com/upload";
        String charset = "UTF-8";
  
        try
        {
            File file = new File(filename);
            String boundary = UUID.randomUUID().toString(); // Just generate some unique random value.
            String CRLF = "\r\n"; // Line separator required by multipart/form-data.

            URLConnection connection = new URL(url).openConnection();
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);

            OutputStream output = connection.getOutputStream();
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(output, charset), true);
           
            // Send text file.
            writer.append("--" + boundary).append(CRLF);
            writer.append("Content-Disposition: form-data; name=\"file\"; filename=\"" + file.getName() + "\"").append(CRLF);
            writer.append("Content-Type: "+contentTypeStr+"; charset=" + charset).append(CRLF); // Text file itself must be saved in this charset!
            writer.append(CRLF).flush();
            Files.copy(file.toPath(), output);
            output.flush(); // Important before continuing with writer!
            writer.append(CRLF).flush(); // CRLF is important! It indicates end of boundary.

            // End of multipart/form-data.
            writer.append("--" + boundary + "--").append(CRLF).flush();
            
            // Request is lazily fired whenever you need to obtain information about response.
            int responseCode = ((HttpURLConnection) connection).getResponseCode();
            
            log.info(" responseCode="+responseCode); // Should be 200    
        }
        catch(Exception e)
        {
            log.error("sendFileToWebServer() failed! e="+e);
            throw e;
        }
    }
    
    public static void findFilesWithPattern(List<String> foundFilenames, String folderName, boolean processSubFolder, String pattern)
    {      
        File path = new File(folderName);
        
        if (path.isDirectory() && path.canRead() && path.canWrite()) 
        {
	    String[] allfiles = path.list(); 
            if (allfiles == null) return;
            
            for(String filename : allfiles)
            {
                String fullpath =folderName+"/"+filename;
               
                File file = new File(fullpath);                
                
                if ( file.isFile() ) // if it's a file 
                {
                    log.info("3333333333333333333333 filename ="+filename+" pattern="+pattern);
                    if ( filename.matches(pattern) )
                        foundFilenames.add(filename);
                }
                else
                {
                    if ( processSubFolder )
                        findFilesWithPattern(foundFilenames,fullpath,processSubFolder,pattern);
                }
            }
	}
    }
     
    public static boolean copyFileToFolder(String sourceFileName,String targetFolder,String newFileName) throws Exception 
    {
        String fullPath = targetFolder+"/"+newFileName;
        
        File file = new File(sourceFileName);
        
        ByteBuffer contentStream = readFileToByteBuffer(file);
        
        writeFileFromByteBuffer(fullPath,contentStream);
        
        return true;
    }
    
    public static boolean copyFileToFolder(String sourceFileName,String targetFileName) throws Exception 
    {
        File file = new File(sourceFileName);
        
        if ( !file.exists() )
            return false;
        
        ByteBuffer contentStream = readFileToByteBuffer(file);
        
        writeFileFromByteBuffer(targetFileName,contentStream);
        
        return true;
    }
    
    public static void writeFileFromByteBuffer(String filename,ByteBuffer contentStream) throws Exception
    {
       try
        {       
            File fr=new File(filename);
            if ( !fr.exists() ) fr.createNewFile();

            InputStream bis = new ByteBufferBackedInputStream(contentStream);
            OutputStream out = new FileOutputStream(fr);
           
            int count = 0;

            byte[] buf = new byte[CommonKeys.FILE_BUFFER_SIZE];

            while( (count=bis.read(buf)) != -1 ) {
                out.write(buf, 0, count);
            }
            out.close();
        }
        catch(Exception e)
        {
            log.error("writeFileFromByteBuffer() failed! e="+e+" filename="+filename+"\n");
            throw e;
        }   
    }
    
    public static String getFileContentEncoding(byte[] fileStream)
    {
        return "UTF-8";
    }
    
    public static String getFileNameWithoutPath(String fileFullpath) 
    {
        try
        {
            int i = fileFullpath.lastIndexOf("/");
            if (i <= 0) 
            {
                int j = fileFullpath.lastIndexOf("\\");
                if ( j<=0 )
                    return fileFullpath;
                else
                    return fileFullpath.substring(j+1);
            } 
            else 
                return fileFullpath.substring(i+1);
        } 
        catch (Exception e) {
            return "";
        }
    }
    
    public static String getFolderName(String fileFullpath) 
    {
        try {
            int i = fileFullpath.lastIndexOf("/");
            if (i <= 0) {
                return fileFullpath;
            } else {
                return fileFullpath.substring(0, i);
            }
        } catch (Exception e) {
            return "";
        }
    }

    public static String getFileExt(String filename)
    {
        int p = filename.lastIndexOf(".");
        
        if ( p == -1 )
            return "";
        else
            return filename.substring(p+1);
    }
    
    public static long getFileSize(String filename)
    {
        try
        {
            File file = new File(filename);
            if ( !file.exists() ) 
                return -1;
            
            return file.length();
        }
        catch(Exception e){
            return -1;
        }
    }
        
    public static String readTextFileToString(String filename)
    {
        StringBuilder fileContent = new StringBuilder();
        String str;
        
        try
        {
            File file = new File(filename);
            if ( !file.exists() ) 
                return null;
            
            InputStream in = new FileInputStream(file);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            
            while((str = br.readLine()) != null)
                fileContent.append(str).append(" ");
            
            in.close();
        }
        catch(Exception e){
            return null;
        }
        
        return fileContent.toString();
    }
     
    public static String readTextFileInClassPath(String filename)
    {
        String fileContent = "";
        
        try
        {
            InputStream in = FileUtil.class.getClassLoader().getResourceAsStream(filename);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            
            String str = null;
            
            while((str = br.readLine()) != null)
            {
                fileContent += str;
            }
        }
        catch(Exception e){
            return null;
        }
        return fileContent;
    }
    
    public static int getSheetMaxColumnNumber(Sheet sheet)
    {
        Row row;
        int maxColumnNumber = 0;
        
        Iterator<Row> rowIterator = sheet.iterator();
            
        int i=1;

        while (rowIterator.hasNext() ) 
        {
            //Get the row object
            row = rowIterator.next(); 
            
            if ( row.getLastCellNum() > maxColumnNumber )
                maxColumnNumber = row.getLastCellNum();
        }
        
        return maxColumnNumber;
    }
    
    public static List<Map<String,Object>> readExcelFileToMapList(String filename,InputStream fileStream, int processStartRow, boolean processMaxData,int processRowNumber,String excelSheetName)
    {
        Row row;
        StringBuilder lineBuilder;
        Map<String,Object> map;
        List<Map<String,Object>> contentList = new ArrayList<>();
        Sheet sheet;
        int columnNumber;

        try
        {
            //Create Workbook instance for xlsx/xls file input stream
            Workbook workbook = null;

            if(filename.toLowerCase().endsWith("xlsx"))
                workbook = new XSSFWorkbook(fileStream);
            else 
            if(filename.toLowerCase().endsWith("xls"))
                workbook = new HSSFWorkbook(fileStream);
          
            //Get the nth sheet from the workbook
            if ( excelSheetName == null || excelSheetName.trim().isEmpty() )
                sheet = workbook.getSheetAt(0);
            else
                sheet = workbook.getSheet(excelSheetName.trim());
 
            columnNumber = getSheetMaxColumnNumber(sheet);
            
            //every sheet has rows, iterate over them
            Iterator<Row> rowIterator = sheet.iterator();
            
            int i=1;

            while (rowIterator.hasNext() ) 
            {
                //Get the row object
                row = rowIterator.next(); 
                
                if ( i < processStartRow )
                {
                    i++;
                    continue;
                }
               
                DataFormatter formatter = new DataFormatter();
                
                lineBuilder = new StringBuilder();
                String strValue;
                    
                for (int j = 0; j < columnNumber; j++) 
                {
                    try
                    {
                        strValue = formatter.formatCellValue(row.getCell(j));
                        strValue = strValue.replace(ColumnSeparatorType.COMMA.getSeparator(), " ");
                    }
                    catch(Exception e)
                    {
                        log.warn("get column data failed! e="+e);
                        strValue = "";
                    }
                    
                    lineBuilder.append(strValue).append(ColumnSeparatorType.COMMA.getSeparator());
                }
                /*
                //Every row has columns, get the column iterator and iterate over them
                Iterator<Cell> cellIterator = row.cellIterator();

                while (cellIterator.hasNext()) 
                {
                    Cell cell = cellIterator.next();
                                       
                    String str = formatter.formatCellValue(cell);
                    
                    if ( cell.getCellType() == Cell.CELL_TYPE_STRING )
                        str = cell.getStringCellValue();
                    else
                    if ( cell.getCellType() == Cell.CELL_TYPE_NUMERIC )
                        str = String.valueOf(cell.getNumericCellValue());
                    else
                    if ( cell.getCellType() == Cell.CELL_TYPE_BOOLEAN )
                        str = cell.getBooleanCellValue()?"true":"false";
                    else
                        str = cell.getStringCellValue();
                    
                    str = str.replace(ColumnSeparatorType.COMMA.getSeparator(), " ");
                    lineBuilder.append(str).append(ColumnSeparatorType.COMMA.getSeparator());
                }*/
                
                String newStr = lineBuilder.toString();
                if ( newStr.length()>1 )
                    newStr = newStr.substring(0,newStr.length()-1);
           
                map = new HashMap<>(); log.info(" read line="+newStr);
                map.put("index",i);
                map.put("line",newStr);
                map.put("originalLine",new String(newStr));
                contentList.add(map);
                
                if ( i == processStartRow+processRowNumber && processMaxData==false )
                    break;
            
                i++;
            
            } //end of rows iterator
             
            fileStream.close();    
        }
        catch(Exception e)
        {
            log.error(" readExcelFileToMapList() failed! e="+e);
            return null;
        }        
         
        return contentList;
    }
    
    public static List<String> getExcelSheetName(String filename,InputStream fileStream)
    {
        List<String> sheetNames = new ArrayList<>();
        
        try
        {
            //Create Workbook instance for xlsx/xls file input stream
            Workbook workbook = null;

            if(filename.toLowerCase().endsWith("xlsx"))
                workbook = new XSSFWorkbook(fileStream);
            else 
            if(filename.toLowerCase().endsWith("xls"))
                workbook = new HSSFWorkbook(fileStream);
          
            for(int i=0;i<workbook.getNumberOfSheets();i++)
            {
                sheetNames.add(workbook.getSheetAt(i).getSheetName());
            }
            
            fileStream.close();
        }
        catch(Exception e)
        {
            log.error(" getExcelSheetName() failed! e="+e);
            return null;
        }        
         
        return sheetNames;
    }
    
    public static List<String> readLogFileToStringList(String filename, boolean fromTop,int lineNumber) throws IOException
    {
        String line;
        List<String> logInfo = new ArrayList<>();
        long fileLine = 0;        
        long startRow = 0;
        long endRow = 0;
        String datetimeStr = null;
        Date date;
        String lastDatetimeStr = null; 
        boolean foundTimeOnce = false;
        
        try
        {
            File file = new File(filename);
            if ( !file.exists() ) 
                return logInfo;
            
            if ( fromTop )
            {
                startRow = 1;
                endRow = lineNumber;
            }
            else
            {
                fileLine = getTextFileLine(file);
                
                startRow = fileLine - lineNumber + 1;
                endRow = fileLine;
            }
            
            InputStream in = new FileInputStream(file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            log.info(" read="+reader + " fileLine ="+fileLine+ " startRow="+startRow+" endRow="+endRow);

            int i = 1;

            while ((line = reader.readLine()) != null)
            {
                if ( i < startRow )
                {
                    i++;
                    continue;
                }

                line = Tool.removeInvisiableASIIC(line);
   
                if ( line.length() < 23 )
                    date = null;
                else
                {
                    datetimeStr = line.substring(0, 23);
                    date = Tool.convertStringToDate(datetimeStr, "yyyy-MM-dd hh:mm:ss,SSS");
                }

                if ( date == null )
                {
                    if ( foundTimeOnce == false )
                        continue;
                    else
                    {
                        if ( !line.trim().isEmpty() )
                            logInfo.add(lastDatetimeStr+line);
                    }
                }
                else
                {
                    foundTimeOnce = true;
                    lastDatetimeStr = datetimeStr;
                    logInfo.add(line);                        
                }


                if ( i == endRow )
                    break;

                i++;
            }

            in.close();
      
        }
        catch(Exception e)
        {
            log.error("readLogFileToStringList() failed! e="+e);
        }
               
        return logInfo;
    }
    
     public static List<String> readLogFileToStringListByTimeRange(String filename, Date startTime, Date endTime) throws IOException
    {
        Date date;
        String line;
        String datetimeStr = null;
        List<String> logInfo = new ArrayList<>();
        String lastDatetimeStr = null;
        boolean foundTimeOnce = false;
        boolean foundOnce = false;
 
        try
        {
            log.info("filename="+filename+ " startTime="+startTime+" endTime="+endTime);  
            
            File file = new File(filename);
            if ( !file.exists() ) 
                return logInfo;
                        
            InputStream in = new FileInputStream(file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            while ((line = reader.readLine()) != null)
            {
                line = Tool.removeInvisiableASIIC(line); 
                
                if ( line.length() < 23 )
                    date = null;
                else
                {
                    datetimeStr = line.substring(0, 23);
                    date = Tool.convertStringToDate(datetimeStr, "yyyy-MM-dd hh:mm:ss,SSS");
                }

                if ( date == null )
                {
                    if ( foundTimeOnce == false )
                        continue;
                    else
                    {
                        if ( !line.trim().isEmpty() && foundOnce )
                            logInfo.add(lastDatetimeStr+line);
                        
                        continue;
                    }
                }

                foundTimeOnce = true;
 
                if ( (date.equals(startTime) || date.after(startTime)) && (date.equals(endTime) || date.before(endTime)) )           
                {
                    logInfo.add(line);
                    foundOnce = true;
                    lastDatetimeStr = datetimeStr;  
                }
                
                if ( date.after(endTime) )
                    break;
            }

            in.close();
        }
        catch(Exception e)
        {
            log.error("readLogFileToStringList() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
        }
        log.info(" log size="+logInfo.size());
        
        return logInfo;
    }
     
    public static List<Map<String,Object>> readTxtFileToMapList(InputStream in, int encodingType, int processStartRow, boolean processMaxData,int processRowNumber) throws IOException
    {
        String line;
        Map<String,Object> map;
        List<Map<String,Object>> contentList = new ArrayList<>();
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(in,EncodingType.findByValue(encodingType).getEncodingStr()));
   
        log.info(" read="+reader);
        
        int i = 1;
        
        while ((line = reader.readLine()) != null)
        {
            if ( i < processStartRow )
            {
                i++;
                continue;
            }
            
            line = Tool.removeInvisiableASIIC(line);
            
            map = new HashMap<>(); log.info(" read line="+line);
            map.put("index",i);
            map.put("line",line);
            map.put("originalLine",new String(line));
            
            if ( i==2 ) 
                map.put("dataValidationInfo","字段：xxx 值不能为空");
            
             if ( i==3 ) 
                map.put("dataValidationInfo","字段：yyy 值超出范围");
            
            contentList.add(map);
            
            if ( i == processStartRow+processRowNumber && processMaxData==false )
                break;
            
            i++;
        }
        
        in.close();
        
        return contentList;
    }
      
    public static List<String> readTxtFileToStringList(String filename, int encodingType, int processStartRow, int processRowNumber,boolean processMaxData) throws IOException
    {
        String line;
        List<String> data = new ArrayList<>();
 
        File file = new File(filename);
          
        InputStream in = new FileInputStream(file);
            
        BufferedReader reader = new BufferedReader(new InputStreamReader(in,EncodingType.findByValue(encodingType).getEncodingStr()));
   
        log.info(" read="+reader);
        
        int i = 1;
        
        while ((line = reader.readLine()) != null)
        {
            if ( i < processStartRow )
            {
                i++;
                continue;
            }
            
            line = Tool.removeInvisiableASIIC(line);
            
            //log.info(" add line = ("+line+")");
            data.add(new String(line));
                 
            if ( i == processStartRow+processRowNumber && processMaxData==false )
                break;
            
            i++;
        }
        
        in.close();
        
        return data;
    }
    
    public static ByteBuffer readInputStreamToByteBuffer(InputStream in) throws IOException 
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        
        int count=0;
        byte[] buf = new byte[1024*8];
        
        while( (count=in.read(buf)) != -1 ) {
            out.write(buf, 0, count);
        }
        in.close();
        
        return ByteBuffer.wrap(out.toByteArray());
    }
    
    public static InputStream getFileInputStream(String filename)
    {
        InputStream in = null;
        
        try 
        {
            File file = new File(filename);
            if ( !file.exists() ) 
                return null;
            
            in = new FileInputStream(file);
        }
        catch(Exception e)
        {
            log.error("getFileInputStream failed! file="+filename+" e="+e);
            return null;
        }
        
        return in;
    }
        
    public static List<byte[]> readFileToByteArrayList(String filename,int sizePerBlock)
    {
        List<byte[]> fileStreamList = new ArrayList<>();
        
        try 
        {
            File file = new File(filename);
            if ( !file.exists() ) 
                return null;
            
            InputStream in = new FileInputStream(file);
            ByteArrayOutputStream out= new ByteArrayOutputStream();

            int count = 0;
            byte[] b = new byte[1024*8];

            while( (count=in.read(b)) != -1 )
            {
                out.write(b,0,count);
                
                if ( out.toByteArray().length > sizePerBlock )
                {
                    fileStreamList.add(out.toByteArray());
                    out= new ByteArrayOutputStream();
                }
            }
            in.close();
            
            if ( out.toByteArray().length>0 )
                fileStreamList.add(out.toByteArray());
            
            return fileStreamList;
        }
        catch(Exception e)
        {
            log.error("readFileToByteArray() failed! file="+filename+" e="+e);
            return null;
        }
    }

    public static ByteBuffer readSmbFileToByteBuffer(String filepath)
    {
        //SmbFile smbFile = new SmbFile("smb://" + remoteUsername + ":" + remotePassword + "@" + remoteFilepath);  
                
        try 
        {
            SmbFile smbFile = new SmbFile(filepath);
                    
            if ( !smbFile.exists() )
                return null;

            InputStream in = new SmbFileInputStream(smbFile); 
            ByteArrayOutputStream out= new ByteArrayOutputStream();

            int count = 0;
            byte[] b = new byte[CommonKeys.FILE_BUFFER_SIZE];

            while( (count=in.read(b)) != -1 )
            {
                out.write(b,0,count); 
                
                if ( out.size()>200000000 )
                    break;
            }

            in.close();

            return ByteBuffer.wrap(out.toByteArray());
        }
        catch(Exception e)
        {
            log.error("readSmbFileToByteArray() failed! file="+filepath+" e="+e);
            e.printStackTrace();
            return null;
        }       
    } 

    public static ByteBuffer readFileToByteBuffer(InputStream is)
    {
        try
        {              
            ByteArrayOutputStream out= new ByteArrayOutputStream();

            int count = 0;
            byte[] b = new byte[CommonKeys.FILE_BUFFER_SIZE];

            while( (count=is.read(b)) != -1 )
                out.write(b,0,count);

            is.close();

            return ByteBuffer.wrap(out.toByteArray());
        }
        catch(Exception e)
        {
            log.error("readFileToByteArray() failed! e="+e);
            return null;
        }       
    }
        
    public static ByteBuffer readFileToByteBuffer(File file)
    {
        try
        {
            if ( file == null)
                return null;
            
            if ( !file.exists() ) 
                return null;
              
            if ( file.length() <= CommonKeys.INTRANET_BIG_FILE_SIZE )
            {
                InputStream in = new FileInputStream(file); 
                ByteArrayOutputStream out= new ByteArrayOutputStream();

                int count = 0;
                byte[] b = new byte[CommonKeys.FILE_BUFFER_SIZE];

                while( (count=in.read(b)) != -1 )
                    out.write(b,0,count);

                in.close();

                return ByteBuffer.wrap(out.toByteArray());
            }
            else // for big file
            {
                RandomAccessFile raf = new java.io.RandomAccessFile(file,"r");
                FileChannel fileChannel  = raf.getChannel();
                MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, raf.length());
                
                fileChannel.close();
                raf.close();
                
                return buffer;
            }
        }
        catch(Exception e)
        {
            log.error("readFileToByteArray() failed! file="+file.getName()+" e="+e);
            e.printStackTrace();
            return null;
        }       
    }
        
    public static byte[] readFileToByteArray(File file)
    {
        try
        {
            if ( file == null)
                return null;
            
            if ( !file.exists() ) 
                return null;
              
            InputStream in = new FileInputStream(file); 
            ByteArrayOutputStream out= new ByteArrayOutputStream();

            int count = 0;
            byte[] b = new byte[CommonKeys.FILE_BUFFER_SIZE];

            while( (count=in.read(b)) != -1 )
                out.write(b,0,count);

            in.close();

            return out.toByteArray();
        }
        catch(Exception e)
        {
            log.error("readFileToByteArray() failed! file="+file.getName()+" e="+e);
            return null;
        }        
    }
     
    public static byte[] readInputStreamToByteArray(InputStream in)
    {
        try 
        {
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            int count = 0;
            byte[] b = new byte[CommonKeys.FILE_BUFFER_SIZE];

            while( (count=in.read(b)) != -1 )
                out.write(b,0,count);

            in.close();
            
            return out.toByteArray();
        }
        catch(Exception e)
        {
            log.error("readInputStreamToByteArray() failed! e="+e);
            return null;
        }
    }
       
    public static boolean deleteDir(File dir) 
    {
        if (dir.isDirectory()) 
        {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) 
            {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {  
                   return false;
                }
            }
        }

        // The directory is now empty so delete it
        return dir.delete();
    }
      
    public static void getFolderAllFiles(String folderName, List<DiscoveredDataobjectInfo> dataInfoList,Processor processor,boolean processSubFolder,boolean countLine,int organizationId,long jobId,DataService.Client dataService, int datasourceId, Job job, String businessDateStr) throws TException, Exception
    {
        log.info("new folder name="+folderName+" locale="+System.getProperty("user.language")+" encoding="+System.getProperty("file.encoding"));
 
        File path = new File(folderName);
        
        if ( !path.exists() ) 
        {
            log.error("folder not exists! folderName="+folderName);
            throw new Exception("folder_not_exists");
        }
        
        if (path.isDirectory() && path.canRead() && path.canWrite()) 
        {
	    //String[] allfiles = path.list();
            //if (allfiles == null) return;
            File[] allfiles = path.listFiles(); 
            
            for(File file : allfiles)
            {                   
                String fullpath = file.getAbsolutePath();
                
                String filename= file.getName();
                   
                log.info(" filename="+filename+" fullepath="+fullpath);
                                
                log.info(" isExist="+file.exists()+"  file.isFile="+file.isFile()+" file.isDirectory="+file.isDirectory()+" is count line="+countLine);
                
                if ( !file.isDirectory() ) // if it's a file 
                {
                    long lastModified =file.lastModified();
                    long createdTime = 0;
                            
                    BasicFileAttributes attr = getFileBasicAttributes(fullpath);
                    
                    if ( attr != null )
                        createdTime = attr.creationTime().toMillis();
                    
                    DiscoveredDataobjectInfo obj = new DiscoveredDataobjectInfo(null,0,folderName,filename,file.length(),lastModified>createdTime?lastModified:createdTime);
                    obj.setBusinessDate(businessDateStr);
                    
                    if ( countLine )
                    {
                       int line = getFileLine(file,filename);
                       log.info("line="+line+"filename="+filename);
                       
                       obj.setLine(line);
                    }
                    
                    if ( processor != null )
                    {
                        if ( processor.validateDataobject(obj) )                    
                            dataInfoList.add(obj);
                        else
                            log.debug(" bypass file =" + fullpath);
                    }
                    else
                        dataInfoList.add(obj);
                    
                    if ( countLine )
                    {
                        dataService.setDatasourceEndJobTime(organizationId, jobId, JobTimeType.LAST_UPDATED_TIME.getValue());
                    }
                    else
                    {
                        if ( dataInfoList.size() % 500 == 1 )
                            dataService.setDatasourceEndJobTime(organizationId, jobId, JobTimeType.LAST_UPDATED_TIME.getValue());
                    }
                }
                else
                {
                    if ( processSubFolder )
                        getFolderAllFiles(fullpath,dataInfoList,processor,processSubFolder,countLine,organizationId,jobId,dataService,datasourceId,job,businessDateStr);
                }
            }
	}
    }
    
    public static int getFileLine(File file,String filename)
    {
        try
        {
            log.info("getfileline() filename="+filename);
            
            if ( filename.endsWith(".txt") || filename.endsWith(".del") || filename.endsWith(".csv"))
                return getTextFileLine(file);
            else
            if ( filename.endsWith(".xls") || filename.endsWith(".xlsx") )
                return getExcelFileLine(filename,new FileInputStream(file),"");      
            else
            if ( filename.endsWith(".pst") )
                return getPstFileLine(file,new FileInputStream(file),"");  
        }
        catch(Exception e)
        {
            log.info("getFileLine() failed! e="+e+" filename="+filename);
        }
        
        return -1;
    }
    
    public static int getTextFileLine(File file)
    {
        String line;
        int lineNumber = 0;
        BufferedReader reader = null;
        
        try
        {
            reader = new BufferedReader(new FileReader(file)); 

            while((line=reader.readLine())!=null)
                lineNumber++;
            
            reader.close();
        }
        catch(Exception e)
        {
            log.error(" getFileLine failed! e="+e+" file ="+file.getAbsolutePath());
            return -1;
        }
        
        return lineNumber;
    }
     
    public static int getFileLine(InputStream in,int encodingType)
    {
        String line;
        int lineNumber = 0;
        
        try
        {
            BufferedReader reader = new BufferedReader(new InputStreamReader(in,EncodingType.findByValue(encodingType).getEncodingStr()));

            while((line=reader.readLine())!=null)
                lineNumber++;
            
            reader.close();
        }
        catch(Exception e)
        {
            log.error(" getFileLine failed! e="+e);
            return 0;
        }
        
        return lineNumber;
    }
     
    public static int getExcelFileLine(String filename,InputStream in,String excelSheetName)
    {
        int lineNumber = 0;
        Sheet sheet;
        
        try 
        {
             //Create Workbook instance for xlsx/xls file input stream
            Workbook workbook = null;

            if(filename.toLowerCase().endsWith("xlsx"))
                workbook = new XSSFWorkbook(in);
            else 
            if(filename.toLowerCase().endsWith("xls"))
                workbook = new HSSFWorkbook(in);

            //Get the nth sheet from the workbook
            if ( excelSheetName == null || excelSheetName.trim().isEmpty() )
                sheet = workbook.getSheetAt(0);
            else
                sheet = workbook.getSheet(excelSheetName.trim());

            //every sheet has rows, iterate over them
            Iterator<Row> rowIterator = sheet.iterator();

            while (rowIterator.hasNext() )
            {
                rowIterator.next();
                lineNumber++;
            }

            in.close();            
        }
        catch(Exception e)
        {
            log.error("getExcelFileLine faield! e="+e+ " filename="+filename);
        }        
        
        return lineNumber;
    }
    
    public static int getPstFileLine(File file,InputStream in,String excelSheetName)
    {
        List<String> list = new ArrayList<>();
        
        try 
        {
            PSTFile pstFile = new PSTFile(file);
            countFolder(pstFile.getRootFolder(),list);
        } 
        catch(Exception e) 
        {
            log.error("getPstFileLine() failed! e="+e);
            return -1;
        }
        
        return list.size();
    }
       
    private static void countFolder(final PSTFolder folder,List<String> list) throws Exception
    {
        // go through the folders...
        if (folder.hasSubfolders()) 
        {
            Vector<PSTFolder> childFolders = folder.getSubFolders();
            for (final PSTFolder childFolder : childFolders) {
                countFolder(childFolder,list);
            }
        }

        // process emails for this folder
        if (folder.getContentCount() > 0) 
        {
            PSTMessage email = (PSTMessage) folder.getNextChild();
            
            while (email != null) 
            {
                list.add(String.valueOf(email.getDescriptorNodeId()));
                email = (PSTMessage) folder.getNextChild();
            }
        }
    }

    public static void getFolderAllFiles(File dir,List<String> filenameList) 
    {                        
        if (dir.isDirectory() && dir.canRead() && dir.canWrite()) 
        {
	    File[] allfiles = dir.listFiles( new FileFilter() 
            {
                public boolean accept(File pathname) 
                {
                    if (pathname.canRead() ) 
                        return true;
                    else
                        return false;
	        }
	    });
            
            for(File file : allfiles)
            {
                if ( file.isFile() ) { // if it's a file 
                    filenameList.add(file.getPath());
                } else {
                    getFolderAllFiles(file, filenameList);
                }
            }
	}
    }
        
    public static void getAllFilesWithSamePrefix(File dir, final String prefix, List<String> filenameList) 
    {                        
        if (dir.isDirectory() && dir.canRead() && dir.canWrite()) 
        {
	    File[] allfiles = dir.listFiles( new FileFilter() 
            {
                public boolean accept(File pathname) 
                {
                    if (pathname.isFile() && pathname.canRead() && pathname.canWrite() 
                            && pathname.getName().startsWith(prefix) || pathname.isDirectory() ) 
                        return true;
                    else
                        return false;
	        }
	    });
            
            for(File file : allfiles)
            {
                if ( file.isFile() ) { // if it's a file 
                    filenameList.add(file.getPath());
                } else {
                    getAllFilesWithSamePrefix(file, prefix, filenameList);
                }
            }
	}
    }
    
    public static void getAllFilesWithSamePrefix1(File dir, final String prefix, List<String> filenameList) 
    {                        
        if (dir.isDirectory() && dir.canRead() && dir.canWrite()) 
        {
	    File[] allfiles = dir.listFiles( new FileFilter() 
            {
                public boolean accept(File path) 
                {
                    String fileName = path.getName();
                    if ( fileName.startsWith(prefix) ) 
                        return true;
                    else
                        return false;
	        }
	    });
            
            for(File file : allfiles)
            {
                if ( file.isFile() ) { // if it's a file 
                    filenameList.add(file.getName());
                } else {
                    getAllFilesWithSamePrefix(file, prefix, filenameList);
                }
            }
	}
    }
        
    public static void getAllFilesWithSameName(File dir, final String fileName, List<File> files) 
    {                        
        if (dir.isDirectory() && dir.canRead() && dir.canWrite()) 
        {
	    File[] allfiles = dir.listFiles( new FileFilter() 
            {
                public boolean accept(File pathname) 
                {
                    if (pathname.isFile() && pathname.canRead() && pathname.canWrite() 
                            && pathname.getName().equals(fileName) || pathname.isDirectory() ) 
                        return true;
                    else
                        return false;
	        }
	    });
            
            for(File file : allfiles)
            {
                if ( file.isFile() ) { // if it's a file 
                    files.add(file);
                } else {
                    getAllFilesWithSameName(file, fileName, files );
                }
            }
	}
    }
    
    public static BasicFileAttributes getFileBasicAttributes(String filename)
    {
        try {
            Path path = Paths.get(filename);
            BasicFileAttributeView basicView = Files.getFileAttributeView(path, BasicFileAttributeView.class);

            BasicFileAttributes basicAttr = basicView.readAttributes();

            return basicAttr;
        } catch (Exception Exception) {
            return null;
        }
    }
       
    public static String getFileOwner(String filename)
    {
        try {
            Path path = Paths.get(filename);
            FileOwnerAttributeView ownerView = Files.getFileAttributeView(path, FileOwnerAttributeView.class);
            return ownerView.getOwner().getName(); 
        } catch (IOException iOException) {
            return null;
        }
    }
    
    /*
     * save string to a file
     */
    public static boolean saveToFile(String fileName,byte[] content) throws Exception
    {
        try
        {       
            File fr=new File(fileName);
            if ( !fr.exists() ) fr.createNewFile();

            OutputStream out = new FileOutputStream(fr);
            out.write(content);
            out.close();
        }
        catch(Exception e)
        {
            log.error("saveToFile() failed! e="+e+" errorFileName="+fileName+"\n");
            throw e;
        }
         
        return true;
    } 
    
    public static void delFile(String fileName) 
    {
    	File _file = new File(fileName);    
        
        if ( _file.exists())
            _file.delete();
    }    
      
    public static boolean removeFile(String fileName) 
    {
    	File file = new File(fileName);    
        
        if ( file.exists()) 
        {
            file.delete();
            return true;
        }
        else
            return false;
    } 
    
    public static String byteArrayToHex(byte[] byteArray) 
    {
        char[] hexDigits = {'0','1','2','3','4','5','6','7','8','9', 'A','B','C','D','E','F' };
        char[] resultCharArray =new char[byteArray.length * 2];
   
        int index = 0;

        for (byte b : byteArray) 
        {
            resultCharArray[index++] = hexDigits[b>>> 4 & 0xf];
            resultCharArray[index++] = hexDigits[b& 0xf];
        }
        return new String(resultCharArray);
    }
    
    public static String isNullToChar(String v) 
    {
        if (v == null || "".equals(v))
            return "0";
        else
            return v;
    }
    
    public static String isNullToCharEmpty(String v,String delimiter) 
    {
        if (v == null || "".equals(v))
            return "";
        else
            return removeSpecialCharFromDB(v.replace(delimiter, ""));
    }
    
    public static String isNullToCharYH(String v,String delimiter) 
    {
        if (v == null || "".equals(v))
            return "\"\"";
        else
            return removeSpecialCharFromDB(v.replace(delimiter, ""));
    }
    
    public static String removeSpecialCharFromDB(String line) 
    {
        if (line == null || line.isEmpty())
                return "";

        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < line.length(); i++) 
        {
            char c = line.charAt(i);

            if (Tool.isChineseChar(c) || c >= '0' && c <= '9' || c >= 'A'
                            && c <= 'Z' || c >= 'a' && c <= 'z' || c == '_' || c == '-'
                            || c == '#' || c == '*' || c == '~' || c == '@' || c == '$'
                            || c == '%' || c == '^' || c == '&' || c == '(' || c == ')'
                            || c == '+' || c == '1' || c == '=' || c == '[' || c == ']'
                            || c == '{' || c == '}' || c == '/' || c == ';' || c == ','
                            || c == '.' || c == '>' || c == '<' || c == '?' || c == ':'
                            || c == '\'' || c == '\"' || c == ' ')
                    builder.append(c);
        }

        return builder.toString();
    }
}
