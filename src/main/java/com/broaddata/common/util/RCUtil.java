/*
 * RCUtil.java  - Runtime Command Utility
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class RCUtil 
{      
    static final Logger log=Logger.getLogger("RCUtil");      

    public static void executeCommand(String commandStr, String workingDir, boolean throwException, String errorFile, String outputFile,boolean noWait)
    {
        int exitVal = 0;
        Process proc;
        String[] commandArray;
      
        log.debug("path ="+System.getenv("PATH"));
        log.info("workingDir ="+System.getProperty("user.dir") );
        log.info("execute command:" + commandStr);
        
        commandArray = convertCommand(commandStr);

        try
        {
             ProcessBuilder builder = new ProcessBuilder(commandArray);

             if (errorFile != null)
                 builder.redirectError(new File(errorFile));
             
             if (outputFile != null)
                 builder.redirectOutput(new File(outputFile));
             
             if (workingDir != null)
                 builder.directory(new File(workingDir));
             
             proc = builder.start();

             if ( !noWait )
                 exitVal = proc.waitFor();            
        } 
        catch (Exception e) 
        {
            log.error("executeCommand() failed! e=" + e);
            
            if ( throwException )
                throw new RuntimeException("executeCommand() failed!");
        }
        
        log.info("executeCommand() exitVal=" + exitVal);
         
        if (exitVal != 0 && throwException)
        {
            log.error("executeCommand() failed! exitVal="+exitVal);
            throw new RuntimeException("executeCommand() failed!");
        }
        
        if ( FileUtil.getFileSize(errorFile) > 0 && throwException )
        {
            log.error("error file size > 0 errfile="+errorFile);
            throw new RuntimeException("executeCommand() failed!");
        }
    }
    
    private static String[] convertCommand(String commandStr)
    {
        int j;
        List<String> list=new ArrayList<>();
        String[] commandArray=new String[1];
        
        for(int i=0; i < commandStr.length(); i++)
        {
            j = commandStr.indexOf(' ', i);
            
            if ( j != -1 )
            {
                String val = commandStr.substring(i, j);
                if ( val.length()>0 )
                    list.add(val);
                
                i = j;
            } else {
                String val = commandStr.substring(i);
                if ( val.length()>0 )
                    list.add(val);      
                
                break;
            }
        }
        
        commandArray = (String[]) list.toArray(commandArray);
        
        return commandArray;
    }
}
