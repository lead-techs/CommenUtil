/*
 * SEUtil.java -- SearchEngine Utility
 *
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.util.List;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.broaddata.common.model.enumeration.SearchEngineInstance;
import com.broaddata.common.thrift.dataservice.DiscoveredDataobjectInfo;

public class SEUtil 
{
    static final Logger log=Logger.getLogger("SEUtil");
                
    public static ByteBuffer readWebPageToByteBuffer(String urlStr) throws Exception
    {
        int retry = 0;
        
        while(true)
        {
            retry++;
            
            try 
            {
                log.info("get url="+urlStr);

                if ( !urlStr.startsWith("http:") )
                {
                    if ( urlStr.startsWith("//"))
                        urlStr="http:"+urlStr;
                    else
                       urlStr="http://"+urlStr;
                }

                URL url = new URL(urlStr);            
                URLConnection conn = url.openConnection(); 

                InputStream in = conn.getInputStream(); 
                ByteArrayOutputStream out= new ByteArrayOutputStream();

                int count = 0;
                byte[] b = new byte[CommonKeys.FILE_BUFFER_SIZE];

                while( (count=in.read(b)) != -1 )
                    out.write(b,0,count);

                in.close();

                return ByteBuffer.wrap(out.toByteArray());       
            }
            catch(Exception e)
            {
                log.error("readWebPageToByteBuffer() failed! e="+e);
                
                if ( retry > 100 )
                    throw e;
                
                Tool.SleepAWhileInMS(100);
            }    
        }
    }
     
    public static void getWebPagesFromSearchEngine(List<DiscoveredDataobjectInfo> dataInfoList,int searchEngineInstanceId,String keyword,int maxExpectedNumberOfResults) throws Exception
    {
        String linkHref;
        String urlContentStr;
        int pageSize = 10;
        String urlStr;
        String charset;
        String key;
        boolean reachEnd = false;
        Elements links;
        
        SearchEngineInstance searchEngineInstance = SearchEngineInstance.findByValue(searchEngineInstanceId);
        
        try 
        {
            // prepare search url
            key = URLEncoder.encode(keyword, "utf-8");
            
            if ( searchEngineInstance == SearchEngineInstance.BAIDU )
            {
                for(int i=0;i<maxExpectedNumberOfResults/pageSize+1;i++)
                {
                    urlStr = String.format(searchEngineInstance.getSearchUrl(),key,i*pageSize);

                    int retry =0;
                    
                    while(true)
                    {
                        retry++;
                        
                        try
                        {
                            charset = getUrlCharSet(urlStr);
                            
                            urlContentStr = getUrlContentString(urlStr,charset);

                            // parse found web page
                            Document doc = Jsoup.parse(urlContentStr);
              
                            Element result = doc.getElementById("content_left");             
                            links = result.getElementsByClass("c-container");
                            
                            break;
                        }
                        catch(Exception e)
                        {
                            log.error(" search baidu failed! retry ="+retry+" e="+e);
                            Tool.SleepAWhileInMS(10);
                            
                            if ( retry > 100 )
                                throw e;
                        }
                    }

                    for(Element e: links)
                    {
                        Elements aList = e.getElementsByTag("a");

                        for(Element a : aList)
                        {
                            linkHref = a.attr("href");    

                            if ( linkHref!=null && !linkHref.isEmpty() )
                            {
                                log.info("get old url="+urlStr);

                                if ( !linkHref.startsWith("http:") )
                                {
                                    if ( linkHref.startsWith("//"))
                                        linkHref="http:"+linkHref;
                                    else
                                        linkHref="http://"+linkHref;
                                }

                                //urlContentStr = getUrlContentString(linkHref);

                                //if ( urlContentStr != null && !urlContentStr.isEmpty() )
                                //{
                                    DiscoveredDataobjectInfo data = new DiscoveredDataobjectInfo();
                                    data.setName(linkHref);
                                    data.setPath("");
                                    //data.setSize(urlContentStr.length());
                                    data.setSize(0);

                                    dataInfoList.add(data);              
                                    
                                    if ( dataInfoList.size() == maxExpectedNumberOfResults )
                                    {
                                        reachEnd = true;
                                        break;
                                    }
                                //}
                            }
                            break;
                        }
                        
                        if ( reachEnd )
                            break;
                    }
                    
                    if ( reachEnd )
                        break;
                }
            }
            else
            if ( searchEngineInstance == SearchEngineInstance.BING )
            {

            }
            else
            if ( searchEngineInstance == SearchEngineInstance.GOOGLE )
            {

            }
        } 
        catch (Exception e) 
        {
            log.error("getWebPagesFromSearchEngine() failed! e="+e);
            throw e;
        }     
    }

    public static String getUrlCharSet(String url) throws IOException
    {      
        String sub = "utf-8";
        String s = "";
        String t="text";
        
        int retry = 0;
        
        while(true)
        {
            retry++;
            
            try
            {
                Document doc = Jsoup.connect(url).get();         
                Elements eles = doc.getElementsByTag("head").select("meta[http-equiv]");

                for(Element ele :eles)
                {
                  if(ele.attr("http-equiv").equals("Content-Type")||
                          ele.attr("http-equiv").equals("Content-type")||
                          ele.attr("http-equiv").equals("content-type")||
                          ele.attr("http-equiv").equals("content-Type"))
                  {
                      String[] str = ele.attr("content").split("[=]");
                      sub = str[1];  
                  }
                }
                break;
            }
            catch(IOException e)
            {
               log.error("getUrlCharSet failed! e="+e);
               
               if ( retry > 100 )
                   throw e;
               
               Tool.SleepAWhileInMS(100);
            }
        }

        return sub; 
    }
    
    public static String getUrlContentString(String urlStr,String charset) throws IOException 
    { 
        StringBuilder contentBuilder =new StringBuilder();
            
        int retry = 0;
        
        while(true)
        {
            retry++;
             
            try 
            {
                log.info("get url="+urlStr);

                URL url = new URL(urlStr);

                URLConnection conn = url.openConnection(); 

                BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), charset));
                String str = reader.readLine();
                while (str != null) {
                    contentBuilder.append(str);
                    str = reader.readLine();
                }
                reader.close();

                return contentBuilder.toString();            
            }
            catch(Exception e)
            {
                log.error("getUrlString() failed! e="+e);
                
                if ( retry > 100 )
                    throw e;
                
                Tool.SleepAWhileInMS(100);
            }
        }
    }
}
