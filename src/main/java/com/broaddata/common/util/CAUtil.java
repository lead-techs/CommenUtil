/*
 * CAUtil.java  - Content Analysis Utility
 *
 */

package com.broaddata.common.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import org.apache.log4j.Logger;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MimeTypes;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.txt.UniversalEncodingDetector;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

public class CAUtil 
{      
    static final Logger log=Logger.getLogger("CAUtil");      

    private static final Detector DETECTOR = new DefaultDetector(MimeTypes.getDefaultMimeTypes());

    public static String detectMimeType(byte[] fileStream)
    {
        TikaInputStream tikaIS = null;
        
        try 
        {
            tikaIS = TikaInputStream.get(fileStream);

            final Metadata metadata = new Metadata();

            return DETECTOR.detect(tikaIS, metadata).toString();
        } 
        catch(Exception e)
        {
            e.printStackTrace();
            return null;
        }
        finally {
            if (tikaIS != null) 
            {
                try {
                    tikaIS.close();
                }catch(Exception e)
                {
                    return null;
                }
            }
        }
    }
        
    public static String detectMimeType(String fileName)
    {
        TikaInputStream tikaIS = null;
        
        try 
        {
            File file = new File(fileName);
            tikaIS = TikaInputStream.get(file);

            /*
             * You might not want to provide the file's name. If you provide an Excel
             * document with a .xls extension, it will get it correct right away; but
             * if you provide an Excel document with .doc extension, it will guess it
             * to be a Word document
             */
            final Metadata metadata = new Metadata();
            // metadata.set(Metadata.RESOURCE_NAME_KEY, file.getName());

            return DETECTOR.detect(tikaIS, metadata).toString();
        } 
        catch(Exception e)
        {
            e.printStackTrace();
            return null;
        }
        finally {
            if (tikaIS != null) 
            {
                try {
                    tikaIS.close();
                }catch(Exception e)
                {
                    return null;
                }
            }
        }
    }
        
    public static String detectMimeType(ByteBuffer fileStream)
    {
        TikaInputStream tikaIS = null;
        
        try 
        {
            fileStream.position(0);
           
            InputStream bis = new ByteBufferBackedInputStream(fileStream);
            tikaIS = TikaInputStream.get(bis);

            final Metadata metadata = new Metadata();

            return DETECTOR.detect(tikaIS, metadata).toString();
        } 
        catch(Exception e)
        {
            e.printStackTrace();
            return null;
        }
        finally {
            if (tikaIS != null) 
            {
                try {
                    tikaIS.close();
                }catch(Exception e)
                {
                    return null;
                }
            }
        }
    }
    
     public static String extractText(ByteBuffer fileStream,String contentType)
    {
        try 
        {
            fileStream.position(0);
            InputStream bis = new ByteBufferBackedInputStream(fileStream);

            Parser parser = new AutoDetectParser();

            ContentHandler handler = new BodyContentHandler(fileStream.capacity()+1);
            ParseContext context = new ParseContext();            
            context.set(Parser.class, parser);
            
            Metadata data = new Metadata();
            
            if ( contentType != null )
                data.set(Metadata.CONTENT_TYPE, contentType);
            
            parser.parse(bis, handler, data, context);

            return handler.toString();
        } 
        catch (Exception e) 
        {
            log.error("extractText failed! e="+e);
            return "";
        }
    }
 
    public static String extractText(ByteBuffer fileStream)
    {
        try 
        {
            fileStream.position(0);
            InputStream bis = new ByteBufferBackedInputStream(fileStream);

            Parser parser = new AutoDetectParser();

            ContentHandler handler = new BodyContentHandler(fileStream.capacity()+1);
            ParseContext context = new ParseContext();            
            context.set(Parser.class, parser);
            Metadata data = new Metadata();
            parser.parse(bis, handler, data, context);

            return handler.toString();
        } 
        catch (Throwable e) 
        {
            log.error("extractText failed! e="+e);
            return "";
        }
    }
    
    public static String extractText(byte[] fileStream)
    {
        try 
        {
            ByteArrayInputStream input = new ByteArrayInputStream(fileStream);
            
            Parser parser = new AutoDetectParser();
            
            ContentHandler handler = new BodyContentHandler(fileStream.length+1);
            ParseContext context = new ParseContext();            
            context.set(Parser.class, parser);
            Metadata data = new Metadata();
            parser.parse(input, handler, data, context);
            
            return handler.toString();
        } 
        catch (Throwable e) 
        {
            log.error("extractText failed! e="+e);
            return "";
        }
    }
    
    public static String extractText(InputStream input)
    {
        try 
        {
            Parser parser = new AutoDetectParser();
            
            ContentHandler handler = new BodyContentHandler();
            ParseContext context = new ParseContext();            
            context.set(Parser.class, parser);
            Metadata data = new Metadata();
            parser.parse(input, handler, data, context);
            
            return handler.toString();
        } 
        catch (Throwable e) 
        {
            log.error("extractText failed! e="+e);
            return "";
        }
    }
    
    public static String detectEncoding(ByteBuffer fileStream)
    {        
        try 
        {           
            fileStream.position(0);
            InputStream in = new ByteBufferBackedInputStream(fileStream);
            InputStream bufferedIn = new BufferedInputStream(in);
            
            Metadata metadata = new Metadata();
            Charset charset = new UniversalEncodingDetector().detect(bufferedIn,metadata);
            if ( charset!=null )
                return charset.name();
            else
                return "";
        } catch (Exception ex) {
            ex.printStackTrace();
            return "";
        }
    }    
        
    public static String detectEncoding(byte[] fileStream)
    {        
        InputStream in = null;    
        
        try 
        {
            in = new ByteArrayInputStream(fileStream);       
            InputStream bufferedIn = new BufferedInputStream(in);
            Metadata metadata = new Metadata();
            Charset charset = new UniversalEncodingDetector().detect(bufferedIn,metadata);
            if ( charset!=null )
                return charset.name();
            else
                return "";
        } 
        catch (Exception ex) {
            ex.printStackTrace();
            return "";
        }
        finally
        {
            try {
                in.close();
            }
            catch(Exception e ) {                
            }
        }        
    }    
    
    public static String detectEncoding(String filename)
    {        
        InputStream in = null;
        
        try 
        {
            File file = new File(filename);
            in = new FileInputStream(file);       
            InputStream bufferedIn = new BufferedInputStream(in);
            Metadata metadata = new Metadata();
            Charset charset = new UniversalEncodingDetector().detect(bufferedIn,metadata);
            if ( charset!=null )
                return charset.name();
            else
                return "";
        } catch (IOException ex) {
            ex.printStackTrace();
            return "";
        }
        finally
        {
            try {
                in.close();
            }
            catch(Exception e ) {                
            }
        }
    }
    
    public static String detectEncoding(InputStream in)
    {        
        try 
        {   
            InputStream bufferedIn = new BufferedInputStream(in);
            Metadata metadata = new Metadata();
            Charset charset = new UniversalEncodingDetector().detect(bufferedIn,metadata);
            if ( charset!=null )
                return charset.name();
            else
                return "";
        } catch (Exception ex) {
            ex.printStackTrace();
            return "";
        }
        finally
        {
            try {
                in.close();
            }
            catch(Exception e ) {                
            }
        }        
    }    
}
