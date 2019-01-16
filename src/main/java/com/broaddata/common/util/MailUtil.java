/*
 * MailUtil.java
 *
 */

package com.broaddata.common.util;

import com.broaddata.common.model.enumeration.EmailFieldNames;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;
import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message;  
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

public class MailUtil
{
    static final Logger log = Logger.getLogger("MailUtil");
   
    public static void sendMail(String sendTomailBox, String fromMailBox, String fromMailBoxUsername, String fromMailBoxPassword, String subject, String body, List<String> attachmentFiles) throws Exception  
    {    
        Properties properties = getMailProperties(fromMailBox);

        // Get the default Session object.
        Session session = Session.getDefaultInstance(properties);
        //session.setDebug(true);

        try
        {
             // Create a default MimeMessage object.
            MimeMessage message = new MimeMessage(session);
 
            String address = String.format("%1$s <%2$s>", javax.mail.internet.MimeUtility.encodeText("edf测试", "UTF-8", "B"), fromMailBox);
            // Set From: header field of the header.
            message.setFrom(new InternetAddress(address)); 

             // Set To: header field of the header.
            message.addRecipient(Message.RecipientType.TO,new InternetAddress(sendTomailBox));

             // Set Subject: header field
            message.setSubject(subject);

             // Now set the actual message
            //message.setText(body);
                    
            // Create the message part
            BodyPart messageBodyPart = new MimeBodyPart();
            messageBodyPart.setText(body);
           
            Multipart multipart = new MimeMultipart();
            multipart.addBodyPart(messageBodyPart);
         
            if ( attachmentFiles != null && !attachmentFiles.isEmpty() )
            {
                for( String filename : attachmentFiles)
                {
                    messageBodyPart = new MimeBodyPart();
                    DataSource source = new FileDataSource(filename);
                    messageBodyPart.setDataHandler(new DataHandler(source));
                    
                    String shortFilename = filename.substring(filename.indexOf(".")+1);
                    
                    if ( shortFilename == null || shortFilename.trim().isEmpty() )
                        shortFilename = filename;
                    
                    messageBodyPart.setFileName(shortFilename);
                    multipart.addBodyPart(messageBodyPart);
                 }
            }

            message.setContent(multipart);   

            // Send message
            Transport.send(message, fromMailBoxUsername, fromMailBoxPassword);
            log.info("Sent message successfully....");
        }
        catch (Exception e) 
        {
            log.error(" send email failed! e="+e.getMessage()+", stacktrace="+ExceptionUtils.getStackTrace(e));
            throw e;
        }
    }
                
    public static ByteBuffer saveToEmlFile(Map<String,String> emailData,Map<String,ByteBuffer> emailAttachments) throws Exception
    {    
        List<String> fileList = new ArrayList<>();
        
        String sendToMailBox = emailData.get(EmailFieldNames.TO.name());
        String fromMailBox = emailData.get(EmailFieldNames.FROM.name());
        String ccMailBox = emailData.get(EmailFieldNames.CC.name());
        String bccMailBox = emailData.get(EmailFieldNames.BCC.name());
        
        String fromMailBoxUsername = "";
        String fromMailBoxPassword = "";
        String subject = emailData.get(EmailFieldNames.SUBJECT.name());
        String body = emailData.get(EmailFieldNames.BODY.name());
              
        Properties properties = getMailProperties(fromMailBox);

        // Get the default Session object.
        Session session = Session.getDefaultInstance(properties);

        try
        {
             // Create a default MimeMessage object.
            MimeMessage message = new MimeMessage(session);

            // Set From: header field of the header.
            InternetAddress addr = new InternetAddress();
            addr.setPersonal(fromMailBox);
            message.setFrom(addr);

             // Set To: header field of the header.
            addr.setPersonal(sendToMailBox);
            message.addRecipient(Message.RecipientType.TO,addr);
            
            //message.addRecipient(Message.RecipientType.CC,new InternetAddress(ccMailBox));
            //message.addRecipient(Message.RecipientType.BCC,new InternetAddress(bccMailBox));

             // Set Subject: header field
            message.setSubject(subject);
            
             // Now set the actual message
           // message.setText(body);
                    
           // Create the message part
           BodyPart messageBodyPart = new MimeBodyPart();
           messageBodyPart.setText(body);
           
           Multipart multipart = new MimeMultipart();
           multipart.addBodyPart(messageBodyPart);
         
            if ( !emailAttachments.isEmpty() )
            {
                for(Map.Entry<String,ByteBuffer> entry : emailAttachments.entrySet())
                {
                    String filename = entry.getKey();
                    ByteBuffer fileBuf = entry.getValue();
                    
                    String tempFile = Tool.getSystemTmpdir()+"/"+UUID.randomUUID().toString();
                    FileUtil.writeFileFromByteBuffer(tempFile,fileBuf);
                  
                    messageBodyPart = new MimeBodyPart();
                    DataSource source = new FileDataSource(tempFile);
                    messageBodyPart.setDataHandler(new DataHandler(source));
          
                    messageBodyPart.setFileName(filename);
                    multipart.addBodyPart(messageBodyPart);
                    
                    fileList.add(tempFile);                    
                }
            }

            message.setContent(multipart);
           
            ByteArrayOutputStream out = new ByteArrayOutputStream(); 
            message.writeTo(out);
            
            for(String filename : fileList )
            {
                File file = new File(filename);
                file.delete();
            }
            
            return ByteBuffer.wrap(out.toByteArray());
        }
        catch(Exception e) 
        {
            log.error(" saveToEmlFile failed! e="+e);
            throw e;
        }
   }
    
    private static Properties getMailProperties(String sendFromMailBox)
    {
        Properties properties = System.getProperties();
        
       // properties.setProperty("mail.smtp.host", sendFromMailBox);
        properties.setProperty("mail.transport.protocol", "smtp");  
        properties.setProperty("mail.smtp.port", "25");
        properties.put("mail.smtp.starttls.enable", "true");
        properties.put("mail.smtp.auth", "true");
    
        if ( sendFromMailBox.contains("hotmail") )
        {
             properties.setProperty("mail.smtp.host", "smtp.live.com");
        }
        
        return properties;
    }
}