/*
 * LogFileUtil.java  - Extract fields from string Utility
 *
 */

package com.broaddata.common.util;

import com.broaddata.common.model.enumeration.ColumnSeparatorType;
import com.broaddata.common.model.enumeration.ExtractValueFromStringType;
import com.broaddata.common.model.platform.LogFileDefinition;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List; 
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

public class LogFileUtil 
{      
    static final Logger log = Logger.getLogger("FieldExtracUtil");      

    public static Map<String, Object> extractFields(Map<String, Object> jsonMap,LogFileDefinition logFileDefinition,String logContentStr,List<Map<String,Object>> logFileFieldDefinition) throws Exception
    {        
        String name = "";
        String fieldValue = "";
        int dataType = 1;
        String datetimeFormat = null;
        int extractType;
        String extractParameter;
        String parameter[];
        boolean useAsEventSummary;
        StringBuilder summaryBuilder = new StringBuilder();

        for(Map<String,Object> field : logFileFieldDefinition)
        {
            fieldValue = "";
            
            try 
            {
                name = (String)field.get("name");
                dataType = Integer.parseInt((String)field.get("dataType"));
                datetimeFormat = (String)field.get("datetimeFormat");
                extractParameter = (String)field.get("extractParameter");                           
                extractType = Integer.parseInt((String)field.get("extractType"));
                parameter = extractParameter.split("\\,");

                int position = 0;

                switch( ExtractValueFromStringType.findByValue(extractType) )
                {
                    case ABSOLUTE_OFFSET: //start postion, number of char
                        fieldValue = logContentStr.substring(Integer.parseInt(parameter[0])-1, Integer.parseInt(parameter[0])-1+Integer.parseInt(parameter[1]));
                        break;
                        
                    case RELATIVE_OFFSET: //matchstr,number,endposition
                        for(int i=0;i<Integer.parseInt(parameter[1]);i++)
                        {
                            position = logContentStr.indexOf(parameter[0], position);

                            if ( position == -1 )
                                break;

                            position += parameter[0].length();                       
                        }
                        
                        fieldValue = logContentStr.substring(position, position+Integer.parseInt(parameter[2])).trim();
                        
                        position = fieldValue.indexOf(" ");
                        if ( position > -1 )
                            fieldValue = fieldValue.substring(0,position);
                        break;                    
                        
                    case COLUMN_EXTRACT: //number of column
                        String separator = ColumnSeparatorType.findByValue(Integer.parseInt(parameter[0])).getSeparator();
                        String[] columns = logContentStr.split(String.format("\\%s+", separator));
                        fieldValue = columns[Integer.parseInt(parameter[1])-1];
                        break;             
                        
                    case REGEXP_EXTRACT: //regExpression,number
                        Pattern pattern  = Pattern.compile(parameter[0]);
                        
                        Matcher m = pattern.matcher(logContentStr);
                        boolean found = false;
                        
                        for(int i=0;i<Integer.parseInt(parameter[1]);i++)
                        {
                            found = m.find(); 
                            if ( !found )
                                break;
                        }
                     
                        if ( found )
                        {
                            fieldValue = m.group();
                            fieldValue = fieldValue.replaceAll(parameter[2], "").trim();
                        }
                        break;
                }
                
                fieldValue = fieldValue.trim();
            }                
            catch(Exception e) {
            }
                                     
            if ( !fieldValue.isEmpty() )
            {
                useAsEventSummary = Integer.parseInt((String)field.get("useAsEventSummary"))==1;
                if ( useAsEventSummary)
                    summaryBuilder.append(fieldValue).append("-");
            }
            
            Object valueObject = Util.getValueObject(fieldValue, dataType, datetimeFormat);
            jsonMap.put(name, valueObject);
        }
        
        String newStr = summaryBuilder.toString();
        if ( !newStr.isEmpty() )
        {
            newStr = newStr.substring(0,newStr.length()-1);
            String objectName = (String)jsonMap.get("dataobject_name");
            jsonMap.put("dataobject_name", String.format("%s-%s",objectName,newStr));
        }
        
        return jsonMap;
    }
    
    public static String getXmlStrFromColumnDefinitions(List<Map<String,Object>> definitions) throws Exception
    {
        String NODES = "//logFileDefinition";
        String NODE = "column";        
        
        try
        {
            String INIT_XML  = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"+"\n"+"<logFileDefinition>\n"+"</logFileDefinition>";
            
            SAXReader saxReader = new SAXReader();
            ByteArrayInputStream in = new ByteArrayInputStream(INIT_XML.getBytes());
            Document doc= saxReader.read(in);

            Element element = (Element)doc.selectSingleNode(NODES);

            for ( Map<String,Object> definition : definitions)
            {
                Element NodeElement = element.addElement(NODE);

                Element nameElement = NodeElement.addElement("name");
                nameElement.setText(definition.get("name")==null?"":(String)definition.get("name"));
                
                Element descriptionElement = NodeElement.addElement("description");
                descriptionElement.setText(definition.get("description")==null?"":(String)definition.get("description"));
                
                Element dataTypeElement = NodeElement.addElement("dataType");
                dataTypeElement.setText(definition.get("dataType")==null?"":definition.get("dataType").toString());

                Element datatimeFormatElement = NodeElement.addElement("datetimeFormat");
                datatimeFormatElement.setText(definition.get("datetimeFormat")==null?"":(String)definition.get("datetimeFormat"));
                
                Element extractTypeElement = NodeElement.addElement("extractType");
                extractTypeElement.setText(definition.get("extractType")==null?"":definition.get("extractType").toString());
                
                Element extractParameterElement = NodeElement.addElement("extractParameter");
                extractParameterElement.setText(definition.get("extractParameter")==null?"":(String)definition.get("extractParameter"));
                
                Element useAsEventSummaryElement = NodeElement.addElement("useAsEventSummary");
                useAsEventSummaryElement.setText(definition.get("useAsEventSummary")==null?"":(String)definition.get("useAsEventSummary"));                
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            OutputFormat format = OutputFormat.createPrettyPrint(); 
           // OutputFormat format = new OutputFormat();
 
            XMLWriter output = new XMLWriter(out,format);
            output.write(doc);

            return out.toString("UTF-8");
        }
        catch(Exception e)
        {
            throw e;
        }
    }    
    
    public static List<Map<String,Object>> getColumnDefinitionsFromXmlStr(String xmlStr) throws Exception
    {
        Map<String,Object> definition;        
        String nodeStr = "//logFileDefinition/column";
        
        List<Map<String,Object>> definitions = new ArrayList<>();
        
        try
        {
            Document metadataXmlDoc = Tool.getXmlDocument(xmlStr);
            List<Element> nodes = metadataXmlDoc.selectNodes(nodeStr);

            for( Element node: nodes)
            {
                 definition = new HashMap<>();
                 
                 definition.put("id", UUID.randomUUID().toString() );
                 definition.put("name", node.element("name")!=null?node.element("name").getTextTrim():"");
                 definition.put("description", node.element("description")!=null?node.element("description").getTextTrim():""); 
                 definition.put("dataType", node.element("dataType")!=null?node.element("dataType").getTextTrim():"");
                 definition.put("datetimeFormat", node.element("datetimeFormat")!=null?node.element("datetimeFormat").getTextTrim():"");
                 definition.put("extractType", node.element("extractType")!=null?node.element("extractType").getTextTrim():"");    
                 definition.put("extractParameter", node.element("extractParameter")!=null?node.element("extractParameter").getTextTrim():"");                 
                 definition.put("useAsEventSummary", node.element("useAsEventSummary")!=null?node.element("useAsEventSummary").getTextTrim():""); 
                 
                 definitions.add(definition);
            }
            
            return definitions;
        }
        catch(Exception e)
        {
            throw e;
        }
    }

    private static String getRelativeOffset(String logContentStr, String[] parameter) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
     
}
