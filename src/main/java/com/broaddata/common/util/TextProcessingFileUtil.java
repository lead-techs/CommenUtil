/*
 * TextProcessingFileUtil.java  - Extract fields from string Utility
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

public class TextProcessingFileUtil 
{      
    static final Logger log = Logger.getLogger("TextProcessingFileUtil");      

    public static Map<String, Object> extractFields(Map<String, Object> resultMap,String contentStr,List<Map<String,Object>> metadataDefinitionList) throws Exception
    {        
        String name;
        String fieldValue;
        int dataType;
        String datetimeFormat;
        int extractType;
        String extractParameter;
        String parameter[];
 
        for(Map<String,Object> field : metadataDefinitionList)
        {
            name = "";
            fieldValue = "";
            dataType = 0;
            datetimeFormat = "";
            
            try 
            {
                name = (String)field.get("name");
                dataType = Integer.parseInt((String)field.get("dataType"));
                datetimeFormat = (String)field.get("datetimeFormat");
                extractParameter = (String)field.get("extractParameter");                           
                extractType = Integer.parseInt((String)field.get("extractType"));
                parameter = extractParameter.split("\\"+CommonKeys.FIELD_SEPERATOR);

                int position = 0;

                switch( ExtractValueFromStringType.findByValue(extractType) )
                {
                    case ABSOLUTE_OFFSET: //start postion, number of char
                        fieldValue = contentStr.substring(Integer.parseInt(parameter[0])-1, Integer.parseInt(parameter[0])-1+Integer.parseInt(parameter[1]));
                        break;
                        
                    case RELATIVE_OFFSET: //matchstr,number,endposition
                        for(int i=0;i<Integer.parseInt(parameter[1]);i++)
                        {
                            position = contentStr.indexOf(parameter[0], position);

                            if ( position == -1 )
                                break;

                            position += parameter[0].length();                       
                        }
                        
                        fieldValue = contentStr.substring(position, position+Integer.parseInt(parameter[2])).trim();
                        
                        position = fieldValue.indexOf(" ");
                        if ( position > -1 )
                            fieldValue = fieldValue.substring(0,position);
                        
                        break;                    
                        
                    case COLUMN_EXTRACT: //number of column
                        String separator = ColumnSeparatorType.findByValue(Integer.parseInt(parameter[0])).getSeparator();
                        String[] columns = contentStr.split(String.format("\\%s+", separator));
                        fieldValue = columns[Integer.parseInt(parameter[1])-1];
                        
                        if ( parameter.length==3 && !parameter[2].isEmpty() )
                        {
                            fieldValue = Tool.removeItemsFromString(fieldValue, parameter[2]);
                        }
                        
                        break;             
                        
                    case REGEXP_EXTRACT: //regExpression,number
                        Pattern pattern  = Pattern.compile(parameter[0]);
                        
                        Matcher m = pattern.matcher(contentStr);
                        boolean found = false;
                        
                        for(int i=0;i<Integer.parseInt(parameter[1]);i++)
                        {
                            found = m.find(); 
                            if ( !found )
                                break;
                        }
                     
                        if ( found )
                        {
                            fieldValue = m.group().trim();

                            if ( parameter.length==3 && !parameter[2].isEmpty() )
                            {
                                fieldValue = Tool.removeItemsFromString(fieldValue, parameter[2]);
                            }
                        }
                        break;
                }
                
                fieldValue = fieldValue.trim();
            }                
            catch(Exception e) {
            }
            
 
            resultMap.put(name, fieldValue);
        }
 
        
        return resultMap;
    }
    
    public static String getXmlStrFromMetadataDefinitions(List<Map<String,Object>> definitions) throws Exception
    {
        String NODES = "//textProcessingFileDefinition";
        String NODE = "metadata";        
        
        try
        {
            String INIT_XML  = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"+"\n"+"<textProcessingFileDefinition>\n"+"</textProcessingFileDefinition>";
            
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
    
    public static List<Map<String,Object>> getMetadataDefinitionsFromXmlStr(String xmlStr) throws Exception
    {
        Map<String,Object> definition;        
        String nodeStr = "//textProcessingFileDefinition/metadata";
        
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
