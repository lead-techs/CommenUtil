
package com.broaddata.common.pojo;

import com.broaddata.common.util.Tool;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

public class MetricsCalculationConfig implements Serializable
{
    static final Logger log = Logger.getLogger("MetricsCalculationConfig");
    
    private int calculationMethodTypeId;
    private int dataviewId = 0;
    private int scriptTypeId = 0;
    private int programTypeId = 0;
    private String programClass = "";
    private List<Map<String,String>> inputParameters;
    private int outputRowNumber = 1;
    private String outputColumnName = "";

    public int getCalculationMethodTypeId() {
        return calculationMethodTypeId;
    }

    public void setCalculationMethodTypeId(int calculationMethodTypeId) {
        this.calculationMethodTypeId = calculationMethodTypeId;
    }

    public int getDataviewId() {
        return dataviewId;
    }

    public void setDataviewId(int dataviewId) {
        this.dataviewId = dataviewId;
    }

    public int getScriptTypeId() {
        return scriptTypeId;
    }

    public void setScriptTypeId(int scriptTypeId) {
        this.scriptTypeId = scriptTypeId;
    }

    public int getProgramTypeId() {
        return programTypeId;
    }

    public void setProgramTypeId(int programTypeId) {
        this.programTypeId = programTypeId;
    }

    public String getProgramClass() {
        return programClass;
    }

    public void setProgramClass(String programClass) {
        this.programClass = programClass;
    }

    public List<Map<String, String>> getInputParameters() {
        return inputParameters;
    }

    public void setInputParameters(List<Map<String, String>> inputParameters) {
        this.inputParameters = inputParameters;
    }

    public int getOutputRowNumber() {
        return outputRowNumber;
    }

    public void setOutputRowNumber(int outputRowNumber) {
        this.outputRowNumber = outputRowNumber;
    }

    public String getOutputColumnName() {
        return outputColumnName;
    }

    public void setOutputColumnName(String outputColumnName) {
        this.outputColumnName = outputColumnName;
    }
    
    public String getCalculationConfigStr() throws Exception
    {
        Element element;
        Element childElement;
        Element newElement;
        Element newElement1;
        final String INIT_XML  = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"+"\n"+"<metricsCaculationDefinition>\n"+"</metricsCaculationDefinition>"; 
   
        try 
        {
            String metadataXML  = INIT_XML;
            
            SAXReader saxReader = new SAXReader();
            ByteArrayInputStream in = new ByteArrayInputStream(metadataXML.getBytes());
            org.dom4j.Document doc= saxReader.read(in);

            element = (Element)doc.selectSingleNode("//metricsCaculationDefinition");
            
            childElement = element.addElement("methodType");
            childElement.setText(String.valueOf(calculationMethodTypeId));
            
            childElement = element.addElement("dataview");
            childElement.setText(String.valueOf(dataviewId));
                                    
            childElement = element.addElement("scriptType");
            childElement.setText(String.valueOf(scriptTypeId));

            childElement = element.addElement("programType");
            childElement.setText(String.valueOf(programTypeId));

            childElement = element.addElement("programClass");
            childElement.setText(programClass);

            childElement = element.addElement("outputRowNumber");
            childElement.setText(String.valueOf(outputRowNumber));

            childElement = element.addElement("outputColumnName");
            childElement.setText(outputColumnName==null?"":outputColumnName);            
                                    
            childElement = element.addElement("inputParameters");
            
            for( Map<String,String> map : inputParameters )
            {
                newElement = childElement.addElement("parameter");
                
                newElement1 = newElement.addElement("parameterName");
                newElement1.setText(map.get("parameterName"));
                
                newElement1 = newElement.addElement("columnSelect");
                newElement1.setText(map.get("columnSelect"));
            }
                            
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            OutputFormat format = OutputFormat.createPrettyPrint(); 
 
            XMLWriter output = new XMLWriter(out,format);
            output.write(doc);

            return out.toString("UTF-8");
        }
        catch(Exception e)
        {
            log.error(" generateCaculationConfig() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
            throw e;
        }          
    }
      
    public void loadFromCalculationConfigStr(String configXml)
    {
        try
        {
            Document doc = Tool.getXmlDocument(configXml);
            Element node = (Element)doc.selectSingleNode("//metricsCaculationDefinition");
            
            calculationMethodTypeId = Integer.parseInt(node.element("methodType").getTextTrim());
            dataviewId = node.element("dataview")!=null?Integer.parseInt(node.element("dataview").getTextTrim()):0;
            scriptTypeId = node.element("scriptType")!=null?Integer.parseInt(node.element("scriptType").getTextTrim()):0;
            programTypeId = node.element("programType")!=null?Integer.parseInt(node.element("programType").getTextTrim()):0;
            programClass = node.element("programClass")!=null?node.element("programClass").getTextTrim():"";
            outputRowNumber = node.element("outputRowNumber")!=null?Integer.parseInt(node.element("outputRowNumber").getTextTrim()):0;
            outputColumnName = node.element("outputColumnName")!=null?node.element("outputColumnName").getTextTrim():"";
            
            Element childNode = node.element("inputParameters");
            List<Element> nodes = childNode.elements("parameter");
            
            inputParameters = new ArrayList<>();
            for( Element sNode: nodes)
            {
                Map map = new HashMap<>();
                map.put("parameterName",sNode.element("parameterName")!=null?sNode.element("parameterName").getTextTrim():"");
                map.put("columnSelect",sNode.element("columnSelect")!=null?sNode.element("columnSelect").getTextTrim():"");
            
                inputParameters.add(map);
            }
        }
        catch (Exception e) {
            log.error(" getValueFromCaculationConfig() failed! e="+e);
            throw e;
        }
    }    
}
