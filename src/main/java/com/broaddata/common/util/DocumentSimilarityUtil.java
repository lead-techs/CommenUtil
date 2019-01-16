/*
 * DocumentSimilarityUtil.java
 */

package com.broaddata.common.util;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DocumentSimilarityUtil 
{
    static final Logger log=Logger.getLogger("DocumentSimilarityUtil");

    public static double getTwoDocumentSimilarityScore(LinkedHashMap firstDocumentTermVectorMap, LinkedHashMap secondDocumentTermVectorMap)
    {
        double score = 0.0;
        
        try
        {
            if ( firstDocumentTermVectorMap.size() == 0 || secondDocumentTermVectorMap.size()==0 )
                return 0.0;
            
            LinkedHashMap<String, Integer>[] maps = formalizeMap(firstDocumentTermVectorMap,secondDocumentTermVectorMap);
            
            score = calculateCos(maps[0],maps[1]);
            
            return (double)(Math.round(score*100)/100.0);
        }
        catch(Exception e)
        {
            log.error("getTwoDocumentSimilarityScore() failed!  e="+e.getMessage());
            e.printStackTrace();
            return -1;
        }
    }
    
    private static LinkedHashMap<String, Integer>[] formalizeMap(LinkedHashMap<String,Integer> map1, LinkedHashMap<String,Integer> map2) 
    {
        LinkedHashMap<String, Integer>[] result=new LinkedHashMap[2];
        result[0]=new LinkedHashMap<>();
        result[1]=new LinkedHashMap<>();
        
        for (Map.Entry<String, Integer> entry:map1.entrySet()) 
        {
            String key=entry.getKey();
            result[0].put(key, entry.getValue());
            
            if (map2.containsKey(key))
                result[1].put(key, map2.get(key));
            else
                result[1].put(key, 0);
        }
        
        for (Map.Entry<String, Integer> entry:map2.entrySet()) 
        {
            String key=entry.getKey();
            if (result[0].containsKey(key))
                    continue;
            
            result[0].put(key, 0);
            result[1].put(key, map2.get(key));
        }

        return result;
    }
    
    public static double calculateCos(LinkedHashMap<String, Integer> first,LinkedHashMap<String, Integer> second)
    {
        List<Map.Entry<String, Integer>> firstList = new ArrayList<>(first.entrySet());
        List<Map.Entry<String, Integer>> secondList = new ArrayList<>(second.entrySet());

        double vectorFirstModulo = 0.00;
        double vectorSecondModulo = 0.00;
        double vectorProduct = 0.00;
        
        for(int i=0;i<firstList.size();i++)
        {
            vectorSecondModulo += secondList.get(i).getValue().doubleValue()*secondList.get(i).getValue().doubleValue();
            vectorProduct += firstList.get(i).getValue().doubleValue()*secondList.get(i).getValue().doubleValue();            
            vectorFirstModulo += firstList.get(i).getValue().doubleValue()*firstList.get(i).getValue().doubleValue();
        }
        
        return vectorProduct/(Math.sqrt(vectorFirstModulo)*Math.sqrt(vectorSecondModulo));
    }
}
