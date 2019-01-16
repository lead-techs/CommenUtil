package com.broaddata.common.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.LinkedHashMap;
/**
 * Created by Duenan on 2018/4/28.
 */
public class CsvUtil {
    
    public static <T> String exportCsv(String[] titles, String[] propertys, List<LinkedHashMap<String,String>> list, String filePathName) throws IOException, IllegalArgumentException, IllegalAccessException{
        File file = new File(filePathName);
        OutputStreamWriter ow = new OutputStreamWriter(new FileOutputStream(file), "gbk");
        for(int i=0;i<titles.length;i++){
            if(i!= titles.length-1){
                ow.write(titles[i]);
                ow.write(",");
            }else {
                ow.write(titles[i]);
            }
        }
 
        ow.write("\r\n");
        for(Map<String,String> obj : list){
            Set<String> keys = obj.keySet();
            Iterator<String> it = keys.iterator();
            while (it.hasNext()) {
                String str = it.next();
                for(int i=0;i<titles.length;i++){
                    String title = titles[i];
                    if(str.equals(title)){
                        if(i!= titles.length-1){
                            ow.write(obj.get(str).toString());
                            ow.write(",");
                            continue;
                        }else {
                            ow.write(obj.get(str).toString());
                        }
                    }
                }

            }
            ow.write("\r\n");
        }
        ow.flush();
        ow.close();
        return "0";
    }
}
