package com.broaddata.common.util;

import org.apache.commons.beanutils.BeanUtils;

import java.io.*;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * 文件操作
 */
public class CSVUtils {

  
  @SuppressWarnings("rawtypes")
  public static File createCSVFile(List exportData, LinkedHashMap map, String outPutPath, String fileName) {
    File csvFile = null;
    BufferedWriter csvFileOutputStream = null;
    try {
      File file = new File(outPutPath);
      if (!file.exists()) {
        file.mkdir();
      }
      //定义文件名格式并创建
      csvFile = File.createTempFile(fileName, ".csv", new File(outPutPath));
      System.out.println("csvFile：" + csvFile);
      System.out.println("csvFileName：" + csvFile.getName());
      // UTF-8使正确读取分隔符"," 
      csvFileOutputStream = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
        csvFile), "UTF-8"), 1024);
      System.out.println("csvFileOutputStream：" + csvFileOutputStream);
      // 写入文件头部 
      for (Iterator propertyIterator = map.entrySet().iterator(); propertyIterator.hasNext();) {
        java.util.Map.Entry propertyEntry = (java.util.Map.Entry) propertyIterator.next();
        csvFileOutputStream.write("\"" + (String) propertyEntry.getValue() != null ? (String) propertyEntry.getValue() : "" + "\"");
        if (propertyIterator.hasNext()) {
          csvFileOutputStream.write(",");
        }
      }
      csvFileOutputStream.newLine();
      // 写入文件内容 
      for (Iterator iterator = exportData.iterator(); iterator.hasNext();) {
        Object row = (Object) iterator.next();
        for (Iterator propertyIterator = map.entrySet().iterator(); propertyIterator
          .hasNext();) {
          java.util.Map.Entry propertyEntry = (java.util.Map.Entry) propertyIterator
            .next();
          csvFileOutputStream.write((String) BeanUtils.getProperty(row,
            (String) propertyEntry.getKey()));
          if (propertyIterator.hasNext()) {
            csvFileOutputStream.write(",");
          }
        }
        if (iterator.hasNext()) {
          csvFileOutputStream.newLine();
        }
      }
      csvFileOutputStream.flush();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        csvFileOutputStream.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return csvFile;
  }


  /**
   * 删除该目录filePath下的所有文件
   * @param filePath
   *      文件目录路径
   */
  public static void deleteFiles(String filePath) {
    File file = new File(filePath);
    if (file.exists()) {
      File[] files = file.listFiles();
      for (int i = 0; i < files.length; i++) {
        if (files[i].isFile()) {
          files[i].delete();
        }
      }
    }
  }

  /**
   * 删除单个文件
   * @param filePath
   *     文件目录路径
   * @param fileName
   *     文件名称
   */
  public static void deleteFile(String filePath, String fileName) {
    File file = new File(filePath);
    if (file.exists()) {
      File[] files = file.listFiles();
      for (int i = 0; i < files.length; i++) {
        if (files[i].isFile()) {
          if (files[i].getName().equals(fileName)) {
            files[i].delete();
            return;
          }
        }
      }
    }
  }

}