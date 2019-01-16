/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.manager;


import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.File;
import java.util.Date;
import javax.persistence.EntityManager;

import com.broaddata.common.model.organization.ImageSet;
import com.broaddata.common.util.FileUtil;
import com.broaddata.common.util.Util;
import com.broaddata.common.model.enumeration.ImageSetType;

public class ImageSetManager 
{
    static final Logger log = Logger.getLogger("ImageSetManager");

    public void init() {
    }

    public static ImageSet createImageSet(EntityManager platformEm,EntityManager em, int organizationId, String name, String description) throws Exception 
    {
        ImageSet imageSet;
        String indexPath = "/edf/storage/image_set";
        int objectStorageId = 0;
        
        try 
        {
            String value = Util.getSystemConfigValue(platformEm,0,"video","default_image_set_root_index_path");
                
            if ( !value.isEmpty() )
                indexPath = value;
            
            indexPath = String.format("%s/%s",indexPath,name);
                      
            value = Util.getSystemConfigValue(platformEm,0,"video","default_object_storage_id_for_image_set");
               
            if ( !value.isEmpty() )
                objectStorageId = Integer.parseInt(value);
            
            em.getTransaction().begin();

            imageSet = new ImageSet();

            imageSet.setOrganizationId(organizationId);
            imageSet.setName(name);
            imageSet.setDescription(description);
            imageSet.setIndexPath(indexPath);
            imageSet.setObjectStorageId(objectStorageId);
            imageSet.setCreateTime(new Date());
            imageSet.setCreatedBy(0);
            imageSet.setStatusTime(new Date());
            imageSet.setType(ImageSetType.EDF_TYPE.getValue());
            
            em.persist(imageSet);
            em.getTransaction().commit();

            return imageSet;
        } catch (Exception e) {
            log.error(" createImageSet() failed! e=" + e);
            throw e;
        }
    }
    
    // void updateImageSet(int organizationId, int imageSetId, String name, String description) 
    public static void updateImageSet(EntityManager em, int organizationId, int imageSetId, String name, String description) 
    {
        ImageSet imageSet;
        try 
        {
            em.getTransaction().begin();

            imageSet = em.find(ImageSet.class, imageSetId);
         
            imageSet.setName(name);
            imageSet.setDescription(description);

            em.merge(imageSet);
            em.getTransaction().commit();
        } catch (Exception e) {
            throw e;
        }
    }

    // List<Map<String, String>> getImageSets(int organizationId, int imageSetId)
    public static List<Map<String, String>> getImageSets(EntityManager em, int imageSetId) 
    {
        List<Map<String, String>> imageSetList = new ArrayList();

        List<ImageSet> imageSets;

        if (imageSetId == 0)
            imageSets = em.createQuery("from ImageSet").getResultList();
        else
            imageSets = em.createQuery("from ImageSet where id=" + imageSetId).getResultList();

        for (ImageSet imageSet : imageSets) 
        {
            Map<String, String> map = new HashMap();

            map.put("id", String.valueOf(imageSet.getId()));
            map.put("name", imageSet.getName() == (null) ? "" : imageSet.getName());
            map.put("description", imageSet.getDescription() == null ? "" : imageSet.getDescription());
            map.put("indexPath", imageSet.getIndexPath() == (null) ? "" : imageSet.getIndexPath());
            
            imageSetList.add(map);
        }

        return imageSetList;
    }

    public static void removeImageSet(EntityManager em, int imageSetId) 
    {
        try 
        {
            em.getTransaction().begin();
            // 
            ImageSet imageSet = (ImageSet) em.find(ImageSet.class, imageSetId);
            //删除目录
            boolean deleteDir = FileUtil.deleteDir(new File(imageSet.getIndexPath()));
            em.remove(imageSet);

            em.getTransaction().commit();
        }
        catch (Exception e) {
            throw e;
        }
    }
}
