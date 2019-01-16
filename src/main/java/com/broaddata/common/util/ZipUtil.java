package com.broaddata.common.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 *
 * @author Duenan
 */
public class ZipUtil
{
    protected static byte[]    buf    = new byte[1024];
 
    private ZipUtil()
    {

    }
 
    private static void recurseFiles(final JarOutputStream jos, final File file, final String pathName)
            throws IOException, FileNotFoundException
    {
        if (file.isDirectory())
        {
            final String sPathName = pathName + file.getName() + "/";
            jos.putNextEntry(new JarEntry(sPathName));
            final String[] fileNames = file.list();
            if (fileNames != null)
            {
                for (int i = 0; i < fileNames.length; i++)
                {
                    recurseFiles(jos, new File(file, fileNames[i]), sPathName);
                }

            }
        }
        else
        {
            final JarEntry jarEntry = new JarEntry(pathName + file.getName());
            final FileInputStream fin = new FileInputStream(file);
            final BufferedInputStream in = new BufferedInputStream(fin);
            //开始写入新的 ZIP 文件条目并将流定位到条目数据的开始处。
            jos.putNextEntry(jarEntry);

            int len;
            while ((len = in.read(buf)) >= 0)
            {
                jos.write(buf, 0, len);
            }

            in.close();
            jos.closeEntry();
        }
    }
 
    public static void makeDirectoryToZip(    final File directory,
                                            final File zipFile,
                                            final String zipFolderName,
                                            final int level) throws IOException, FileNotFoundException
    {

        FileOutputStream fos = null;
        try
        {
            fos = new FileOutputStream(zipFile);
        }
        catch (final Exception e)
        {
            new File(zipFile.getParent()).mkdirs();
            zipFile.createNewFile();
            fos = new FileOutputStream(zipFile);
        }

        final JarOutputStream jos = new JarOutputStream(fos, new Manifest());
        jos.setLevel(checkZipLevel(level));
        final String[] fileNames = directory.list();
        if (fileNames != null)
        {
            for (int i = 0; i < fileNames.length; i++)
            {
                recurseFiles(jos, new File(directory, fileNames[i]), zipFolderName == null ? "" : zipFolderName);
            }
        }

        jos.close();

    }
 
    public static int checkZipLevel(final int level)
    {
        if (level < 0 || level > 9)
        {
            return 7;
        }
        else
        {
            return level;
        }
    }
    
    public static void main(final String args[]) throws FileNotFoundException, IOException
    {
        //makeDirectoryToZip();
        //final String homeDir = System.getProperty("user.dir");
        final String homeDir = "F:\\tmp"; 
        
        //final File zipFile = new File(homeDir, "download" + File.separatorChar + "test.zip");
        //final File zipFile = new File(homeDir, File.separatorChar + "test.zip");   
        final File zipFile = new File("F:\\test", File.separatorChar + "test12211.zip");

        //final File pagesDirectory = new File(homeDir, "src");
        final File pagesDirectory = new File("F:\\tmp");
        System.out.println("Making zip file from folder /src to " + zipFile);

        ZipUtil.makeDirectoryToZip(pagesDirectory, zipFile, "", 9);

        System.out.println("Zip file " + zipFile + " has been made.");
        System.out.println("---------------test github -----------");
    }
    
}
 