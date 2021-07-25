package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

public class ListFile {
  public static void main(String[] args) throws IOException {
    if (!fs.exists(new Path(userhdfsPath))) {
      fs.mkdirs(new Path(userhdfsPath));
    } else {
      System.out.println("路径已存在");
    }
    FileStatus[] list = fs.listStatus(new Path(hdfsPath));
    for (FileStatus f : list) {
      System.out.println(f.getPath().getName());
    }
  }

  public static String hdfsPath = null;
  public static String rootname = null;
  public static String username = null;
  public static String userhdfsPath = null;
  public static Configuration conf;
  public static FileSystem fs;
  static {
    InputStream in = ListFile.class.getResourceAsStream("/userinfo.properties");
    Properties p = new Properties();
    try {
      p.load(in);
      if (p.getProperty("hdfsPath") != null && p.getProperty("userName") != null) {
        hdfsPath = p.getProperty("hdfsPath");
        if (p.getProperty("rootName") != null) {
          rootname = "root";
        } else {
          rootname = p.getProperty("rootName");
        }
        System.setProperty("HADOOP_USER_NAME", rootname);
        username = p.getProperty("userName");
        userhdfsPath = hdfsPath + username;
        conf = new Configuration();
        conf.set("fs.defaultFS", hdfsPath);
        fs = FileSystem.get(URI.create(hdfsPath), conf);
      } else {
        System.out.println("请输入正确的hdfs路径");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
