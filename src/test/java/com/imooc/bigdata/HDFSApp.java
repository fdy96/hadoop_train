package com.imooc.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;


public class HDFSApp {

    /**
     * 使用Java API操作HDFS文件系统
     * 关键点：
     * 1）创建Configuration
     * 2）获取FileSystem
     * 3）编写你的HDFS_API操作
     */
    public static final String HDFS_PATH = "hdfs://192.168.1.233:8020";
    Configuration configuration;
    FileSystem fileSystem;

    @Before
    public void setUp() throws Exception {
        System.out.println("-------setup-------");

        configuration = new Configuration();
        configuration.set("dfs.replication", "1");

        /**
         * 构造一个访问指定HDFS系统的客户端对象
         * @param uri HDFS的URI
         * @param conf 客户端自定义的配置参数
         * @param user 客户端的身份，说白了就是用户名
         */
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");
    }

    /**
     * 查看目标文件夹下的所有文件
     * #不包括子文件夹下的文件
     */
    @Test
    public void listFiles() throws Exception {
        FileStatus[] statuses = fileSystem.listStatus(new Path("/hdfsapi/test"));
        for (FileStatus file : statuses) {
            printFileStatus(file);
        }
    }

    private void printFileStatus(FileStatus file) {
        String isDir = file.isDirectory() ? "文件夹" : "文件";
        String permission = file.getPermission().toString();
        short replication = file.getReplication();
        long length = file.getLen();
        String path = file.getPath().toString();

        System.out.println(isDir + "\t" + permission + "\t" +
                replication + "\t" + length + "\t" + path);
    }

    /**
     * 能够以递归的方式遍历文件夹下所有文件
     * #包括子文件夹下的文件
     */
    @Test
    public void listFileRecursive() throws Exception {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(
                new Path("/hdfsapi"), true);
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            printFileStatus(file);
        }
    }

    /**
     * 查看文件内容
     */
    @Test
    public void text() throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/cdh_version.properties"));
        IOUtils.copyBytes(in, System.out, 1024);
    }

    /**
     * 查看文件块信息
     */
    @Test
    public void getFielBlockLocations() throws Exception {
        FileStatus fileStatus = fileSystem.getFileStatus(
                new Path("/hdfsapi/test/idea.dmg"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus,
                0, fileStatus.getLen());

        for (BlockLocation block : blocks) {
            for (String name : block.getNames()) {
                System.out.println(name + ":" + block.getOffset() + ":" +
                        block.getLength() + ":" + block.getHosts());
            }
        }
    }

    /**
     * 创建文件夹
     */
    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /**
     * 创建文件
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/b.txt"));
        out.writeUTF("hello world");
        out.flush();
        out.close();
    }

    /**
     * 删除文件
     */
    @Test
    public void delete() throws Exception {
        fileSystem.delete(new Path("/hdfsapi/test/idea.dmg"), true);
    }

    /**
     * 文件名更改/文件移动
     */
    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/hdfsapi/test/b.txt");
        Path newPath = new Path("/hdfsapi/test/c.txt");
        fileSystem.rename(oldPath, newPath);
    }

    /**
     * 拷贝本地文件到HDFS文件系统
     */
    @Test
    public void copyFromLocalFile() throws Exception {
        Path src = new Path("/Users/fengdeyu/Documents/GitHub/python_code/README.md");
        Path dst = new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(src, dst);
    }

    /**
     * 拷贝大文件到HDFS文件系统
     * #带进度
     */
    @Test
    public void copyFromLocalBigFile() throws Exception {
        InputStream in = new BufferedInputStream(new FileInputStream(
                new File("/Users/fengdeyu/Downloads/ideaIU-2020.1.2.dmg")));
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/idea.dmg"),
                new Progressable() {
                    @Override
                    public void progress() {
                        System.out.print("-");
                     }
        });
        IOUtils.copyBytes(in, out, 20480);
    }

    /**
     * 拷贝HDFS文件到本地：下载
     */
    @Test
    public void copyToLocalFile() throws Exception{
        Path src=new Path("/hdfsapi/test/README.mdå");
        Path dst=new Path("/Users/fengdeyu/Downloads/");
        fileSystem.copyToLocalFile(src,dst);
    }


    @After
    public void tearDown() {
        configuration = null;
        fileSystem = null;
        System.out.println("-------tearDown-------");
    }
}
