package priv.lzj.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;

/**
 * 该类实现了一些基本的hdfs操作，实现大概步骤主要为：
 * 设定config（fs地址，若要追加写入的设置）-->通过config获取fileSystem
 * --> 通过fs（加上读写目录）获取读写流-->实现读写操作-->关闭流-->关闭fileSystem
 */

public class HdfsAction {
    private Configuration config;
    private FileSystem hdfsFs = null;
    private OutputStream out = null;
    private FSDataInputStream fsr = null;
    private Path outPath = null;
    public BufferedReader bfr = null;

    /**
     *构造函数，可以指定fs地址（初始化fs）
     */
    public HdfsAction(){}
    public HdfsAction(String fsDefault){
        initHdfsFs(fsDefault);
    }

    /**
     * 配置config（append时所需的配置使用后者），构建filesystem
     * @param defaultFs
     */
    public void initHdfsFs(String defaultFs){
        this.config = new Configuration();
        config.set("fs.defaultFS",defaultFs);
        try {
            hdfsFs = FileSystem.get(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void initHdfsFsAppend(String defaultFs){
        this.config = new Configuration();
        config.set("fs.defaultFS",defaultFs);
        config.setBoolean("dfs.support.append",true);
        config.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        config.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        try {
            hdfsFs = FileSystem.get(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *打开输入流（需入参输出路径，且需预先初始化filesystem），若输出路径存在则追加，若不存在则创建文件
     * @param path
     */
    public void openOutputStream(String path){
        outPath = new Path(path);
        if(!fileIsExist(outPath)) {
            createFile(path);
        }
        try {
            out = hdfsFs.append(outPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 打开输入流，之后可用bfr进行行读取
     * @param inPath
     */
    public void openReader(String inPath){
        Path pa = new Path(inPath);
        try {
            fsr = hdfsFs.open(pa);
            bfr = new BufferedReader(new InputStreamReader(fsr));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建文件（入参路径），需初始化filesystem
     * @param filePath
     */
    public void createFile(String filePath){
        Path pa = new Path(filePath);
        try {
            hdfsFs.create(pa).close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断文件是否存在（入参路径，返参boolean），需初始化filesystem
     * @param path
     * @return
     */
    public Boolean fileIsExist(Path path){
        try {
            return hdfsFs.exists(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 追加写入文件（入参所需追加的文本），需初始化filesystem并打开输出流
     * @param message
     */
    public void messageAppend(String message){
        if(this.fileIsExist(this.outPath)) {
            try {
                ((FSDataOutputStream) out).writeUTF(message + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 关闭filesystem，输出流和输入流
     */
    public void closeFS(){
        try {
            hdfsFs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void closeOutputStream(){
        try {
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void closeReader(){
        try {
            fsr.close();
            bfr.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        HdfsAction act = new HdfsAction();
        act.initHdfsFs("hdfs://192.168.122.1:9000");
        if(act.hdfsFs.exists(new Path("/output/315"))){
            System.out.println("asssssssss");
        }

    }
}
