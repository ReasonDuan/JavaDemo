package com.reason.io;

import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;

public class WriteFileUtil {


    private String fileName;

    public static WriteFileUtil build(String fileName){
        return new WriteFileUtil(fileName);
    }

    public WriteFileUtil(String fileName){
        this.fileName=fileName;
    }


    /**
     * 追加文件：使用FileWriter
     *
     * @param content
     */
    public void fileWriter(String content) {
        FileWriter writer = null;
        try {
            // 打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
            writer = new FileWriter(fileName, true);
            writer.write(content);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(writer != null){
                    writer.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 追加文件：使用RandomAccessFile
     *
     * @param content 追加的内容
     */
    public void accessFile(String content) {
        RandomAccessFile randomFile = null;
        try {
            // 打开一个随机访问文件流，按读写方式
            randomFile = new RandomAccessFile(fileName, "rw");
            // 文件长度，字节数
            long fileLength = randomFile.length();
            // 将写文件指针移到文件尾。
            randomFile.seek(fileLength);
            randomFile.writeBytes(content);
            randomFile.write("\r#####\r".getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        } finally{
            if(randomFile != null){
                try {
                    randomFile.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
