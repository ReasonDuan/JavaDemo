package com.reason.io;

import com.reason.util.NumberUtil;
import org.apache.log4j.Logger;

import java.io.*;

public abstract class AbstractRead {

    private static Logger logger = Logger.getLogger(AbstractRead.class.getName());

    /**
     * 随机读取文件内容
     */
    public void test(String fileName) {
        RandomAccessFile randomFile = null;
        try {
            logger.debug("随机读取一段文件内容：");
            // 打开一个随机访问文件流，按只读方式
            randomFile = new RandomAccessFile(fileName, "r");
            // 文件长度，字节数
            long fileLength = randomFile.length();
            // 将读文件的开始位置移到beginIndex位置。
            randomFile.seek(31);
            byte[] l = new byte[8];
            byte[] t = new byte[8];
            byte[] i = new byte[4];
            randomFile.read(l);
            randomFile.read(t);
            randomFile.read(i);

            byte[] key = new byte[NumberUtil.bytes2Int(i)];
            byte[] val = new byte[(int) (NumberUtil.bytes2Long(l)-NumberUtil.bytes2Int(i)-8-8-4)];
            randomFile.read(key);
            randomFile.read(val);


            logger.debug(NumberUtil.bytes2Long(l));
            logger.debug(NumberUtil.bytes2Long(t));
            logger.debug(NumberUtil.bytes2Int(i));
            logger.debug(new String(key));
            logger.debug(new String(val));

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (randomFile != null) {
                try {
                    randomFile.close();
                } catch (IOException e1) {
                }
            }
        }
    }


    /**
     * 以行为单位读取文件，常用于读面向行的格式化文件
     */
    public void readFileByLines(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            logger.debug("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                logger.debug("line " + line + ": " + tempString);
                executeOneLine(tempString);
                line++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }

    /**
     * readFileByLines 读到的一行信息
     */
    public abstract void executeOneLine(String oneLine);

    /**
     * 随机读取文件内容
     */
    public static void readFileByRandomAccess(String fileName) {
        RandomAccessFile randomFile = null;
        try {
            logger.debug("随机读取一段文件内容：");
            // 打开一个随机访问文件流，按只读方式
            randomFile = new RandomAccessFile(fileName, "r");
            // 文件长度，字节数
            long fileLength = randomFile.length();
            // 读文件的起始位置
            int beginIndex = (fileLength > 4) ? 4 : 0;
            // 将读文件的开始位置移到beginIndex位置。
            randomFile.seek(beginIndex);
            byte[] bytes = new byte[10];
            int byteread = 0;
            // 一次读10个字节，如果文件内容不足10个字节，则读剩下的字节。
            // 将一次读取的字节数赋给byteread
            while ((byteread = randomFile.read(bytes)) != -1) {
                System.out.write(bytes, 0, byteread);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (randomFile != null) {
                try {
                    randomFile.close();
                } catch (IOException e1) {
                }
            }
        }
    }
}
