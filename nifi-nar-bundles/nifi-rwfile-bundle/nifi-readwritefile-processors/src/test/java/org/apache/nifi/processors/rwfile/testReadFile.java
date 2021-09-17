package org.apache.nifi.processors.rwfile;

import org.junit.Test;

import java.io.*;

public class testReadFile {
    @Test
    public void testFileReader()throws Exception{
        String path= "/Users/every/eclipse-workspace/oiue_release/uploadfile/csv/8b8aa130-05a6-42c2-ab2f-ee87ced1bc59/2_城市面数据.csv";
        File file = new File(path);
        FileReader fileReader = null;
        fileReader = new FileReader(file);
        BufferedReader in = new BufferedReader(fileReader);
        String line = null;
        int index=0;
        while ((line = in.readLine()) != null) {

            System.out.println("this line("+index+")：" + line);
            index++;
            line = null;
        }
        in.close();
        fileReader.close();
    }

    @Test
    public void testBufferedReader()throws Exception{
        String path= "/Users/every/eclipse-workspace/oiue_release/uploadfile/csv/8b8aa130-05a6-42c2-ab2f-ee87ced1bc59/2_城市面数据.csv";
        String fileEncoding = "GBK";
        File file = new File(path);
        FileInputStream fis = new FileInputStream(file);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis, fileEncoding));
        String line;
        int index=0;
        while ((line=br.readLine())!=null){
            System.out.println("this line("+index+")：" + line);
            index++;
        }
        br.close();
    }

}
