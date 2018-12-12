import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSFileOperate {
	private Configuration m_conf;
	private  FileSystem m_hdfs;
	HDFSFileOperate(){
		try {
			m_conf = new Configuration();
			m_conf.set("fs.defaultFS", "hdfs://localhost:9000");
			m_conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			m_hdfs = FileSystem.get(m_conf);
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}
	// judge hadoop file existed ?
	public boolean isHadoopFileExist(String fileName) {
		boolean ret = false;
		try {
            if(m_hdfs.exists(new Path(fileName))){
                System.out.println("文件存在");
                ret = true;
            }else{
                System.out.println("文件不存在");
                ret = false;
            }
		}catch(Exception e) {
			e.printStackTrace();
		}
		return ret;
	}
	
	// read hadoop file
	public void readHadoopFile(String fileName) {
		try {
			Path file = new Path(fileName); 
            FSDataInputStream getIt = m_hdfs.open(file);
            BufferedReader d = new BufferedReader(new InputStreamReader(getIt));
            String content = d.readLine(); //读取文件一行
            System.out.println(content);
            d.close(); //关闭文件		
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	// write hadoop file
	public void writeHadoopFile(String filename) {
        try {
            byte[] buff = "Hello world".getBytes(); // 要写入的内容
            FSDataOutputStream os = m_hdfs.create(new Path(filename));
			os.write(buff,0,buff.length);
	        System.out.println("Create:"+ filename);
	        os.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// close HDFS File
	public void closeHDFSFile() {
		try {
			m_hdfs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// main function
	public static void main(String[] args) {
		String fsInputFile = "/user/hadoop/input/myLocalFile.txt";
		String fsOutputFile = "/user/hadoop/input/myOutFile.txt";
		HDFSFileOperate hdfs = new HDFSFileOperate();
		if(hdfs.isHadoopFileExist(fsInputFile)) {
			hdfs.readHadoopFile(fsInputFile);
		}
		hdfs.writeHadoopFile(fsOutputFile);
		hdfs.closeHDFSFile();
	}
}
