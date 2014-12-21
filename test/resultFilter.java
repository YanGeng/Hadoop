package test;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class resultFilter {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		// 以下两句中, hdfs和local分别对应hdfs实例和本地文件系统实例
		FileSystem hdfs = FileSystem.get(conf);
		FileSystem local = FileSystem.getLocal(conf);
		
		Path inputDir, localFile;
		FileStatus[] inputFiles;
		FSDataOutputStream out = null;
		FSDataInputStream in = null;
		Scanner scan;
		String str;
		byte[] buf;
		int singleFileLines;
		int numLines, numFiles, i;
		
		if (args.length != 4) {
			// 输入参数数量不够，提示参数格式后终止程序执行
			System.err.println("usage resultFilter <dfs pth><local path" + " <match str><single file lines");
			return;
		}
		
		inputDir = new Path(args[0]);
		singleFileLines = Integer.parseInt(args[3]);
		
		try {
			inputFiles = hdfs.listStatus(inputDir);
			numLines = 0;
			numFiles = 1;
			localFile = new Path(args[1]);
			if (local.exists(localFile))
				local.delete(localFile, true);
			
			for (i = 0; i < inputFiles.length; i++) {
				if (inputFiles[i].isDir())
					continue;
				System.out.println(inputFiles[i].getPath().getName());
				in = hdfs.open(inputFiles[i].getPath());
				scan = new Scanner(in);
				while (scan.hasNext()) {
					str = scan.nextLine();
					if (str.indexOf(args[2]) == -1)
						continue;
					numLines++;
					if (numLines == 1) {
						localFile = new Path(args[1] + File.separator + numFiles);
						out = local.create(localFile);
						numFiles++;
					}
					buf = (str + "\n").getBytes();
					out.write(buf, 0, buf.length);
					if (numLines == singleFileLines) {
						out.close();
						numLines = 0;
					}
				}
				scan.close();
				in.close();
				if (out != null) {
					out.close();
				}
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

}
