import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PutMerge{

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop-2.6.0/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-2.6.0/etc/hadoop/hdfs-site.xml"));
		FileSystem hdfs = FileSystem.get(conf);
		FileSystem local = FileSystem.getLocal(conf);
		Path inputDir = new Path(args[0]);
		Path hdfsFile = new Path(args[1]);

		try {
			FileStatus[] inputFiles = local.listStatus(inputDir);
			FSDataOutputStream out = hdfs.create(hdfsFile);
			PutMerge obj=new PutMerge();
			obj.Directory(inputFiles,local,out);
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public void Directory(FileStatus[] inputFiles,FileSystem local,FSDataOutputStream out) throws IOException
	{

		for (int i=0; i<inputFiles.length; i++) {
			System.out.println(inputFiles[i].getPath().getName());
			if(inputFiles[i].isDirectory())
			{
				FileStatus[] newFile=local.listStatus(inputFiles[i].getPath());
				PutMerge obj=new PutMerge();
				obj.Directory(newFile, local, out);		
			}

			else
			{	
				FSDataInputStream in =local.open(inputFiles[i].getPath());
				byte buffer[] = new byte[256];
				int bytesRead = 0;
				while( (bytesRead = in.read(buffer)) > 0) 
				{
					out.write(buffer, 0, bytesRead);
				}
				in.close();

			}

		}

	}
}
