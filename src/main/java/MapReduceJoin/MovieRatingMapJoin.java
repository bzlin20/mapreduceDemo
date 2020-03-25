package MapReduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MovieRatingMapJoin {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.deafultFS","qyl01:9000");
        System.setProperty("HADOOP_USER_NAME","qyl");
        Job job = Job.getInstance(conf);

        job.setJar("/home/qyl/mrmr.jar");

        job.setMapperClass(MovieRatingMapper.class);
        job.setMapOutputKeyClass(MovieRate.class);
        job.setMapOutputValueClass(NullWritable.class);

       job.setNumReduceTasks(0);  //因为不需要reduce来进行处理，所有设置为0

        String minInput = args[0];
        String maxInput = args[1];
        String output = args[2];

        FileInputFormat.setInputPaths(job, new Path(maxInput));
        Path outputPath = new Path(output);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        URI uri = new Path(minInput).toUri();
        job.addCacheFile(uri);
        boolean status = job.waitForCompletion(true);
        System.exit(status?0:1);

    }

    static class MovieRatingMapper extends Mapper<LongWritable, Text,MovieRate, NullWritable>{
        //用来存储小份数据的所有解析出来的key-value
        private static Map<String,Movie> movieMap = new HashMap<String,Movie>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] localCacheFilePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());  //获取文件地址
            String myfilePath = localCacheFilePaths[0].toString();
            System.out.println(myfilePath);
            URI[] cacheFiles =context.getCacheFiles();
            System.out.println(cacheFiles.toString());

            BufferedReader br = new BufferedReader(new FileReader(myfilePath.toString()));
            //此处的Line就是从文件当中逐行读取到的movie
            String line = "";
            while(null !=(line =br.readLine())){
                String[] split = line.split("::");   //切分一行数据
               movieMap.put(split[0],new Movie(split[0],split[1],split[2]));
            }
            IOUtils.closeStream(br);

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("::");
            String userid = splits[0];
            String movieid = splits[1];
            int rate = Integer.parseInt(splits[2]);
            long ts = Long.parseLong(splits[3]);
            String movieName = movieMap.get(movieid).getMovieName();
            String movieType = movieMap.get(movieid).getMoiveType();
            MovieRate mr = new MovieRate(movieid,userid,rate,movieName,movieType,ts);
            context.write(mr,NullWritable.get());
        }


    }

}


