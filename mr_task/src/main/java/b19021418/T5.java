package b19021418;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class T5 {
    private static final String fileInput = "hdfs://172.16.17.10:9000/data/B19021418/stu.csv";
    private static final String fileOutput = "hdfs://172.16.17.10:9000/data/B19021418/output/T5";

    private static class WcMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable k1, Text v1, Context context) {
            try {
                //将文件的每一行传递过来，使用split分割后利用字符数组进行接收
                String[] stu = v1.toString().split(",");
                //向Reducer传递参数-> Key：性别 Value：姓名
                context.write(new Text(stu[3]), new Text(stu[1]));
            } catch (Exception e) {
                System.out.println(v1.toString());
                e.printStackTrace();
            }
        }
    }

    private static class WcReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //创建集合来去除重复值(HashSet不允许重复值的存在，故可用来去重)
            List<String> names = new ArrayList<>();
            for (Text value : values) {
                names.add(value.toString());
            }
            HashSet<String> singleNames = new HashSet(names);
            //创建StringBuffer用来接收同性别学生的姓名
            StringBuffer sb = new StringBuffer();
            //拼接学生姓名以及统计人数
            int num = 0;
            for (String singleName : singleNames) {
                sb.append(singleName.toString()).append(",");
                num++;
            }
            //输出
            String result = "生一共有" + num + "名,他们的名字是：" + sb.toString();
            System.out.println("********************************************");
            System.out.println(key.toString() + result);
            context.write(key, new Text(result));
        }
    }

    private static class Worker {
        public void deal() {
            //设置文件输入路径
            Path fileInPath = new Path(fileInput);
            //设置文件输出路径
            Path fileOutPath = new Path(fileOutput);
            //进行配置
            Configuration conf = new Configuration();
            try {
                Job job = Job.getInstance(conf);
                job.setJarByClass(Worker.class);
                job.setMapperClass(WcMapper.class);
                job.setReducerClass(WcReducer.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                //设置文件路径
                FileInputFormat.addInputPath(job, fileInPath);
                FileOutputFormat.setOutputPath(job, fileOutPath);
                job.waitForCompletion(true);
            } catch (IOException | InterruptedException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        Worker worker = new Worker();
        worker.deal();
    }
}
