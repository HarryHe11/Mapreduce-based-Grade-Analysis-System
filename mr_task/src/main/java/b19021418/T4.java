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

public class T4 {
    private static final String fileInput = "hdfs://172.16.17.10:9000/data/B19021418/stu.csv";
    private static final String fileOutput = "hdfs://172.16.17.10:9000/data/B19021418/output/T4";

    private static class WcMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable k1, Text v1, Context context) {
            try {     //将文件的每一行传递过来，使用split分割后利用字符数组进行接收
                String[] stu = v1.toString().split(",");
                //拼接字符串：课程和成绩
                String sc = stu[0] + "\t" + stu[2];
                //向Reducer传递参数-> Key：课程+成绩 Value：学生名
                context.write(new Text(sc), new Text(stu[1]));
            } catch (Exception e) {
                System.out.println(v1.toString());
                e.printStackTrace();
            }
        }
    }

    private static class WcReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //创建StringBuffer用来接收该课程中成绩相同的学生的姓名
            StringBuffer sb = new StringBuffer();
            //num变量用来计数
            int num = 0;
            //遍历values参数，将所有的value拼接进sb，并统计学生数量
            for (Text value : values) {
                sb.append(value.toString()).append(",");
                num++;
            }
            //如果num=1，则表明该课程的这个成绩只有一个学生，否则就输出
            if (num > 1) {
                String names = "一共有" + num + "名学生,他们的名字是：" + sb.toString();
                System.out.println("*************************************************");
                System.out.println(key.toString() + names);
                context.write(key, new Text(names));
            }
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
