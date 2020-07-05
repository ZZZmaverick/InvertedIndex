import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;

public class InvertedIndex
{
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> 
    {
        // 统计词频时，需要去掉标点符号等符号，此处定义表达式
        private String pattern = "[^a-zA-Z0-9-]";
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
            // 用FileSplit获取文件所属的切片信息及文件名
            FileSplit fileInfo = (FileSplit) context.getInputSplit();
            String fileName = fileInfo.getPath().getName();
            // 将每一行转化为一个String
            String line = value.toString();
            // 将标点符号等字符用空格替换，这样仅剩单词
            line = line.replaceAll(pattern, " ");
            // 将String划分为一个个的单词
            String[] words = line.split("\\s+");
            // 依次进行word、fileName和该word在fileName文件中位置的相关输出操作
            for (int i=0; i<words.length; i++)
            {
                if (words[i].length() > 0)
                {
                    String pos = "" + i;    //该词在该文件中的位置，此处用单词序数表示
                    context.write(new Text(words[i]), new Text(fileName + "," + pos));  //文件名和位置都放在value中
                }
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> 
    {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            Configuration conf = context.getConfiguration();
            // 从conf获取停止词
            String Str = conf.get("swords"); 
            String[] stopWords = Str.split(",");
            boolean tag = true;
            // 进行停止词判断
	        String k = key.toString();
            for(String tmp:stopWords)
            {
	 	        //System.out.println(tmp);
	 	        //System.out.println("key========" + k);
                if(tmp.equals(k)) 
                {
                    tag = false;
                    break;
                }
            }
            // 判断为停止词则不进行后续操作
            if(tag)
            {
                String tp;
                int count = 1;
                String temp = "";
                String strOut = "";
                // 使用hashmap储存文件名和次数的对应关系
                // 此处定义两个map，前者用来储存文件名和词频，后者用来记录文件名和单词在该文件中的出现位置
                Map<String, Integer> countMap = new HashMap<String, Integer>();
                Map<String, String> posMap = new HashMap<String, String>();
                // 进行次数相关的统计
                for (Text value : values) 
                {
                    String mesg = value.toString();
                    String[] nameAndPos = mesg.split(",");
                    count = 1;
                    if(countMap.containsKey(nameAndPos[0]))
                        count += countMap.get(nameAndPos[0]);
                    countMap.put(nameAndPos[0], count);
                    temp = nameAndPos[1];
                    if(posMap.containsKey(nameAndPos[0]))
                        temp = posMap.get(nameAndPos[0]) + "," + nameAndPos[1];
                    posMap.put(nameAndPos[0], temp);
                }
                // Map数据转为一定格式的String
                for (Map.Entry<String, Integer> entry: countMap.entrySet())
                {
                    tp = entry.getKey();
                    strOut += "<" + tp + ", " + entry.getValue() + ", [" + posMap.get(tp) + "]>; ";
                }
                // 格式处理
                strOut = strOut.substring(0, strOut.length()-2);
                // 输出操作
                //System.out.println("========" + key + "   " + strOut);
                context.write(key, new Text(strOut));
            }
        }
    }

    public static void main(String[] args) throws Exception {
            // 创建配置对象
            Configuration conf = new Configuration();
            //stop_words设定
            String swFilePath = "stop_words.txt";   //读停止词文件
            Path swPath = new Path(swFilePath);
            String sword = "";
            try (FileSystem fs = FileSystem.get(conf);
                FSDataInputStream in = fs.open(swPath);
                BufferedReader bfr = new BufferedReader(new InputStreamReader(in));) {
                String line;
                while ((line = bfr.readLine()) != null) 
                {
                    line = line.replaceAll("[^a-zA-Z0-9]", "");
                    if(line!=null)
                        sword += line + ",";               
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            //获取停止词内容到conf
            conf.set("swords", sword);

            // 创建Job对象
            Job job = Job.getInstance(conf, "wordcount");
            // 设置运行Job的类
            job.setJarByClass(InvertedIndex.class);
            // 设置Mapper类
            job.setMapperClass(InvertedIndexMapper.class);
            // 设置Reducer类
            job.setReducerClass(InvertedIndexReducer.class);
            // 设置Map输出的Key value
            job.setMapOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // 设置Reduce输出的Key value
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // 设置输入输出的路径
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            // 提交job
            boolean b = job.waitForCompletion(true);

            if(!b) {
                    System.out.println("InvertedIndex task fail!");
            }
    }
}

