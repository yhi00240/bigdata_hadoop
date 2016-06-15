package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool 
{

    public static void main(String[] args) throws Exception 
    {
        System.out.println(Arrays.toString(args));

        int res = ToolRunner.run(new Configuration(), new WordCount(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception 
    {
        System.out.println(Arrays.toString(args));

        Job job = Job.getInstance(getConf());               // 맵리듀스를 실행하기 위한 job 객체 생성
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
  
        job.setJarByClass(WordCount.class);                 // job 객체의 라이브러리를 지정
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);     // input과 output에 대한 지정
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputKeyClass(Text.class);                  // reducer 클래스의 출력데이터와 key, value의 타입 설정
        job.setOutputValueClass(String.class);

        job.waitForCompletion(true);
     
        return 0;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>   // line number, input(real data), output, output
    {
        private Text key_word = new Text();
        private final static IntWritable count = new IntWritable(1);

        public void map(LongWritable key, Text vlaue, Context context) throws IOException, InterruptedException
        {
            String[] candidate = { "CLINTON", "SANDERS", "CRUZ", "KASICH","TRUMP" };
            String[] twitid = { "HILLARYCLINTON", "BERNIESANDERS", "TEDCRUZ", "JOHNKASICH","REALDONALDTRUMP" };
            String[] namechk = { "HILLARY", "BERNIE", "TED", "JOHN","DONALD" };

            String tmpStr = value.toString();

            tmpStr = tmpStr.replaceAll("[^A-Za-z]", " ");           // 정규표현식을 이용하여 영문자를 제외한 문자를 공백으로 처리

            String[] token = tmpStr.toUpperCase().split("\\s+");    // 각 문자들을 대문자로 변환, 공백을 기준으로 스트링에 넣음

            int textidx = 0;
            int textchk = 0;
            int locateidx = 0;

            for(int i = 0; i < token.length; i++)           // 트위터에서 가져온 데이터셋에서 text부분만 가져오기위해 index값을 찾는 부분
            {
                if(token[i].equals("TEXT"))
                {
                    if(textchk == 1)
                        continue;

                    textidx = i;
                    textchk = 1;
                }

                if(token[i].equals("LOCATE"))
                {
                    locateidx = i;
                }
            }

            for(int i = textidx + 1; i < locateidx; i++)    // 대선 후보들의 이름을 하나로 통일 ex) 힐러리 클린턴
            {
                if(token[i].equals(namechk[0]))
                {
                    if(token[i+1].equals(candidate[0]))     //힐러리를 찾고 그 다음 단어를 확인하여 클린턴일 경우, 힐러리를 공백으로 하여 문장에 클린턴만 넣어줌
                        token[i] = token[i].replace(namechk[0], " ");
                    else                                    // 힐러리를 찾고 그 다음 단어를 확인했는데 클린턴이 없을 경우, 힐러리를 클린턴으로 변환하여 넣어줌

                        token[i] = token[i].replace(namechk[0], candidate[0]);
                }
                else if(token[i].equals(namechk[1]))
                {
                    if(token[i+1].equals(candidate[1]))
                        token[i] = token[i].replace(namechk[1], " ");
                    else
                        token[i] = token[i].replace(namechk[1], candidate[0]);
                }
                else if(token[i].equals(namechk[2]))
                {
                    if(token[i+1].equals(candidate[2]))
                        token[i] = token[i].replace(namechk[2], " ");
                    else
                        token[i] = token[i].replace(namechk[2], candidate[0]);
                }
                else if(token[i].equals(namechk[3]))
                {
                    if(token[i+1].equals(candidate[3]))
                        token[i] = token[i].replace(namechk[3], " ");
                    else
                        token[i] = token[i].replace(namechk[3], candidate[0]);
                }
                else if(token[i].equals(namechk[4]))
                {
                    if(token[i+1].equals(candidate[4]))
                        token[i] = token[i].replace(namechk[4], " ");
                    else
                        token[i] = token[i].replace(namechk[4], candidate[0]);
                }
                                                            
                for(int j = 0; j < twitid.length; j++)          // 만약 이름이 트위터 아이디로 되어있을 경우 클린턴으로 변환하여 넣어줌
                {
                    if(token[i].equals(twitid[j]))
                        token[i] = token[i].replace(twitid[j], candidate[j]);                                                        
                }
            }
                            
            for(int p = textidx + 1; p < locateidx; p++)                // 대선후보들의 이름이 key, 텍스트 안에서 발견하게 되면 1을 value로 해서 리듀스로 보냄
            {
                for(int i = 0; i < candidate.length; i++)
                {
                    if(token[p].equals(candidate[i]))
                    {                                    
                        key_word.set(token[p]);
                        context.write(key_word,count);
                        key_word.set("TotalNum");               // 총 대선후보들이 검색된 트윗갯수를 TotalNum에 저장하기 위해 key값으로 리듀스로 보냄
                        context.write(key_word,count);
                    }
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, String>     // 입력키 타입, 입력값 타입, 출력키 타입, 출력값 타입
    {
        HashMap<String, Integer> hashmap = new HashMap<String, Integer>();          // 대선 후보들을 key로 하여, 각각의 카운트 된 값을 value로 하기 위해 hashmap을 사용

        //@Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            String[] candidate = { "CLINTON", "SANDERS", "CRUZ", "KASICH","TRUMP" };

            int sum = 0;
            double rate = 0;
            String prt = "";

            for(IntWritable val : values)   // 각 키 값에 대해 카운트된 값을 총 더해줌
            {
                sum += val.get();
            }

            hashmap.put(key.toString(), sum);

            if(key.toString().equals("TotalNum"))
            {
                prt = sum + "";
            }
            else
            {
                prt = sum + "(KeyWord Num)";
            }

            context.write(key, new String(prt));

            if(key.toString().equals("TotalNum"))       // hashmap에서 'TotalNum'이라는 키에 총 대선후보들이 검색된 트윗갯수가 value로 지정되어있으므로, 이를 이용하여 비율로 나타냄
            {
                for(int i = 0; i < candidate.length; i++)
                {
                    rate = Math.round(((double)((Integer)hashmap.get(candidate[i])) / (double)sum) * 100);
                    prt = rate + "%(KeyWord Rate)";
                    context.write(new Text(candidate[i]), new String(prt));
                }
            }
        }
    }
}
