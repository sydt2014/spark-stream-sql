# 分治思想:从ForkJoin到MapReduce

大数据的海量数据需要成千上万台计算运算，将计算分配到每台计算机上面，其实是分治的思想。关于分治的思想，Java里面的实现是ForkJoin。ForkJoin是开启一个线程处理当前任务，如果任务计算量太大，则将任务分给两个子线程计算。如果子线程感觉计算量依旧很大，继续分给两个子线程计算，以此类推。

![](f:/ForkJoin.png)

最后，将计算结果归并返回。这种计算方式与mapReduce不谋而合，都是分治的思想，将任务分解成许多小的任务分发到不同的机器上面，最后也将计算结果合并返回。不同的是，ForkJoin是在本地机器上面开启多个线程，而mapreduce是在不同的集群里的多台机器上面开启多个进程，在进程里面启动多个线程而已。涉及到集群时，就要考虑到网络，序列化等相对本地计算而言，不得不做的一些额外的传输工作，这也是大数据计算所尽量避免的。宏观层面上，整个集群可以看多一台计算机，例如超级计算机，从这个角度讲，两者区别不大。

假设我们计算1+2+3+4+5+6+7+8+9+10

那我们可以这样来做

```
public class CountTaskextendsRecursiveTask {

       private static final int THRESHOLD= 3;//阈值

       private int start;

       private int end;

       public CountTask(intstart,intend) {
                   this.start= start;
                   this.end= end;
        }

       @Override
       protected Integer compute() {

                   int sum = 0;

                   //如果任务足够小就计算任务
                   boolean canCompute = (end-start) <=THRESHOLD;
                   if(canCompute) {
                       for(inti =start; i <=end; i++) {
                         sum += i;
                       }
                    }else{
                             //如果任务大于阀值，就分裂成两个子任务计算
                              int middle = (start+end) / 2;
                              CountTask leftTask =newCountTask(start, middle);
                              CountTask rightTask =newCountTask(middle + 1,end);

                              //执行子任务
                              leftTask.fork();
                              rightTask.fork();

                              //等待子任务执行完，并得到其结果
                              intleftResult=leftTask.join();
                              intrightResult=rightTask.join();
                              
                              //合并子任务
                              sum = leftResult  + rightResult;
                    }

                   return sum;

        }

       public static void main(String[] args) {

                    ForkJoinPool forkJoinPool =newForkJoinPool();

                   //生成一个计算任务，负责计算1+2+3+4...+10

                    CountTask task =newCountTask(1, 10);

                   //执行一个任务

                    Future result = forkJoinPool.submit(task);

                   try{

                       System.out.println(result.get());

                    }catch(InterruptedException e) {

                    }catch(ExecutionException e) {

                    }

        }

}
```

整个计算过程如下：

![](f:/ForkJoin2.png)

从我们熟悉的wordCount开始入手，将一下ForkJoin。假设一个文件目录下面有需要文本文件，现在统计所有的文本的单词的出现次数。

ForkJoin实现的代码如下：

map阶段

```
package com.fork;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RecursiveTask;

/**
 * RecursiveTask 表示有返回值的计算
 */
public class FileRecursiveTask extends RecursiveTask<Map<String, Integer>> {

	private static final long serialVersionUID = 1L;
	private final List<String> contents;
	
	public FileRecursiveTask(File file) {
		System.out.println("处理文件：" + file.getAbsolutePath());
		try {
			contents = Files.readAllLines(Paths.get(file.toURI()));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * 业务逻辑处理，相当于map
	 */
	protected Map<String, Integer> compute() {
		Map<String, Integer> map = new HashMap<>();
		for(String content : contents){
			String[] strs = content.split(" ");
			for(String str : strs){
				if(map.containsKey(str)){
					int val = map.get(str);
					map.put(str, val+1);
				} else {
					map.put(str, 1);
				}
			}
		}
		return map;
	}
}

```

reduce阶段

```
package com.fork;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

/**
 * 每个文件会使用一个线程去处理，最终汇总到一起
 */
public class ForkRecursiveTask extends RecursiveTask<Map<String, Integer>> {

	private static final long serialVersionUID = 1L;
	private final File[] files;
	
	public ForkRecursiveTask(String path) {
		files = new File(path).listFiles();
	}
	
	/**
	 * 汇总处理，相当于reduce
	 */
	protected Map<String, Integer> compute() {
		List<ForkJoinTask<Map<String, Integer>>> tasks = new ArrayList<>();
		for(File file : files){
			FileRecursiveTask frt = new FileRecursiveTask(file); //读入文件，map处理，有几个文件，就启动几个task
			tasks.add(frt.fork());
		}		
		Map<String, Integer> result = new HashMap<>();
		for(ForkJoinTask<Map<String, Integer>> task : tasks){
			Map<String, Integer> map = task.join();  //读取每个map的处理结果
			for(String key : map.keySet()){ 		//处理结果合并
				if(result.containsKey(key)){
					result.put(key, result.get(key) + map.get(key));
				} else {
					result.put(key, map.get(key));
				}
			}
		}
		return result; 
	}
}
```

main函数

```
package com.fork;

import java.util.Map;
import java.util.concurrent.ForkJoinPool;

public class ForkJoinPoolTest {
	public static void main(String[] args) {
		ForkJoinPool forkJoinPool = new ForkJoinPool();
		//输入文件目录
		Map<String, Integer> map = forkJoinPool.invoke(new ForkRecursiveTask("e:/txts"));
		//输出最终所有的计算结果
		for(String key : map.keySet()){
			System.out.println(key + "=" + map.get(key));
		}
	}
}
```

采用mapreduce的框架实现代码如下：

```
public class WordCount {  
  
 public static class TokenizerMapper   
      extends Mapper<Object, Text, Text, IntWritable>{  
      
   private final static IntWritable one = new IntWritable(1);  
   private Text word = new Text();  
        
   public void map(Object key, Text value, Context context  
                   ) throws IOException, InterruptedException {  
     StringTokenizer itr = new StringTokenizer(value.toString());  
     while (itr.hasMoreTokens()) {  
       word.set(itr.nextToken());  
       context.write(word, one);   //每个单词变成（word，1）的格式
     }  
   }  
 }  
    
 public static class IntSumReducer   
      extends Reducer<Text,IntWritable,Text,IntWritable> {  
   private IntWritable result = new IntWritable();  
  
   public void reduce(Text key, Iterable<IntWritable> values,   
                      Context context  
                      ) throws IOException, InterruptedException {  
     int sum = 0;  
     for (IntWritable val : values) {  
       sum += val.get();    
     }  
     result.set(sum);    //统计单词出现的次数
     context.write(key, result);    //（word，出现的次数）
   }  
 }  
  
 public static void main(String[] args) throws Exception {  
   Configuration conf = new Configuration();  
   String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  
   if (otherArgs.length != 2) {  
     System.err.println("Usage: wordcount <in> <out>");  
     System.exit(2);  
   }  
   Job job = new Job(conf, "word count");  
   job.setJarByClass(WordCount.class);  
   job.setMapperClass(TokenizerMapper.class);  
   job.setCombinerClass(IntSumReducer.class);  
   job.setReducerClass(IntSumReducer.class);  
   job.setOutputKeyClass(Text.class);  
   job.setOutputValueClass(IntWritable.class);  
   FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  //输入文件所在的目录
   FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); //结果输出的目录
   System.exit(job.waitForCompletion(true) ? 0 : 1);  
 }  
```

可以看到，真正的计算处理部分只有几行代码，将整个程序运行起来所做的准备工作比计算的工作还要多，这也是分布式计算处理的特点：将处理框架启动起来比数据计算本身还难。关于分布式计算，可以参考团队协作。一件工作，一个人做需要一天，实际上没有多少工作量。交个十个人组成的团队来做，可能需要一个星期，甚至更多，因此分解任务需要一天，分配任务需要一天，其他的协调等工作需要更长的时间。由此可见，分治思想的难点在于分和治。

ForkJoin是在本地运行，免去了网络传输，hdfs的等一系列配置信息，直接输入一个目录就可以了。细想一下，如果e:/txts目录的某个文件特别的大，就是数据倾斜现象，那样处理这个文件的FileRecursiveTask会耗时特别长，其他的任务耗时较短，而且FileRecursiveTask是以文件为单位处理，显然不符合我们的预期。

假设有三个文件，第一个文件有1000行，第二个文件有2000行，第三个文件有3000行，启动三个线程。那么我们希望每个线程能够处理(1000+2000+3000)/3=2000的数据。这一点，在hadoop的FileInputFormat里面的getSplits方法已经实现了。

```
 /** Splits files returned by {@link #listStatus(JobConf)} when
   * they're too big.*/ 
  public InputSplit[] getSplits(JobConf job, int numSplits)
    throws IOException {
    StopWatch sw = new StopWatch().start();
    FileStatus[] files = listStatus(job);
    
    // Save the number of input files for metrics/loadgen
    job.setLong(NUM_INPUT_FILES, files.length);
    long totalSize = 0;                           // compute total size
    for (FileStatus file: files) {                // check we have valid files
      if (file.isDirectory()) {
        throw new IOException("Not a file: "+ file.getPath());
      }
      totalSize += file.getLen();  //文件的总的大小
    }

    long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
    long minSize = Math.max(job.getLong(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MINSIZE, 1), minSplitSize);

    // generate splits
    ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
    NetworkTopology clusterMap = new NetworkTopology();
    for (FileStatus file: files) {
      Path path = file.getPath();
      long length = file.getLen();
      if (length != 0) {
        FileSystem fs = path.getFileSystem(job);
        BlockLocation[] blkLocations;
        if (file instanceof LocatedFileStatus) {
          blkLocations = ((LocatedFileStatus) file).getBlockLocations();
        } else {
          blkLocations = fs.getFileBlockLocations(file, 0, length);
        }
        if (isSplitable(fs, path)) {
          long blockSize = file.getBlockSize();
          long splitSize = computeSplitSize(goalSize, minSize, blockSize);

          long bytesRemaining = length;
          while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
            String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,
                length-bytesRemaining, splitSize, clusterMap);
            splits.add(makeSplit(path, length-bytesRemaining, splitSize,
                splitHosts[0], splitHosts[1]));
            bytesRemaining -= splitSize;
          }

          if (bytesRemaining != 0) {
            String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, length
                - bytesRemaining, bytesRemaining, clusterMap);
            splits.add(makeSplit(path, length - bytesRemaining, bytesRemaining,
                splitHosts[0], splitHosts[1]));
          }
        } else {
          if (LOG.isDebugEnabled()) {
            // Log only if the file is big enough to be splitted
            if (length > Math.min(file.getBlockSize(), minSize)) {
              LOG.debug("File is not splittable so no parallelization "
                  + "is possible: " + file.getPath());
            }
          }
          String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,0,length,clusterMap);
          splits.add(makeSplit(path, 0, length, splitHosts[0], splitHosts[1]));
        }
      } else { 
        //Create empty hosts array for zero length files
        splits.add(makeSplit(path, 0, length, new String[0]));
      }
    }
    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Total # of splits generated by getSplits: " + splits.size()
          + ", TimeTaken: " + sw.now(TimeUnit.MILLISECONDS));
    }
    return splits.toArray(new FileSplit[splits.size()]);
  }

  protected long computeSplitSize(long goalSize, long minSize, long blockSize) {
    return Math.max(minSize, Math.min(goalSize, blockSize));
  }
```

getSplit帮助我们将要处理的文件已经逻辑上分割，

```
public FileSplit(Path file, long start, long length, String[] hosts,
     String[] inMemoryHosts) {
   fs = new org.apache.hadoop.mapreduce.lib.input.FileSplit(file, start,
          length, hosts, inMemoryHosts);
 }
```

生成FileSplit，file决定要处理哪个文件，start，length决定每个task的从哪个位置（从哪一行）开始处理数据，这部分数据有多大，host表明这部分数据实际存储在哪些机器上面。

有了这些信息，在分配处理任务的时候，安装计算本地原则，数据在哪台机器上，就让哪台机器进行处理，因为数据是在本地，不需要网络传输了。



