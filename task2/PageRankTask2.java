import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PageRankTask2
{
    public static void main(String[] args)
    {
        if(args.length < 2)
        {
            System.err.println("Usage: PageRankTask2 <input_edges> <output_path> [iterations] [weightMode");
            System.exit(1);
        }

        String inputpath = args[0];
        String outputpath = args[1];
        
        int iterations;
        if(args.length >= 3)
        {
            iterations = Integer.parseInt(args[2]);
        }
        else
        {
            iterations = 10;
        }
        
        String weightMode;
        if (args.length >= 4) 
        {
            weightMode = args[3];
        }    
        else 
        {
            weightMode = "log";
        }
        
        final double d = 0.85; 

        SparkConf conf = new SparkConf().setAppName("Task 2 Weighted PageRank");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // ---------------- 读取文件 clean_edges.tsv, 并且计算 raw(A, B) ----------------
        // raw(A,B) = 1.0 × to次数 + 0.5 × cc次数 
        JavaRDD<String> lines = sc.textFile(inputpath);
        
        JavaPairRDD<Tuple2<String, String>, Double> rawEdgeWeights = lines
            .mapToPair(line -> {
                String[] parts = line.split("\t");
                String src = parts[0].trim();
                String dst = parts[1].trim();
                String type = parts[2].trim();
                double typeWeight;

                if("cc".equals(type))
                {
                    typeWeight = 0.5;
                }
                else
                {
                    typeWeight = 1.0;
                }
                return new Tuple2<>(new Tuple2<>(src, dst), typeWeight);
            })
            .filter(edge -> !edge._1._1.equals(edge._1._2))
            .reduceByKey((a,b) -> a + b);

        // --------------- 对 raw(A, B) 做数据平滑，得到最终的边权重 w(A, B) -----------------
        // w(A,B) = 1 + log(1 + raw(A,B))

        // JavaPairRDD<Tuple2<String, String>, Double> edgeWeights = rawEdgeWeights
        //     .mapValues(raw -> 1.0 + Math.log(1.0 + raw))
        //     .cache();
        
        // 根据 weightMode 决定要不要 log 平滑
        JavaPairRDD<Tuple2<String, String>, Double> edgeWeights;
        if ("freq".equals(weightMode)) 
        {
            edgeWeights = rawEdgeWeights;
        } 
        else 
        {
            edgeWeights = rawEdgeWeights.mapValues(w -> 1.0 + Math.log(1.0 + w));
        }
        edgeWeights = edgeWeights.cache();

        // ----------------------------- 获取所有的节点 nodes -----------------------------
        JavaRDD<String> nodes = edgeWeights
            .flatMap(element -> Arrays.asList(element._1._1, element._1._2).iterator())
            .distinct()
            .cache();
        long nodeCount = nodes.count();

        // --------------------------- 构建加权邻接表 ------------------------------------
        JavaPairRDD<String, Iterable<Tuple2<String, Double>>> links = edgeWeights
            .mapToPair(element -> {
                String src = element._1._1;
                String dst = element._1._2;
                double weight = element._2;
                
                return new Tuple2<>(src, new Tuple2<>(dst, weight));
            })
            .groupByKey()
            .cache();
        
        // -------------------------- 标记有出边的节点，用于识别 Dangling Node ----------------
        JavaPairRDD<String, Integer> hasOutLinks = links.keys()
            .mapToPair(node -> new Tuple2<>(node, 1))
            .cache();
            
        // -------------------------- 初始化 PageRank --------------------------------------
        JavaPairRDD<String, Double> ranks = nodes
            .mapToPair(node -> new Tuple2<>(node, 1.0 / nodeCount))
            .cache();
        ranks.count(); // 这个操作是为了可以让 ranks 里面立即生效，使得  ranks 可以先被缓存起来

        final double baseScore = (1.0 - d) / nodeCount;

        for(int i = 0; i < iterations; ++i)
        {
            // ---------------------- 计算没有出边的 rank 总和 ------------------------------
            double danglingMass = ranks
                .subtractByKey(hasOutLinks)
                .values()
                .fold(0.0, (a,b) -> a + b);

            final double danglingShare = danglingMass / nodeCount;

            // ---------------------- 计算每一个节点向外传递的 PageRank 贡献 ------------------
            JavaPairRDD<String, Double> contributions = links
                .join(ranks)
                .flatMapToPair(element -> {
                    Iterable<Tuple2<String, Double>> outLinks = element._2._1;
                    double rank = element._2._2;

                    List<Tuple2<String, Double>> outList = new ArrayList<>();
                    double totalWeight = 0.0;

                    for(Tuple2<String, Double> link : outLinks)
                    {
                        String receiver = link._1;
                        double weight = link._2;

                        if(weight > 0)
                        {
                            outList.add(new Tuple2<>(receiver, weight));
                            totalWeight += weight;
                        }
                    }

                    List<Tuple2<String, Double>> result = new ArrayList<>();
                    if(totalWeight > 0)
                    {
                        for(Tuple2<String, Double> link : outList)
                        {
                            String receiver = link._1;
                            double weight = link._2;

                            double contribution = rank * (weight / totalWeight);
                            result.add(new Tuple2<>(receiver, contribution));
                        }
                    }
                    return result.iterator();
                })
                .reduceByKey((a, b) -> a + b);

            // -------------------------- 更新每一个节点的 PageRank -------------------------
            JavaPairRDD<String, Double> oldranks = ranks;
            ranks = nodes
                    .mapToPair(node -> new Tuple2<>(node, baseScore + d * danglingShare))
                    .leftOuterJoin(contributions)
                    .mapToPair(item -> {
                        String currentNode = item._1;
                        Double baseRankPart = item._2._1;
                        Optional<Double> receivedContribution = item._2._2;

                        double contribution = 0.0;
                        if(receivedContribution.isPresent())
                        {
                            contribution = receivedContribution.get();
                        }
                        else
                        {
                            contribution = 0.0;
                        }
                        double updatedPageRank = baseRankPart + d * contribution;
                        return new Tuple2<>(currentNode, updatedPageRank);
                    })
                    .cache();
            
            ranks.count();
            oldranks.unpersist();
            System.out.println("Iteration " + (i + 1) + " finished.");
        }

        // -------------------------- 输出 Top 20 影响力人物 -----------------------------
        List<Tuple2<Double, String>> top20 = ranks
                .mapToPair(item -> new Tuple2<>(item._2, item._1)) // (人,分值)→(分值,人)
                .sortByKey(false) // 降序
                .take(20);

        System.out.println("===== Top 20 影响力人物（加权）=====");
        List<String> outputLines = new ArrayList<>();
        int rankNo = 1;
        for(Tuple2<Double, String> element : top20)
        {
            outputLines.add(rankNo + "\t" + element._2 + "\t" + element._1);
            rankNo++;
        }

        // 验证：PR 总和应 ≈ 1.0
        double sum = ranks.values().fold(0.0, (a, b) -> a + b);
        System.out.println("PR 总和 = " + sum);

        sc.parallelize(outputLines, 1).saveAsTextFile(outputpath);
        System.out.println("Task2 finished. Output path: " + outputpath);
        sc.stop();

    }
}