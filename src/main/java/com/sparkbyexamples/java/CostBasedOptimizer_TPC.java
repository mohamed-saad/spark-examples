package com.sparkbyexamples.java;

import org.apache.log4j.Level;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class CostBasedOptimizer_TPC {

    private static final String LINE = "-------------------------------------------------------------------------------------";
//    private static final String EXTENSION = "tbl";
    private static final String EXTENSION = "dat";

    private static void csvToParquet(SparkSession spark, String headersPath, String inPath, String outPath, String table, int partitionCount) {
        Dataset<Row> headers = spark.read()
                .option("delimiter", "|").option("header", "true")
                .csv(headersPath + "/" + table + ".txt");
        Dataset<Row> ds = spark.read()
                .option("delimiter", "|").option("inferSchema", "true").option("header", "true")
                .csv(inPath + "/" + table + "." + EXTENSION).toDF(headers.columns());
        if (partitionCount > 0)
            ds = ds.repartition(partitionCount);
        ds.write().mode("overwrite").parquet(outPath + "/" + table + "/");
    }

    private static void csvToParquet(SparkSession spark, String headersPath, String inPath, String outPath, List<String> tables) {
        if (outPath.startsWith("gs://") || outPath.startsWith("hdfs://") || outPath.startsWith("s3://" )) {
            System.out.println("Skip generation, output exists at remote location " + outPath);
            return;
        }
        File outDir = new File(outPath);
        if (outDir.exists()) {
            System.out.println("Skip generation, output exists at " + outDir.getAbsolutePath());
            return;
        }
        outDir.mkdirs();
        System.out.println("Generate " + tables.size() + " tables as parquet saved in " + outDir.getAbsolutePath());
        long start = System.currentTimeMillis();
        for (String table: tables) {
            csvToParquet(spark, headersPath, inPath, outPath, table, 0);
        }
        long end = System.currentTimeMillis();
        System.out.println("Generate tables completed in " + (end-start)/1000 + " seconds");
        System.out.println(LINE);
    }

    private static void createTable(SparkSession spark, String path, String table) {
        /*
        The following method copies the table locally again in the spark-warehouse folder,
        so I didn't use it and used CREATE TABLE using LOCATION directly

        spark.read().parquet(path + "/" + table).createOrReplaceTempView(table + "_vw");
        spark.sql("CREATE TABLE " + table + " USING PARQUET AS SELECT * FROM `" + table + "_vw`");
        */

        spark.sql("CREATE TABLE " + table + " USING PARQUET LOCATION '" + path + "/" + table + "'");
        Dataset<Row> ds = spark.sql("SELECT * FROM " + table + " WHERE FALSE");
        StringBuilder columns = new StringBuilder();
        String separator = "";
        // TODO we should not collect statistics over all columns but only the columns referenced in the queries
        for (StructField col : ds.schema().fields()) {
            columns.append(separator).append(col.name());
            separator = ", ";
        }
        spark.sql("ANALYZE TABLE " + table + " COMPUTE STATISTICS FOR COLUMNS " + columns);
        // spark.sql("ANALYZE TABLE US_FLIGHT COMPUTE STATISTICS NOSCAN");
    }

    private static void createTable(SparkSession spark, String path, List<String> tables) {
        System.out.println("Create " + tables.size() + " optimized tables");
        long start = System.currentTimeMillis();
        for (String table: tables) {
            createTable(spark, path, table);
        }
        long end = System.currentTimeMillis();
        System.out.println("Create optimized tables completed in " + (end-start)/1000 + " seconds");
        System.out.println(LINE);
    }

    private static void createView(SparkSession spark, String path, String table) {
        spark.read().parquet(path + "/" + table).createOrReplaceTempView(table);
    }

    private static void createView(SparkSession spark, String path, List<String> tables) {
        System.out.println("Create " + tables.size() + " views");
        long start = System.currentTimeMillis();
        for (String table: tables) {
            createView(spark, path, table);
        }
        long end = System.currentTimeMillis();
        System.out.println("Create views completed in " + (end-start)/1000 + " seconds");
        System.out.println(LINE);
    }

    private static long query(SparkSession spark, String name, String query) {
        System.out.println("Executing " + name);
        spark.sparkContext().setJobDescription(name);
        long start = System.currentTimeMillis();
        int collected = 0;

        Iterator<Row> data = spark.sql(query).toLocalIterator();
        while (true) {
            try {
                data.next();
                collected++;
            } catch (NoSuchElementException e) {
                break;
            }
        }
        long end = System.currentTimeMillis();
        long time = end - start;
        System.out.println("Executed " + name + " in " + time / 1000 + " seconds returned " + collected + " rows");
        return time;
    }

    private static void cost(SparkSession spark, String name, String query) {
        System.out.println("Estimating " + name);
        spark.sparkContext().setJobDescription(name);
        int collected = 0;

        Iterator<Row> data = spark.sql("explain cost " + query).toLocalIterator();
        while (true) {
            try {
                System.out.println(data.next());
            } catch (NoSuchElementException e) {
                break;
            }
        }
        System.out.println("\n\n" + LINE);
    }

    private static void query(SparkSession spark, File queryDir, boolean execute, int runs) throws IOException {
        if (execute) {
            System.out.println("Running queries for " + runs + " runs");
        } else {
            System.out.println("Estimating cost for queries");
            runs = 1;
        }
        Map<String, List<Long>> executionTime = new TreeMap<>();
        File[] files = queryDir.listFiles();
        Arrays.sort(files);
        for (int r=0; r<runs; r++) {
            System.out.println(LINE);
            System.out.println("Run# " + (r+1) + "/" + runs);
            for (File f : files) {
                String query = new String(Files.readAllBytes(f.toPath()));
                if (execute)
                    executionTime.computeIfAbsent(f.getName(), k -> new LinkedList<>()).add(query(spark, f.getName(), query));
                else
                    cost(spark, f.getName(), query);
            }
        }

        if (execute) {
            System.out.println(LINE);
            long totalTime = 0;
            for (Map.Entry<String, List<Long>> query : executionTime.entrySet()) {
                System.out.print(query.getKey());
                for (Long t: query.getValue()) {
                    totalTime += t;
                    System.out.print("\t" + t);
                }
                System.out.println();
            }
            System.out.println(LINE);
            System.out.println("Total Time\t" + totalTime);
        }
    }

    private static void cli(SparkSession spark) {
        Scanner myObj = new Scanner(System.in);
        while(true) {
            System.out.println(">>");
            String query = myObj.nextLine();  // Read user input
            if (query.equalsIgnoreCase("exit")) {
                System.out.println("Good bye!");
                return;
            }
            try {
                Iterator<Row> data = spark.sql(query).toLocalIterator();
                while (true) {
                    try {
                        System.out.println(data.next());
                    } catch (NoSuchElementException e) {
                        break;
                    }
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    public static void run(boolean local, boolean optimize, boolean adaptive, boolean execute, int runs,
                           String headersPath, String inputData, String outputData, String queryPath) throws IOException {
        // disable loggers
        org.apache.log4j.Logger.getLogger("org").setLevel(Level.FATAL);
        org.apache.log4j.Logger.getLogger("akka").setLevel(Level.FATAL);

        // create session
        SparkSession.Builder sessionBuilder = SparkSession.builder();

        String appName = "TPC-H";
        if (!optimize && !adaptive) appName += "_classic";
        if (optimize) appName += "_optimize";
        if (adaptive) appName += "_adaptive";

        sessionBuilder = sessionBuilder.appName(appName);

        if (local)
            sessionBuilder = sessionBuilder.master("local[*]");

        if (optimize) {
            System.out.println("Adding CBO configuration");
            sessionBuilder = sessionBuilder
                    // Enables CBO for estimation of plan statistics when set true.
                    .config("spark.sql.cbo.enabled", "true")
                    // The logical plan will fetch row counts and column statistics from catalog.
                    .config("spark.sql.cbo.planStats.enabled", "true")
                    // It enables join reordering based on star schema detection.
                    .config("spark.sql.cbo.starSchemaDetection", "true")
                    // Applies star-join filter heuristics to cost based join enumeration.
                    .config("spark.sql.cbo.joinReorder.dp.star.filter", "true")
                    // The maximum number of joined nodes allowed in the dynamic programming algorithm.
                    .config("spark.sql.cbo.joinReorder.dp.threshold", "12")
                    // Weight is a tuning parameter used in the cost plan calculations where: weight * cardinality + (1 — weight) * size
                    .config("spark.sql.cbo.joinReorder.card.weight", "0.7")
                    // Enables join reorder in CBO.
                    .config("spark.sql.cbo.joinReorder.enabled", "true")
                    // Generates histograms when computing column statistics if enabled.
                    .config("spark.sql.statistics.histogram.enabled", "true")
                    // The default number of histogram buckets
                    .config("spark.sql.statistics.histogram.numBins", "254")
                    // Fall back to HDFS if the table statistics are not available from table metadata
                    .config("spark.sql.statistics.fallBackToHdfs", "true")
                    // Number of unique values
                    .config("spark.sql.statistics.ndv.maxError", "0.05")
                    // Percentile Approximation is used for calculating the histogram bins
                    .config("spark.sql.statistics.percentile.accuracy", "10000")
                    // Enables automatic update of the table size statistic of a table after the table has changed.
                    .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
            ;
        }

        if (adaptive) {
            System.out.println("Adding Adaptive configuration");
            sessionBuilder = sessionBuilder
                // enable adaptive query execution, which re-optimizes the query plan in the middle of query execution, based on accurate runtime statistics.
                .config("spark.sql.adaptive.enabled", "true")
                // Spark tries to use local shuffle reader to read the shuffle data when the shuffle partitioning is not needed
                .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
                // Spark dynamically handles skew in sort-merge join by splitting (and replicating if needed) skewed partitions
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
                // A partition is considered as skewed if its size is larger than this factor multiplying the median partition size and also larger than 'spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes'
                .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
                // A partition is considered as skewed if its size in bytes is larger than this threshold and also larger than it
                .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB");
        }

        SparkSession spark = sessionBuilder.getOrCreate();

        // uncomment the following line to configure remote GCS bucket
//        spark.sparkContext().hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", "/workspace/keys/gcs-sys-1769.json");

        List<String> tables = Arrays.stream(new File(headersPath).list()).map(name -> name.substring(0, name.indexOf("."))).collect(Collectors.toList());

        csvToParquet(spark, headersPath, inputData, outputData, tables);

        if (optimize)
            createTable(spark, outputData, tables);
        else
            createView(spark, outputData, tables);

        File queryDir;
        if (queryPath != null && (queryDir = new File(queryPath)).exists()) {
            query(spark, queryDir, execute, runs);
        } else {
            System.out.println("Start interactive mode");
            cli(spark);
        }

        System.out.println("Application ID " + spark.sparkContext().applicationId());
    }

    private static void splitQueries() throws Throwable{
        String input = "/home/msaad/workspace/data/ds-queries/query_s100.sql";
        StringBuilder query = new StringBuilder();
        int scale = 100;
        int index = 0;
        for (String line: Files.readAllLines(Paths.get(input))) {
            query.append(line).append("\n");
            if (line.startsWith("-- end ")) {
                Files.write(Paths.get(String.format("src/main/resources/tpcds-queries-s%d/tpcds-s%d-q%02d.sql", scale, scale, ++index)), query.toString().getBytes());
                query = new StringBuilder();
            }
        }
    }

    /**
     *
     *   spark-submit --master [master] --class com.sparkbyexamples.java.CostBasedOptimizer_TPCH [jar] \
     *      [tbl-headers-path] [tbl-path] [parquet-path] [queries-path] \
     *      [local-master] \         -- "true" to use local[*] as master
     *      [use-cbo] \              -- "true" to enable cost based optimization, "false" for default rule based optimization
     *      [adaptive-plan] \        -- "true" to enable adaptive query plan, "false" for static query plan
     *      [execute] \              -- "true" for execute, "false" for estimate cost
     *      [runs]
     *
     *   Example:
     *   # run locally

        "/workspace/git/spark-examples/src/main/resources/tpch-headers" \
        "/home/msaad/workspace/data/tpch-1g-tbl" \
        "/home/msaad/workspace/data/tpch-1g-parq" \
        "/workspace/git/spark-examples/src/main/resources/tpch-queries-s1" \
        true false false true 3

     *   # run on cluster

         ~/spark-3.2.0-bin-hadoop3.2/bin/spark-submit --master spark://ip-10-0-21-77.us-east-2.compute.internal:7077 \
             --class com.sparkbyexamples.java.CostBasedOptimizer_TPCH ~/spark-examples/target/java-spark-examples-1.0-SNAPSHOT.jar \
             "~/spark-examples/src/main/resources/tpch-headers" \
             "~/data/tbl-100" \
             "~/data/parq-100" \
             "~/spark-examples/src/main/resources/tpch-queries" \
             false false false true 3
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 9) {
            System.err.println("Invalid number of arguments, expected 9 arguments while found " + args.length + " arguments");
            System.out.println(
                    "      [tbl-headers-path] [tbl-path] [parquet-path] [queries-path] \\\n" +
                    "      [local-master] \\         -- \"true\" to use local[*] as master\n" +
                    "      [use-cbo] \\              -- \"true\" to enable cost based optimization, \"false\" for default rule based optimization\n" +
                    "      [adaptive-plan] \\        -- \"true\" to enable adaptive query plan, \"false\" for static query plan\n" +
                    "      [execute] \\              -- \"true\" for execute, \"false\" for estimate cost\n" +
                    "      [runs]");
            System.exit(1);
        }

        String tblHeadersPath = args[0];
        String tblPath = args[1];
        String parquetPath = args[2];
        String queriesPath = args[3];

        boolean localMaster = Boolean.parseBoolean(args[4]);

        boolean optimize = Boolean.parseBoolean(args[5]);    // "true" to enable cost based optimization, "false" for default rule based optimization
        boolean adaptive = Boolean.parseBoolean(args[6]);    // "true" to enable adaptive query plan, "false" for static query plan

        boolean execute = Boolean.parseBoolean(args[7]);    // "true" for execute, "false" for estimate cost

        int runs = Integer.parseInt(args[8]);

        run(localMaster, optimize, adaptive, execute, runs, tblHeadersPath, tblPath, parquetPath, queriesPath);

        if (localMaster) {
            // wait
            System.out.println("Now open http://localhost:4040/SQL/execution/?id=0");
            System.out.println("Press any key to terminate the program");
            System.in.read();
        }
    }
}