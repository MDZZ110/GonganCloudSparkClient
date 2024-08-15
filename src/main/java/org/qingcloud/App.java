package org.qingcloud;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import paas.computation.memoryComputation.*;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    static String className = "paas.computation.memoryComputation.MemoryComputationImpl";

    public static void main( String[] args ) throws Exception {
        String token = "INHUOPYHWNTDDMJVSWHN|BRXHHZqw07uhwhq3wDIjE09YnuywLW1QEBP9uVgM";

        token = "";
//        tryToParseJson();
//        return;
        testCollect("gab4", token);

        testCount(token);

        testTake(token);
        testCollect("take-gab1", token);

//
//        testSaveFile(token);
//
//        testMap(token);
//        testCollect("map-gab1", token);
//
//        testFilter(token);
//        testCollect("filter-gab1", token);
//
//        testSample(token);
//        testCollect("sample-gab1", token);
//
//        testUnion(token);
//        testCollect("union-gab1-gab2", token);
//
//        testIntersection(token);
//        testCollect("intersection-gab1-gab2", token);
//
//        testDistinct(token);
//        testCollect("distinct-gab1", token);
//
//        testGroupByKey(token);
//        testCollect("groupByKey-gab3", token);
//
//        testReduceByKey(token);
//        testCollect("reduceByKey-gab3", token);
//
//        testSortByKey(token);
//        testCollect("sortByKey-gab3", token);
//
//        testJoin(token);
//        testCollect("join-gab3-gab4", token);

        testPartition(token);
        testCollect("partition-gab3", token);

//        testActionEntry(token);
//
//
//        testTransformationEntry(token);
    }

    public static void tryToParseJson()  {
        String tmpJson = "[1, 2, 3]";
        ObjectMapper mapper = new ObjectMapper();
        List<String> stringList;
        List<Integer> intList;
        try {

            stringList = mapper.readValue(tmpJson, List.class);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("解析为 String 失败！");
            return;
        }

        try {

            intList = mapper.readValue(tmpJson, List.class);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("解析为 Int 失败！");
            return;
        }

        System.out.println("都可以解析！！");
    }

    public static void printFailed(String message) {
        System.out.println("==========> " + message  + " <==========");
    }

    public static Boolean testCollect(String dataName, String token) {
        try {
            // 获取目标类的Class对象
            Class<?> clazz = Class.forName(className);

            // 创建目标类的实例
            Object instance = clazz.newInstance();

            Method method = clazz.getMethod("collect", Object.class, String.class);

            CollectResponse resp = (CollectResponse) method.invoke(instance, dataName, token);
            if ((resp.getErrorCode() == 0) && (!resp.getResult().isEmpty())) {
                printFailed("testCollect Successfully!");
                return true;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        printFailed("testCollect failed");
        return false;
    }

    public static Boolean testCount(String token)  {
        CountResponse resp = new MemoryComputationImpl().count("gab1", token);
        if (resp.getErrorCode() == 0) {
            printFailed("testCount Successfully!");
            return true;
        }

        printFailed("testCount failed");
        return false;
    }

    public static Boolean testTake(String token)  {
        TakeResponse resp = new MemoryComputationImpl().take("gab1", 3, token);
        if (resp.getErrorCode() == 0) {
            printFailed("testTake Successfully!");
            return true;
        }

        printFailed("testTake failed");
        return false;
    }

    public static Boolean testSaveFile(String token)  {
//        List<String> fileTypeList = Arrays.asList("txt", "csv", "json", "parquet", "orc");
        List<String> fileTypeList = Arrays.asList("orc");
        String localFileDir = "file:///tmp/memcompute";
        String remoteFileDir = "dfs:///hadoop/memcompute";
        SaveFileResponse resp;
        Boolean pass = true;
        for(String fileTypeName:fileTypeList){
            String localFilePath = localFileDir + "/test_save_file_" + fileTypeName;
            String remoteFilePath = remoteFileDir + "/test_save_file_" + fileTypeName;

            resp = new MemoryComputationImpl().saveFile("gab4", fileTypeName, localFilePath, token);
            if (resp.getErrorCode() != 0) {
                pass = false;
                break;
            }

            resp = new MemoryComputationImpl().saveFile("gab4", fileTypeName, remoteFilePath, token);
            if (resp.getErrorCode() != 0) {
                pass = false;
                break;
            }
        }

        if (!pass) {
            printFailed("testSaveFile failed");
        } else {
            printFailed("testSaveFile Successfully!");
        }

        return pass;
    }

    public static Boolean testMap(String token) {
        String userDefinedFunction = "com.qingcloud.MapObject.udfMap";
        MapResponse resp = new MemoryComputationImpl().map("gab1", userDefinedFunction, token);
        if (resp.getErrorCode() == 0) {
            printFailed("testMap Successfully");
            return true;
        }

        printFailed("testMap failed");
        return false;
    }

    public static Boolean testFilter(String token) {
        String userDefinedFunction = "com.qingcloud.MapObject.udfFilter";
        FilterResponse resp = new MemoryComputationImpl().filter("gab1", userDefinedFunction, token);
        if (resp.getErrorCode() == 0) {
            printFailed("testFilter Successfully");
            return true;
        }

        printFailed("testFilter failed");
        return false;
    }

    public static Boolean testSample(String token) {
        String replace = "1";
        Float percentage = 0.32f;
        int randomSeed = 100023;
        SampleResponse resp = new MemoryComputationImpl().sample("gab1", replace, percentage, randomSeed, token);
        if (resp.getErrorCode() == 0) {
            printFailed("testSample Successfully");
            return true;
        }

        printFailed("testSample failed");
        return false;
    }

    public static Boolean testUnion(String token) {

        UnionResponse resp = new MemoryComputationImpl().union("gab1", "gab2", token);
        if ((resp.getErrorCode() == 0) && (resp.getDistributedDataset().equals("union-gab1-gab2"))){
            printFailed("testUnion Successfully");
            return true;
        }

        printFailed("testUnion failed");
        return false;
    }

    public static Boolean testIntersection(String token) {

        IntersectionResponse resp = new MemoryComputationImpl().intersection("gab1", "gab2", token);
        if ((resp.getErrorCode() == 0) && (resp.getDistributedDataset().equals("intersection-gab1-gab2"))) {
            printFailed("testIntersection Successfully");
            return true;
        }

        printFailed("testIntersection failed");
        return false;
    }

    public static Boolean testDistinct(String token) {
        DistinctResponse resp = new MemoryComputationImpl().distinct("gab1", token);
        if (resp.getErrorCode() == 0) {
            printFailed("testDistinct Successfully");
            return true;
        }

        printFailed("testDistinct failed");
        return false;
    }

    public static Boolean testGroupByKey(String token) {
        GroupByKeyResponse resp = new MemoryComputationImpl().groupByKey("gab3", token);
        if (resp.getErrorCode() == 0) {
            printFailed("testGroupByKey Successfully");
            return true;
        }

        printFailed("testGroupByKey failed");
        return false;

    }

    public static Boolean testReduceByKey(String token) {
        ReduceByKeyResponse resp = new MemoryComputationImpl().reduceByKey("gab3", "udf.Udf.udfReduce", "A", token);
        if (resp.getErrorCode() == 0) {
            printFailed("testReduceByKey Successfully");
            return true;
        }

        printFailed("testReduceByKey failed");
        return false;
    }

    public static Boolean testSortByKey(String token) {
        SortByKeyResponse resp = new MemoryComputationImpl().sortByKey("gab3", "0", "A", token);
        if (resp.getErrorCode() == 0) {
            printFailed("testSortByKey Successfully");
            return true;
        }

        printFailed("testSortByKey failed");
        return false;
    }

    public static Boolean testJoin(String token) {
        JoinResponse resp = new MemoryComputationImpl().join("gab3", "gab4", 0, token);
        if (resp.getErrorCode() == 0) {
            printFailed("testJoin Successfully");
            return true;
        }

        printFailed("testJoin failed");
        return false;
    }

    public static Boolean testPartition(String token) {
        PartitionResponse resp = new MemoryComputationImpl().partition("gab3", 2, token);
        if (resp.getErrorCode() == 0) {
            printFailed("testPartition Successfully");
            return true;
        }

        printFailed("testPartition failed");
        return false;
    }

    public static Boolean testActionEntry(String token) {
        ActionEntryResponse resp = new MemoryComputationImpl().actionEntry("gab1", 1, "", token);
        if (resp.getErrorCode() != 0) {
            printFailed("testActionEntry Collect failed");
            return false;
        }

        printFailed("testActionEntry Collect Successfully!");

        resp = new MemoryComputationImpl().actionEntry("gab1", 2, "", token);
        if (resp.getErrorCode() != 0) {
            printFailed("testActionEntry Count failed");
            return false;
        }

        printFailed("testActionEntry Count Successfully!");

        String takeParameter = "{\"amount\":3}";
        resp = new MemoryComputationImpl().actionEntry("gab1", 3, takeParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testActionEntry Take failed");
            return false;
        }

        printFailed("testActionEntry Take Successfully!");

        List<String> fileTypeList = Arrays.asList("txt", "csv", "json", "parquet", "orc");
        String localFileDir = "file:///tmp/actionEntry";
        String remoteFileDir = "dfs:///hadoop/actionEntry";
        for(String fileTypeName:fileTypeList){
            String localFilePath = localFileDir + "/test_save_file_" + fileTypeName;
            String remoteFilePath = remoteFileDir + "/test_save_file_" + fileTypeName;

            String localSaveFileParameter = String.format("{\"fileType\": \"%s\", \"filePath\": \"%s\"}", fileTypeName, localFilePath);
            resp = new MemoryComputationImpl().actionEntry("gab3", 4, localSaveFileParameter, token);
            if (resp.getErrorCode() != 0) {
                printFailed("testActionEntry saveFile local failed");
                return false;
            }

            printFailed("testActionEntry saveFile " + fileTypeName + " to local Successfully");

            String remoteSaveFileParameter = String.format("{\"fileType\": \"%s\", \"filePath\": \"%s\"}", fileTypeName, remoteFilePath);;
            resp = new MemoryComputationImpl().actionEntry("gab3", 4, remoteSaveFileParameter, token);
            if (resp.getErrorCode() != 0) {
                printFailed("testActionEntry saveFile local failed");
                return false;
            }

            printFailed("testActionEntry saveFile " + fileTypeName + " to remote HDFS Successfully");
        }

        return true;

    }

    public static String mapToJson(HashMap map) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(map);
    }

    public static Boolean testTransformationEntry(String token) {
        String mapParameter = "{\"userDefinedFunction\":\"com.qingcloud.MapObject.udfMap\"}";
        TransformatEntryResponse resp = new MemoryComputationImpl().transformationEntry("gab1", "map", mapParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry Map failed");
            return false;
        }

        printFailed("testTransformationEntry Map Successfully!");


        String filterParameter = "{\"userDefinedFunction\":\"com.qingcloud.MapObject.udfFilter\"}";
        resp = new MemoryComputationImpl().transformationEntry("gab1", "filter", filterParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry Filter failed");
            return false;
        }
        printFailed("testTransformationEntry Filter Successfully");

        String replace = "1";
        Double percentage = 0.32;
        Long randomSeed = 100023L;

        String sampleParameter = String.format("{\"replace\":\"%s\", \"percentage\":\"%s\", \"randomSeed\": \"%s\"}", replace, percentage, randomSeed);
        resp = new MemoryComputationImpl().transformationEntry("gab1", "sample", sampleParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry Sample failed");
            return false;
        }
        printFailed("testTransformationEntry Sample Successfully");


        String unionParameter = "{\"distributedDataset1\": \"gab1\", \"distributedDataset2\": \"gab2\"}";
        resp = new MemoryComputationImpl().transformationEntry("", "union", unionParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry union failed");
            return false;
        }
        printFailed("testTransformationEntry union Successfully");


        String intersectionParameter = "{\"distributedDataset1\": \"gab1\", \"distributedDataset2\": \"gab2\"}";
        resp = new MemoryComputationImpl().transformationEntry("", "intersection", intersectionParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry intersection failed");
            return false;
        }
        printFailed("testTransformationEntry intersection Successfully");


        resp = new MemoryComputationImpl().transformationEntry("gab5", "distinct", "", token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry distinct failed");
            return false;
        }
        printFailed("testTransformationEntry distinct Successfully");



        resp = new MemoryComputationImpl().transformationEntry("gab3", "group", "", token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry group failed");
            return false;
        }
        printFailed("testTransformationEntry group Successfully");


        String reduceParameter = "{\"reduceFunction\":\"udf.Udf.udfReduce\", \"keyField\":\"A\"}";
        resp = new MemoryComputationImpl().transformationEntry("gab3", "reduce", reduceParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry reduce failed");
            return false;
        }
        printFailed("testTransformationEntry reduce Successfully");

        String sortParameter = "{\"sort\":\"0\", \"keyField\":\"A\"}";
        resp = new MemoryComputationImpl().transformationEntry("gab3", "sort", sortParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry sort failed");
            return false;
        }
        printFailed("testTransformationEntry sort Successfully");


        String joinParameter = "{\"joinMethod\":\"1\", \"distributedDataset1\": \"gab3\", \"distributedDataset2\": \"gab4\"}";
        resp = new MemoryComputationImpl().transformationEntry("", "join", joinParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry join failed");
            return false;
        }
        printFailed("testTransformationEntry join Successfully");


        String partitionParameter = "{\"partitionNumber\":\"2\"}";
        resp = new MemoryComputationImpl().transformationEntry("gab3", "partition", partitionParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry partition failed");
            return false;
        }
        printFailed("testTransformationEntry partition Successfully");


        return true;
    }

}
