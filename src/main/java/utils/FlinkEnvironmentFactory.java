package utils;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkEnvironmentFactory {

    public enum ExecutionType{
        BATCH,
        STREAMING
    }

    public static ExecutionEnvironment createBatchEnvironment() {
        return ExecutionEnvironment.getExecutionEnvironment();
    }

    public static StreamExecutionEnvironment createStreamingEnvironment() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

//    public static ExecutionEnvironment createEnvironment(ExecutionType type) {
//        switch (type) {
//            case BATCH:
//                return createBatchEnvironment();
//            case STREAMING:
//                return createStreamingEnvironment();
//            default:
//                throw new IllegalArgumentException("Invalid execution type");
//        }
//    }

}
