package com.colak.executorservice;

import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * See <a href="https://stackoverflow.com/questions/78112610/how-should-hazelcast-dynamically-allocated-fencedlocks-be-destroyed">...</a>
 */
@Slf4j
class ExecutorServiceTest {

    private static final String EXECUTOR_NAME = "my-executor";

    public static void main(String[] args) throws Exception {
        log.info("Starting Test");

        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();
        testSubmit(hazelcastServer);

        hazelcastServer.shutdown();
        log.info("Test completed");
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        ExecutorConfig executorConfig = config.getExecutorConfig(EXECUTOR_NAME);
        executorConfig.setPoolSize(1);

        return Hazelcast.newHazelcastInstance(config);
    }

    interface SerializableCallable extends Callable<String>, Serializable {}

    private static void testSubmit(HazelcastInstance hazelcastServer) throws InterruptedException, ExecutionException {
        IExecutorService executorService = hazelcastServer.getExecutorService(EXECUTOR_NAME);
        Future<String> future = executorService.submit((SerializableCallable) () -> "Test");
        String result = future.get();
        log.info("Result : {}", result);
    }
}
