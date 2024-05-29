package com.colak.datastructures.mapentrprocessor;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

// See https://stackoverflow.com/questions/78545375/hazelcast-entryprocessor-process-method-called-twice
@Slf4j
public class ClusterEntryProcessorTest {

    private static final String MAP_NAME = "testMap";

    public static void main(String[] args) throws InterruptedException {

        List<HazelcastInstance> hazelcastInstances = setupHazelcast(2);
        HazelcastInstance hazelcastInstance = hazelcastInstances.get(0);

        IMap<Integer, String> testMap;
        testMap = hazelcastInstance.getMap(MAP_NAME);

        testMap.put(1, "Holmes");
        testMap.executeOnEntries(new UpdateNameEntryProcessor("Watson"));

        hazelcastInstances.forEach(HazelcastInstance::shutdown);

    }

    private static List<HazelcastInstance> setupHazelcast(int numberOfInstances) {
        List<HazelcastInstance> instances = new ArrayList<>();
        IntStream.range(0, numberOfInstances).forEach(i -> {
            Config config = new Config();
            MapConfig mapConfig = config.getMapConfig(MAP_NAME);
            mapConfig.setBackupCount(0);

            JetConfig jetConfig = config.getJetConfig();
            jetConfig.setEnabled(true);
            instances.add(Hazelcast.newHazelcastInstance(config));
        });
        return instances;
    }

    private static class UpdateNameEntryProcessor implements HazelcastInstanceAware, EntryProcessor<Integer, String, String> {

        private final String name;
        private transient HazelcastInstance hazelcastInstance;

        public UpdateNameEntryProcessor(String name) {
            this.name = name;
        }

        @Override
        public EntryProcessor<Integer, String, String> getBackupProcessor() {
            return null;
        }

        @Override
        public String process(Map.Entry<Integer, String> entry) {
            log.info("Coming Here Key: {} Value : {}", entry.getKey(), entry.getValue());
            log.info(hazelcastInstance.getName());
            entry.setValue(name);
            return name;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }
    }
}
