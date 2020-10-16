package com.totango.dispatcher;

import com.google.common.base.Functions;
import com.totango.dispatcher.zk.ZooKeeperService;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import lombok.extern.log4j.Log4j2;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Log4j2
public class DispatcherNode {

    private final ZooKeeperService service;

    private final Worker worker;

    private ConfigDetails mainConfigDetails;

    private List<String> children;

    public void start() {
        try {
            service.init();
            service.electLeader(() -> {
                Executors.newSingleThreadExecutor().execute(() -> {
                    try {
                        log.info("Start looking on main configuration changes...");
                        service.addUpdateChildrenListener(this::onChildrenChanges, true);
                        service.addMainConfigUpdateListener(this::onMainConfigurationChanges, true);
                    } catch (Exception e) {
                        log.error(e, e);
                    }
                });
            });
            service.addNodeUpdateListener(this::onNewConfigDetails, true);
            worker.start();
        } catch (Exception e) {
            log.error(e, e);
        }
    }

    public void stop() {
        try {
            worker.close();
        } catch (IOException e) {
            log.error(e, e);
        }
    }

    protected void onNewConfigDetails(ConfigDetails configDetails) {
        //workload update received. notify the worker
        log.info("Updated workload received.");
        worker.workloadUpdated(configDetails);
    }

    protected void onChildrenChanges(List<String> children) {
        //got a changes in a workers list, re-balance the workload
        log.info("got a changes in a workers list, re-balance the workload");
        this.children = children;
        rebalanceWorkload();
    }

    public void onMainConfigurationChanges(ConfigDetails mainConfigDetails) {
        //main configuration changed
        log.info("main configuration changed");
        this.mainConfigDetails = mainConfigDetails;
        rebalanceWorkload();
    }

    private void rebalanceWorkload() {
        if (this.mainConfigDetails != null && this.children != null) {
            log.info("re-balance Workload");
        } else {
            log.info("Wait with re-balancing, not all data ready");
            return;
        }
        int workers = children.size();
        LinkedHashMap<String, ConfigDetails> loadPerWorker = children.stream().collect(Collectors.toMap(
                Functions.identity(),
                k -> ConfigDetails.newBuilder().setJobs(new ArrayList<>()).build(),
                (u, v) -> u,
                LinkedHashMap::new));

        for (JobDetails jobDetails : mainConfigDetails.getJobs()) {
            int consumers = jobDetails.getConsumers();
            for (Integer consumerPerWorker : split(workers, consumers)) {
                Map.Entry<String, ConfigDetails> mapEntry = loadPerWorker.entrySet().iterator().next();
                mapEntry.getValue().getJobs().add(JobDetails.newBuilder(jobDetails).setConsumers(consumerPerWorker).build());
                loadPerWorker = resort(loadPerWorker);
            }
        }
        for (Map.Entry<String, ConfigDetails> entry : loadPerWorker.entrySet()) {
            try {
                log.info("Update config " + entry.getKey());
                service.updateNodeConfig(entry.getKey(), entry.getValue());
            } catch (Exception e) {
                log.error(e);
            }
        }
    }

    private List<Integer> split(int totalWorkers, int requiredConsumers) {
        int workers = (int) Math.ceil((float) requiredConsumers / totalWorkers);
        List<Integer> load = new ArrayList<>();
        while (requiredConsumers > 0) {
            load.add(workers);
            totalWorkers -= 1;
            requiredConsumers -= workers;
            if (totalWorkers > 0) {
                workers = (int) Math.ceil((float) requiredConsumers / totalWorkers);
            } else {
                break;
            }
        }
        return load;
    }

    public LinkedHashMap<String, ConfigDetails> resort(LinkedHashMap<String, ConfigDetails> map) {
        return map.entrySet().stream().sorted(Comparator.comparingInt(e -> e.getValue().getJobs().stream().map(JobDetails::getConsumers).mapToInt(Integer::intValue).sum()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (u, v) -> u,
                        LinkedHashMap::new));
    }

}
