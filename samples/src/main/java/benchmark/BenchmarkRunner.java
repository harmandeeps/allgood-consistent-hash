package benchmark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class BenchmarkRunner
{
    private static int REQUEST_COUNT = 10_000;
    protected static int NODES_COUNT = 20;
    protected static int NODES_DELTA = 5;
    protected static int runs = 80;
    private static List<String> requests;
    private Map<String, Map<Actions, Double>> metric;

    public BenchmarkRunner()
    {
        requests = new ArrayList<>();
        for(int i=0; i<REQUEST_COUNT; i++)
        {
            String key = UUID.randomUUID().toString();
            requests.add(key);
        }
        metric = new HashMap<>();
    }

    enum Actions {
        VERIFY,
        REPLACE,
        ADD,
        DELETE;
    }

    private void runTest(Benchmark benchmark)
    {
        System.out.println("\nVerifying if equals working fine");
        getMissCountAndPrint(benchmark, Actions.VERIFY);
        System.out.println();

        System.out.println(benchmark.getBenchmarkName());

        System.out.print("Replace node: ");
        benchmark.replaceNodes();
        getMissCountAndPrint(benchmark, Actions.REPLACE);
        //benchmark.locateKeys(requests);

        System.out.println();

        System.out.print("Add node:     ");
        benchmark.addNodes();
        getMissCountAndPrint(benchmark, Actions.ADD);
        //benchmark.locateKeys(requests);

        System.out.println();

        System.out.print("Delete node:  ");
        benchmark.deleteNodes();
        getMissCountAndPrint(benchmark, Actions.DELETE);
        benchmark.locateKeys(requests);
    }

    private void getMissCountAndPrint(Benchmark benchmark, Actions action)
    {
        long start = System.currentTimeMillis();
        double missCount = 0L;
        for (String key : requests) {
            if (!benchmark.isSameNode(key)) {
                missCount++;
            }
        }
        float timeTaken = (float) (System.currentTimeMillis() - start) / 1000;
        if (action.equals(Actions.VERIFY))
        {
            assert missCount == 0 : "Verification failed";
            return;
        }

        System.out.print(String.format(" Size: %d, Miss count: %s", benchmark.getRingSize(), missCount));
        System.out.print(String.format(" , Miss count (%%): %.2f, Total Time Taken: %f sec",
                (missCount / REQUEST_COUNT) * 100, timeTaken));

        Map<Actions, Double> metricMap = metric.get(benchmark.getBenchmarkName());
        double value = metricMap.getOrDefault(action, 0.0);
        metricMap.put(action, value+missCount);
        metric.put(benchmark.getBenchmarkName(), metricMap);
    }


    public static void main(String[] args)
    {
        BenchmarkRunner benchmarkRunner = new BenchmarkRunner();
        AdvanceConsistentHash advanceConsistentHash = new AdvanceConsistentHash(NODES_COUNT, NODES_DELTA);
        AllGoodConsistentHash allGoodConsistentHash = new AllGoodConsistentHash(NODES_COUNT, NODES_DELTA);

        advanceConsistentHash.initialize(requests);
        allGoodConsistentHash.initialize(requests);

        benchmarkRunner.metric.put(advanceConsistentHash.getBenchmarkName(), new HashMap<>());
        benchmarkRunner.metric.put(allGoodConsistentHash.getBenchmarkName(), new HashMap<>());

        for(int i=0; i<runs; i++) {
            benchmarkRunner.runTest(advanceConsistentHash);
            benchmarkRunner.runTest(allGoodConsistentHash);
        }

        System.out.println();
        for(String benchmark : benchmarkRunner.metric.keySet())
        {
            System.out.println(" ---- " + "Metrics for: " + benchmark + " ---- ");
            for(Actions actions : benchmarkRunner.metric.get(benchmark).keySet())
            {
                System.out.println(String.format("Action: %s Miss rate:  %.2f", actions.name(),
                        benchmarkRunner.metric.get(benchmark).get(actions)/(runs*REQUEST_COUNT)*100));
            }
        }
    }
}
