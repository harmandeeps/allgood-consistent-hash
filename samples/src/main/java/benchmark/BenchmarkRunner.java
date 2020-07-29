package benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class BenchmarkRunner
{
    private static int REQUEST_COUNT = 10_000;
    protected static int NODES_COUNT = 100;
    protected static int NODES_DELTA = 20;
    private List<String> requests;

    public BenchmarkRunner()
    {
        requests = new ArrayList<>();
        for(int i=0; i<REQUEST_COUNT; i++)
        {
            String key = UUID.randomUUID().toString();
            requests.add(key);
        }
    }

    private void runTest(Benchmark benchmark)
    {
        System.out.println("\n--------------------");
        benchmark.initialize(requests);
        System.out.println("\nVerifying if equals working fine");
        getMissCountAndPrint(benchmark);
        System.out.println();

        System.out.println(benchmark.getBenchmarkName());

        System.out.print("Replace node: ");
        benchmark.replaceNodes();
        getMissCountAndPrint(benchmark);

        System.out.println();

        System.out.print("Add node:     ");
        benchmark.addNodes();
        getMissCountAndPrint(benchmark);

        System.out.println();

        System.out.print("Delete node:  ");
        benchmark.deleteNodes();
        getMissCountAndPrint(benchmark);
    }

    private void getMissCountAndPrint(Benchmark benchmark)
    {
        long start = System.currentTimeMillis();
        double missCount = 0L;
        for (String key : requests) {
            if (!benchmark.isSameNode(key)) {
                missCount++;
            }
        }
        float timeTaken = (float) (System.currentTimeMillis() - start) / 1000;

        System.out.print(" Miss count: " + missCount);
        System.out.print(String.format(" , Miss count (%%): %.2f, Total Time Taken: %f sec",
                (missCount / REQUEST_COUNT) * 100, timeTaken));
    }


    public static void main(String[] args)
    {
        BenchmarkRunner benchmarkRunner = new BenchmarkRunner();
        BasicConsistentHash basicConsistentHash = new BasicConsistentHash(NODES_COUNT, NODES_DELTA);
        benchmarkRunner.runTest(basicConsistentHash);

        AdvanceConsistentHash advanceConsistentHash = new AdvanceConsistentHash(NODES_COUNT, NODES_DELTA);
        benchmarkRunner.runTest(advanceConsistentHash);

        AllGoodConsistentHash allGoodConsistentHash = new AllGoodConsistentHash(NODES_COUNT, NODES_DELTA);
        benchmarkRunner.runTest(allGoodConsistentHash);
    }
}
