package org.ishugaliy.allgood.consistent.hash.samples;

import org.ishugaliy.allgood.consistent.hash.ConsistentHash;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.hasher.DefaultHasher;
import org.ishugaliy.allgood.consistent.hash.node.SimpleNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.ishugaliy.allgood.consistent.hash.samples.ClusterManager.NODES_COUNT;
import static org.ishugaliy.allgood.consistent.hash.samples.ClusterManager.NODES_DELTA;
import static org.ishugaliy.allgood.consistent.hash.samples.ClusterManager.REQUEST_COUNT;

public class AllGoodConsistentHashBenchmark
{
    Map<String, SimpleNode> defaultKeyNodeMap;
    ConsistentHash<SimpleNode> ring;

    public AllGoodConsistentHashBenchmark()
    {
        // MURMUR_3,
        //Replace node:  Miss count: 4143.0 , Miss count (%): 41.43
        //Add node:      Miss count: 4143.0 , Miss count (%): 41.43
        //Delete node:   Miss count: 5271.0 , Miss count (%): 52.71
        ring = HashRing.<SimpleNode>newBuilder()
                .hasher(DefaultHasher.MURMUR_3)
                .partitionRate(1000)
                .nodes(IntStream.range(0, NODES_COUNT)
                        .mapToObj(i -> SimpleNode.of("192.168.1." + i))
                        .collect(Collectors.toSet()))
                .build();

        defaultKeyNodeMap = new HashMap<>();

        for(int i=0; i<REQUEST_COUNT; i++)
        {
            String key = UUID.randomUUID().toString();
            defaultKeyNodeMap.put(key, ring.locate(key).get());
        }
    }

    private void replaceNodes()
    {
        deleteNodes();
        addNodes();
    }

    private void deleteNodes()
    {
        // remove some nodes from the ring
        Random rand = new Random();
        for (int i = 0; i < NODES_DELTA; i++) {
            List<SimpleNode> nodes = new ArrayList<>(ring.getNodes());
            ring.remove(nodes.get(rand.nextInt(nodes.size() - 1)));
        }
    }

    private void addNodes()
    {
        ring.addAll(IntStream.range(NODES_COUNT, NODES_COUNT+ NODES_DELTA)
                .mapToObj(i -> SimpleNode.of("192.168.1." + i))
                .collect(Collectors.toList()));
    }

    public void RunBenchMark()
    {
        System.out.println("Verifying if equals working fine");
        getMissCountAndPrint(defaultKeyNodeMap);
        System.out.println();

        System.out.println("Guava Consistent Hashing");
        System.out.print("Replace node: ");

        replaceNodes();

        getMissCountAndPrint(defaultKeyNodeMap);

        System.out.println();

        System.out.print("Add node:     ");
        addNodes();
        getMissCountAndPrint(defaultKeyNodeMap);

        System.out.println();

        System.out.print("Delete node:  ");
        deleteNodes();
        getMissCountAndPrint(defaultKeyNodeMap);
    }

    private void getMissCountAndPrint(Map<String, SimpleNode> originalKeyMap)
    {
        double missCount = 0L;
        long start = System.currentTimeMillis();
        for (String key : originalKeyMap.keySet()) {
            if (!originalKeyMap.get(key).getKey().equals(
                    ring.locate(key).get().getKey())) {
                missCount++;
            }
        }
        float timeTaken = (float) (System.currentTimeMillis() - start) / 1000;

        System.out.print(" Miss count: " + missCount);
        System.out.print(String.format(" , Miss count (%%): %.2f, Total Time Taken: %f sec",
                (missCount / REQUEST_COUNT) * 100, timeTaken));
    }

    public static void main(String[] args) {
        AllGoodConsistentHashBenchmark allGoodConsistentHashBenchmark = new AllGoodConsistentHashBenchmark();
        allGoodConsistentHashBenchmark.RunBenchMark();
    }

}