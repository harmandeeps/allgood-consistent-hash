package org.ishugaliy.allgood.consistent.hash.samples;

import org.ishugaliy.allgood.consistent.hash.ConsistentHash;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.node.SimpleNode;

import java.util.HashMap;
import java.util.Map;

public class AllGoodConsistentHashBenchmark
{
    static int REQUEST_COUNT = 7;
    Map<String, SimpleNode> defaultKeyNodeMap;
    ConsistentHash<SimpleNode> ring;

    public AllGoodConsistentHashBenchmark()
    {
        ring = HashRing.<SimpleNode>newBuilder().build();
        ring.add(SimpleNode.of("node0"));
        ring.add(SimpleNode.of("node1"));
        ring.add(SimpleNode.of("node2"));
        ring.add(SimpleNode.of("node3"));
        defaultKeyNodeMap = new HashMap<>();
        defaultKeyNodeMap.put("a", ring.locate("a").get());
        defaultKeyNodeMap.put("b", ring.locate("b").get());
        defaultKeyNodeMap.put("c", ring.locate("c").get());
        defaultKeyNodeMap.put("d", ring.locate("d").get());
        defaultKeyNodeMap.put("e", ring.locate("e").get());
        defaultKeyNodeMap.put("f", ring.locate("f").get());
        //defaultKeyNodeMap.put("g", ring.locate("g").get());

        for (String key : defaultKeyNodeMap.keySet()) {
            System.out.println("Key: " + key + " Value: " + defaultKeyNodeMap.get(key).getKey());
        }
    }

    public void RunBenchMark()
    {
        System.out.println("Verifying if equals working fine");
        getMissCountAndPrint(defaultKeyNodeMap);
        System.out.println();

        System.out.println("Guava Consistent Hashing");
        System.out.print("Replace node: ");

        ring = HashRing.<SimpleNode>newBuilder().build();
        ring.add(SimpleNode.of("node1"));
        ring.add(SimpleNode.of("node2"));
        ring.add(SimpleNode.of("node3"));
        ring.add(SimpleNode.of("node4"));

        getMissCountAndPrint(defaultKeyNodeMap);

        System.out.println();

        System.out.print("Add node:     ");
        ring = HashRing.<SimpleNode>newBuilder().build();
        ring.add(SimpleNode.of("node0"));
        ring.add(SimpleNode.of("node1"));
        ring.add(SimpleNode.of("node4"));
        ring.add(SimpleNode.of("node2"));
        ring.add(SimpleNode.of("node3"));
        getMissCountAndPrint(defaultKeyNodeMap);

        System.out.println();

        System.out.print("Delete node:  ");
        ring = HashRing.<SimpleNode>newBuilder().build();
        ring.add(SimpleNode.of("node0"));
        ring.add(SimpleNode.of("node1"));
        ring.add(SimpleNode.of("node2"));
        getMissCountAndPrint(defaultKeyNodeMap);
    }

    private void getMissCountAndPrint(Map<String, SimpleNode> originalKeyMap)
    {
        double missCount = 0L;
        for (String key : originalKeyMap.keySet()) {
            if (!originalKeyMap.get(key).getKey().equals(
                    ring.locate(key).get().getKey())) {
                missCount++;
            }
        }

        System.out.print(" Miss count: " + missCount);
        System.out.print(String.format(" , Miss count (%%): %.2f", (missCount / REQUEST_COUNT) * 100));
    }

    public static void main(String[] args) {
        AllGoodConsistentHashBenchmark allGoodConsistentHashBenchmark = new AllGoodConsistentHashBenchmark();
        allGoodConsistentHashBenchmark.RunBenchMark();
    }

}