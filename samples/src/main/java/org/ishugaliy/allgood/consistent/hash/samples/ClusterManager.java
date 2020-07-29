package org.ishugaliy.allgood.consistent.hash.samples;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClusterManager
{
    public static int REQUEST_COUNT = 10000;
    public static int NODES_COUNT = 100;
    public static final int NODES_DELTA = 20;
    Map<String, SimpleNode> defaultKeyNodeMap;
    List<SimpleNode> ring;

    public ClusterManager()
    {
        ring = IntStream.range(0, NODES_COUNT)
                .mapToObj(i -> new SimpleNode("192.168.1." + i, NodeState.RUNNING))
                .collect(Collectors.toList());

        defaultKeyNodeMap = new HashMap<>();

        for(int i=0; i<REQUEST_COUNT; i++)
        {
            String key = UUID.randomUUID().toString();
            defaultKeyNodeMap.put(key, ring.get(this.getNodeIndex(key, GuavaCases.SIMPLE)));
        }
    }

    private void replaceNodesSimple()
    {
        deleteNodesSimple();
        addNodesSimple();
    }

    private void replaceNodesRetainNodes()
    {
        deleteNodesRetainNodes();
        addNodesSimple();
    }

    private void deleteNodesSimple()
    {
        // remove some nodes from the ring
        Random rand = new Random();
        for (int i = 0; i < NODES_DELTA; i++) {
            ring.remove(ring.get(rand.nextInt(ring.size() - 1)));
        }
    }

    private void deleteNodesRetainNodes()
    {
        // remove some nodes from the ring
        Random rand = new Random();
        for (int i = 0; i < NODES_DELTA; i++) {
            int nodeIndex = rand.nextInt(ring.size() - 1);
            SimpleNode changeNode = ring.remove(nodeIndex);
            changeNode.setNodeState(NodeState.STOPPED);
            ring.add(nodeIndex, changeNode);
        }
    }

    private void addNodesSimple()
    {
        ring.addAll(IntStream.range(NODES_COUNT, NODES_COUNT+ NODES_DELTA)
                .mapToObj(i -> new SimpleNode("192.168.1." + i, NodeState.RUNNING))
                .collect(Collectors.toList()));
    }

    private void GuavaConsistentHashing()
    {
        System.out.println("Verifying if equals working fine");
        getMissCountAndPrint(GuavaCases.SIMPLE);
        System.out.println();

        System.out.println("Guava Consistent Hashing");

        System.out.print("Replace node: ");
        replaceNodesSimple();
        getMissCountAndPrint(GuavaCases.SIMPLE);

        System.out.println();

        System.out.print("Add node:     ");
        addNodesSimple();
        getMissCountAndPrint(GuavaCases.SIMPLE);

        System.out.println();

        System.out.print("Delete node:  ");
        deleteNodesSimple();
        getMissCountAndPrint(GuavaCases.SIMPLE);
    }

    private void RetainNodesConsistentHashing()
    {
        System.out.println("Verifying if equals working fine");
        getMissCountAndPrint(GuavaCases.RETAINING_NODES);
        System.out.println();

        System.out.println("Retain Nodes Consistent Hashing");
        System.out.print("Replace node: ");

        replaceNodesRetainNodes();
        getMissCountAndPrint(GuavaCases.RETAINING_NODES);

        System.out.println();

        System.out.print("Add node:     ");
        addNodesSimple();
        getMissCountAndPrint(GuavaCases.RETAINING_NODES);

        System.out.println();

        System.out.print("Delete node:  ");
        deleteNodesRetainNodes();
        getMissCountAndPrint(GuavaCases.RETAINING_NODES);
    }

    public static void main(String[] args)
    {
        new ClusterManager().GuavaConsistentHashing();
        System.out.println();
        new ClusterManager().RetainNodesConsistentHashing();
        System.out.println();
        new AllGoodConsistentHashBenchmark().RunBenchMark();
    }

    private void getMissCountAndPrint(GuavaCases guavaCase)
    {
        double missCount = 0L;
        long start = System.currentTimeMillis();
        for (String key : defaultKeyNodeMap.keySet()) {
            if (!defaultKeyNodeMap.get(key).getName().equals(
                    ring.get(getNodeIndex(key, guavaCase)).getName())) {
                missCount++;
            }
        }
        float timeTaken = (float) (System.currentTimeMillis() - start) / 1000;

        System.out.print(" Miss count: " + missCount);
        System.out.print(String.format(" , Miss count (%%): %.2f, Total Time Taken: %f sec",
                (missCount / REQUEST_COUNT) * 100, timeTaken));
    }

    public int getNodeIndex(String key, GuavaCases guavaCase)
    {
        HashFunction hf = Hashing.md5();
        hf = Hashing.hmacMd5(hf.hashString(key, Charsets.UTF_8).asBytes());
        HashCode hc = hf.hashString(key, Charsets.UTF_8);
        int nodeIndex = Hashing.consistentHash(hc, ring.size());

        if (guavaCase.equals(GuavaCases.SIMPLE)) {
            return nodeIndex;
        }

        if (hc.asInt() % 2 == 0) {
            while(!ring.get(nodeIndex).getNodeState().equals(NodeState.RUNNING)) {
                //System.out.println(" increasing " + ring.get(nodeIndex).getName());
                nodeIndex = (nodeIndex+1)%ring.size();
            }
        }
        else {
            while(!ring.get(nodeIndex).getNodeState().equals(NodeState.RUNNING)) {
                //System.out.println(" decreasing " + ring.get(nodeIndex).getName());
                if (nodeIndex == 0) {
                    nodeIndex = ring.size();
                }
                nodeIndex = (nodeIndex-1)%ring.size();
            }
        }
        return nodeIndex;
    }

    public enum NodeState
    {
        RUNNING,
        STOPPED
    }

    public enum GuavaCases
    {
        SIMPLE,
        RETAINING_NODES;
    }

    class SimpleNode
    {
        private NodeState nodeState;
        private String name;

        public SimpleNode(String name, NodeState nodeState)
        {
            this.nodeState = nodeState;
            this.name = name;
        }

        public NodeState getNodeState()
        {
            return nodeState;
        }

        public String getName()
        {
            return name;
        }

        public void setNodeState(NodeState nodeState)
        {
            this.nodeState = nodeState;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) { return true; }
            if (o == null || getClass() != o.getClass()) { return false; }

            SimpleNode that = (SimpleNode) o;

            if (nodeState != that.nodeState) { return false; }
            return name.equals(that.name);
        }

        @Override
        public int hashCode()
        {
            int result = nodeState.hashCode();
            result = 31 * result + name.hashCode();
            return result;
        }
    }
}
