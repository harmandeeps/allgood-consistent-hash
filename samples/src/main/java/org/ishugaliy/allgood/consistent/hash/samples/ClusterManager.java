package org.ishugaliy.allgood.consistent.hash.samples;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ClusterManager
{
    static int REQUEST_COUNT = 10000;
    Map<String, SimpleNode> defaultKeyNodeMap;
    List<SimpleNode> ring;

    public ClusterManager()
    {
        ring = new ArrayList<>();
        ring.add(new SimpleNode("node0", NodeState.RUNNING));
        ring.add(new SimpleNode("node1", NodeState.RUNNING));
        ring.add(new SimpleNode("node2", NodeState.RUNNING));
        ring.add(new SimpleNode("node3", NodeState.RUNNING));
        defaultKeyNodeMap = new HashMap<>();

        for(int i=0; i<REQUEST_COUNT; i++)
        {
            String key = UUID.randomUUID().toString();
            defaultKeyNodeMap.put(key, ring.get(this.getNodeIndex(ring, key, GuavaCases.SIMPLE)));
        }
    }

    private void GuavaConsistentHashing()
    {
        System.out.println("Verifying if equals working fine");
        getMissCountAndPrint(GuavaCases.SIMPLE);
        System.out.println();

        System.out.println("Guava Consistent Hashing");

        System.out.print("Replace node: ");
        ring.clear();
        ring.add(new SimpleNode("node1", NodeState.RUNNING));
        ring.add(new SimpleNode("node2", NodeState.RUNNING));
        ring.add(new SimpleNode("node3", NodeState.RUNNING));
        ring.add(new SimpleNode("node4", NodeState.RUNNING));
        getMissCountAndPrint(GuavaCases.SIMPLE);

        System.out.println();

        System.out.print("Add node:     ");
        ring.clear();
        ring.add(new SimpleNode("node0", NodeState.RUNNING));
        ring.add(new SimpleNode("node1", NodeState.RUNNING));
        ring.add(new SimpleNode("node4", NodeState.RUNNING));
        ring.add(new SimpleNode("node2", NodeState.RUNNING));
        ring.add(new SimpleNode("node3", NodeState.RUNNING));
        getMissCountAndPrint(GuavaCases.SIMPLE);

        System.out.println();

        System.out.print("Delete node:  ");
        ring.clear();
        ring.add(new SimpleNode("node0", NodeState.RUNNING));
        ring.add(new SimpleNode("node1", NodeState.RUNNING));
        ring.add(new SimpleNode("node2", NodeState.RUNNING));
        getMissCountAndPrint(GuavaCases.SIMPLE);
    }

    private void RetainNodesConsistentHashing()
    {
        System.out.println("Verifying if equals working fine");
        getMissCountAndPrint(GuavaCases.RETAINING_NODES);
        System.out.println();

        System.out.println("Retain Nodes Consistent Hashing");
        System.out.print("Replace node: ");

        ring.clear();
        ring.add(new SimpleNode("node0", NodeState.STOPPED));
        ring.add(new SimpleNode("node1", NodeState.RUNNING));
        ring.add(new SimpleNode("node2", NodeState.RUNNING));
        ring.add(new SimpleNode("node3", NodeState.RUNNING));
        ring.add(new SimpleNode("node4", NodeState.RUNNING));
        getMissCountAndPrint(GuavaCases.RETAINING_NODES);

        System.out.println();

        System.out.print("Add node:     ");
        ring.clear();
        ring.add(new SimpleNode("node0", NodeState.RUNNING));
        ring.add(new SimpleNode("node1", NodeState.RUNNING));
        ring.add(new SimpleNode("node4", NodeState.RUNNING));
        ring.add(new SimpleNode("node2", NodeState.RUNNING));
        ring.add(new SimpleNode("node3", NodeState.RUNNING));
        getMissCountAndPrint(GuavaCases.RETAINING_NODES);

        System.out.println();

        System.out.print("Delete node:  ");
        ring.clear();
        ring.add(new SimpleNode("node0", NodeState.RUNNING));
        ring.add(new SimpleNode("node1", NodeState.RUNNING));
        ring.add(new SimpleNode("node2", NodeState.RUNNING));
        ring.add(new SimpleNode("node3", NodeState.STOPPED));
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
        for (String key : defaultKeyNodeMap.keySet()) {
            if (!defaultKeyNodeMap.get(key).getName().equals(
                    ring.get(getNodeIndex(ring, key, guavaCase)).getName())) {
                missCount++;
            }
        }

        System.out.print(" Miss count: " + missCount);
        System.out.print(String.format(" , Miss count (%%): %.2f", (missCount / REQUEST_COUNT) * 100));
    }

    public int getNodeIndex(List<SimpleNode> stateRing, String key, GuavaCases guavaCase)
    {
        HashFunction hf = Hashing.md5();
        hf = Hashing.hmacMd5(hf.hashString(key, Charsets.UTF_8).asBytes());
        HashCode hc = hf.hashString(key, Charsets.UTF_8);
        int nodeIndex = Hashing.consistentHash(hc, stateRing.size());

        if (guavaCase.equals(GuavaCases.SIMPLE)) {
            return nodeIndex;
        }

        if (hc.asInt() % 2 == 0) {
            while(!stateRing.get(nodeIndex).getNodeState().equals(NodeState.RUNNING)) {
                nodeIndex = (nodeIndex+1)%stateRing.size();
            }
        }
        else {
            while(!stateRing.get(nodeIndex).getNodeState().equals(NodeState.RUNNING)) {
                nodeIndex = Math.abs(nodeIndex-1)%stateRing.size();
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
