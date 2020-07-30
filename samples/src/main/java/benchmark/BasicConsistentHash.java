package benchmark;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.ishugaliy.allgood.consistent.hash.node.SimpleNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BasicConsistentHash
    implements Benchmark
{
    protected int nodesCount = 100;
    protected int nodesDelta = 20;
    protected Map<String, SimpleNode> defaultKeyNodeMap;
    protected List<SimpleNode> ring;
    private int count=1;

    public BasicConsistentHash(int nodesCount, int nodesDelta)
    {
        this.nodesCount = nodesCount;
        this.nodesDelta = nodesDelta;
    }

    @Override
    public void initialize(List<String> requests)
    {
        ring = IntStream.range(0, nodesCount)
                .mapToObj(i -> new SimpleNode("192.168.1." + i, NodeState.RUNNING))
                .collect(Collectors.toList());

        defaultKeyNodeMap = new HashMap<>();
        locateKeys(requests);
    }

    @Override
    public void locateKeys(List<String> requests)
    {
        for(String request : requests)
        {
            defaultKeyNodeMap.put(request, ring.get(this.getNodeIndex(request)));
        }
    }

    @Override
    public void replaceNodes()
    {
        addNodes();
        deleteNodes();
    }

    @Override
    public void addNodes()
    {
        count++;
        ring.addAll(IntStream.range(0, nodesDelta)
                .mapToObj(i -> new SimpleNode("192.168." + count + "." + i, NodeState.RUNNING))
                .collect(Collectors.toList()));
        //System.out.println("add nodes: " + ring.size());
    }

    @Override
    public void deleteNodes()
    {
        // remove some nodes from the ring
        Random rand = new Random();
        for (int i = 0; i < nodesDelta; i++) {
            ring.remove(ring.get(rand.nextInt(ring.size() - 1)));
        }
    }

    @Override
    public boolean isSameNode(String key)
    {
        return defaultKeyNodeMap.get(key).getName().equals(
                ring.get(getNodeIndex(key)).getName());
    }

    @Override
    public int getRingSize()
    {
        return ring.size();
    }

    @Override
    public String getBenchmarkName()
    {
        return new String("Basic Consistent Hash");
    }

    public int getNodeIndex(String key)
    {
        HashFunction hf = Hashing.md5();
        hf = Hashing.hmacMd5(hf.hashString(key, Charsets.UTF_8).asBytes());
        HashCode hc = hf.hashString(key, Charsets.UTF_8);
        int nodeIndex = Hashing.consistentHash(hc, ring.size());

        return nodeIndex;
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
}
