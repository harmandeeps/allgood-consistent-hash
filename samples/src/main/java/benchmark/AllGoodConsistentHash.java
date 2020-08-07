package benchmark;

import org.ishugaliy.allgood.consistent.hash.ConsistentHash;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.hasher.DefaultHasher;
import org.ishugaliy.allgood.consistent.hash.node.SimpleNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AllGoodConsistentHash
    implements Benchmark
{
    protected int nodesCount = 100;
    protected int nodesDelta = 20;
    private Map<String, SimpleNode> defaultKeyNodeMap;
    private Map<SimpleNode, Integer> nodeToKeyMap;
    private ConsistentHash<SimpleNode> ring;
    private int count=1;

    public AllGoodConsistentHash(int nodesCount, int nodesDelta)
    {
        this.nodesCount = nodesCount;
        this.nodesDelta = nodesDelta;
    }

    @Override
    public void initialize(List<String> requests)
    {
        ring = HashRing.<SimpleNode>newBuilder()
                .hasher(DefaultHasher.MURMUR_3)
                .nodes(IntStream.range(0, nodesCount)
                        .mapToObj(i -> SimpleNode.of("192.168.1." + i))
                        .collect(Collectors.toSet()))
                .build();
        defaultKeyNodeMap = new HashMap<>();
        nodeToKeyMap = new HashMap<>();
        locateKeys(requests);
    }

    @Override
    public void locateKeys(List<String> requests)
    {
        nodeToKeyMap.clear();
        for(String request : requests)
        {
            SimpleNode simpleNode = ring.locate(request).get();
            defaultKeyNodeMap.put(request, simpleNode);
            nodeToKeyMap.put(simpleNode, nodeToKeyMap.getOrDefault(simpleNode, 0)+1);
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
        ring.addAll(IntStream.range(nodesCount, nodesCount+ nodesDelta)
                .mapToObj(i -> SimpleNode.of("192.168." + count + "." + i))
                .collect(Collectors.toList()));
    }

    @Override
    public void deleteNodes()
    {
        // remove some nodes from the ring
        Random rand = new Random();
        for (int i = 0; i < nodesDelta; i++) {
            List<SimpleNode> nodes = new ArrayList<>(ring.getNodes());
            ring.remove(nodes.get(rand.nextInt(nodes.size() - 1)));
        }
    }

    @Override
    public boolean isSameNode(String key)
    {
        return defaultKeyNodeMap.get(key).getKey().equals(
                ring.locate(key).get().getKey());
    }

    @Override
    public int getRingSize()
    {
        return ring.getNodes().size();
    }

    @Override
    public String getBenchmarkName()
    {
        return "All Good Consistent Hash";
    }

    @Override
    public Collection<Integer> getNodeLoadList()
    {
        return nodeToKeyMap.values();
    }
}
