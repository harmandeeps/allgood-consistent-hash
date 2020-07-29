package benchmark;

import org.ishugaliy.allgood.consistent.hash.ConsistentHash;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.hasher.DefaultHasher;
import org.ishugaliy.allgood.consistent.hash.node.SimpleNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.ishugaliy.allgood.consistent.hash.samples.ClusterManager.NODES_COUNT;

public class AllGoodConsistentHash
    implements Benchmark
{
    protected int nodesCount = 100;
    protected int nodesDelta = 20;
    private Map<String, SimpleNode> defaultKeyNodeMap;
    private ConsistentHash<SimpleNode> ring;

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
                .nodes(IntStream.range(0, NODES_COUNT)
                        .mapToObj(i -> SimpleNode.of("192.168.1." + i))
                        .collect(Collectors.toSet()))
                .build();
        defaultKeyNodeMap = new HashMap<>();
        for(String request : requests)
        {
            defaultKeyNodeMap.put(request, ring.locate(request).get());
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
        ring.addAll(IntStream.range(nodesCount, nodesCount+ nodesDelta)
                .mapToObj(i -> SimpleNode.of("192.168.1." + i))
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
    public String getBenchmarkName()
    {
        return new String("All Good Consistent Hash");
    }
}
