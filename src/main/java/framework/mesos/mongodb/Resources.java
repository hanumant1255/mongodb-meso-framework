package framework.mesos.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Resource;

public class Resources {
    public static final String RESOURCE_PORTS = "ports";
    public static final String RESOURCE_CPUS = "cpus";
    public static final String RESOURCE_MEM = "mem";
    public static final String RESOURCE_DISK = "disk";
    
    
    public static ArrayList<Protos.Resource> buildFrameworkResources(Configuration configuration) {		 
        Protos.Resource cpus = Resources.cpus(configuration.getCpus(), configuration.getframeworkRole());
        Protos.Resource mem = Resources.mem(configuration.getMem(), configuration.getframeworkRole());
        Protos.Resource disk = Resources.disk(configuration.getDisk(), configuration.getframeworkRole());
        return new ArrayList<>(Arrays.asList(cpus, mem, disk));
        
    }
    public static Protos.Resource cpus(double cpus, String frameworkRole) {
        return Protos.Resource.newBuilder()
                .setName(RESOURCE_CPUS)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpus).build())
                .setRole(frameworkRole)
                .build();
    }

    public static Protos.Resource mem(double mem, String frameworkRole) {
        return Protos.Resource.newBuilder()
                .setName(RESOURCE_MEM)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(mem).build())
                .setRole(frameworkRole)
                .build();
    }

    public static Protos.Resource disk(double disk, String frameworkRole) {
        return Protos.Resource.newBuilder()
                .setName(RESOURCE_DISK)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(disk).build())
                .setRole(frameworkRole)
                .build();
    }
    
    public static Protos.Resource portRange(long beginPort, long endPort, String frameworkRole) {
        Protos.Value.Range singlePortRange = Protos.Value.Range.newBuilder().setBegin(beginPort).setEnd(endPort).build();
        return Protos.Resource.newBuilder()
                .setName(RESOURCE_PORTS)
                .setType(Protos.Value.Type.RANGES)
                .setRanges(Protos.Value.Ranges.newBuilder().addRange(singlePortRange))
                .setRole(frameworkRole)
                .build();
    }
    
    public static Protos.Resource singlePortRange(long port, String frameworkRole) {
        return portRange(port, port, frameworkRole);
    }
    
    public static boolean isPortAvailable(List<Resource> resourcesList, Integer port) {
        final AtomicBoolean available = new AtomicBoolean(false);
        resourcesList.stream().filter(resource -> resource.getType().equals(org.apache.mesos.Protos.Value.Type.RANGES))
                .forEach(resource -> resource.getRanges().getRangeList().stream().forEach(range -> {
                    if (range.getBegin() <= port && port <= range.getEnd()) {
                        available.set(true);
                    }
                }));
        return available.get();
    }


}
