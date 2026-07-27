package io.milvus.talon;

/** A cluster node: its id, its dialable address, and whether it is a worker. */
public final class NodeInfo {

    private final String id;
    private final String address;
    private final boolean worker;

    public NodeInfo(String id, String address, boolean worker) {
        this.id = id;
        this.address = address;
        this.worker = worker;
    }

    public String id() {
        return id;
    }

    /** {@code host:port} to dial for the data plane. */
    public String address() {
        return address;
    }

    public boolean isWorker() {
        return worker;
    }

    @Override
    public String toString() {
        return id + "@" + address + (worker ? " (worker)" : " (coordinator)");
    }
}
