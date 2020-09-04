package acekg.janusgraph.importer;

import java.io.Serializable;

public class VertexIDPairs implements Serializable {
    private long vtxid;
    private long vid;

    public long getVtxid() { return vtxid; }
    public void setVtxid(long vtxid) { this.vtxid = vtxid; }
    public long getVid() { return vid; }
    public void setVid(long vid) { this.vid = vid; }
    public VertexIDPairs(long vtxid, long vid) { this.vtxid = vtxid; this.vid = vid; }
}
