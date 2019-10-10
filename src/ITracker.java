import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.*;
public interface ITracker extends Remote {

    public boolean registerReply(NodeStruct node, int num_files, Map<String, Double> files, Map<String, ArrayList<String>> fc) throws RemoteException;

    public Map<String, Double> fileListReply() throws RemoteException;
    public Map<String, ArrayList<NodeStruct>> FileLocationsReply(String req_file) throws RemoteException;
    public boolean chunkRegisterReply(NodeStruct peer, String chunk_id) throws RemoteException;
    public boolean leaveReply(NodeStruct peer)  throws RemoteException;
    public boolean heartBeat(NodeStruct node) throws RemoteException;

}
