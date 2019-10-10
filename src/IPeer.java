import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IPeer extends Remote {

    public void registerRequestToServer() throws RemoteException;
    public void fileListRequest() throws RemoteException; //to get all files
    public boolean fileLocationsRequest() throws RemoteException; //to get file file locations
    public void chunkRegisterRequest(String filename, String chunkname) throws RemoteException; //tells the tracker that it received a new chunk and registers
    public void leaveRequest() throws RemoteException; //tells the server to remove this peer and all its files

    //public OutputStream getOutputStream(File f) throws IOException;
    //public InputStream getInputStream(File f) throws IOException;
    public byte[] peerFileSend(NodeStruct peer, String src_file) throws RemoteException;
}