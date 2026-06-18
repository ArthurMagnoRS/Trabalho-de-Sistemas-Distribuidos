package Trabaio;
import java.io.IOException;
import java.net.UnknownHostException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

public interface InterfaceRMI extends Remote {
	String buscarDonoChunk (int chunkId) throws RemoteException;
	
	void registrarPosseChunk (int chunkId, String IpDoNode) throws RemoteException;
	
	void NoduloAtivo(String Ip, int Porta) throws RemoteException;

	int verificaVazio() throws RemoteException;
	
	ArrayList<String> getNosAtivos() throws RemoteException;
		 
	ArrayList<Integer> getPortaNosAtivos() throws RemoteException;

}
