package Trabaio;
import java.io.IOException;
import java.net.UnknownHostException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

public interface InterfaceRMI extends Remote {
	String buscarDonoChunk (int chunkId) throws RemoteException;
	
	void registrarPosseChunk (int chunkId, String IpDoNode) throws RemoteException;
	
	void NoduloAtivo(String Ip, int Porta) throws RemoteException;

	int verificaVazio() throws RemoteException;
	
	ArrayList<String> getNosAtivos() throws RemoteException;
		 
	ArrayList<Integer> getPortaNosAtivos() throws RemoteException;

	List<Integer> buscarChunkDono(String Ip) throws RemoteException;
	
	void setNomeArquivo(String nome) throws RemoteException;
	
	String getNomeArquivo() throws RemoteException;
	
	
}
