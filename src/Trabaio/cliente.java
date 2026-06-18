package Trabaio;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.io.DataInputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class cliente {

	public static void main (String[] args) {
		try {
			String ipServer = "127.0.0.1";
			String meuIP = "127.0.0.1";
			int minhaPorta = 5000;
			int portaServer = 1099;
			
			Registry registry = LocateRegistry.getRegistry(ipServer, portaServer);
			InterfaceRMI coordenadoremoto = (InterfaceRMI) registry.lookup("CoordenadorServidor");
			System.out.println("Conectando ao registro RMI em: " + ipServer);
			coordenadoremoto.NoduloAtivo(meuIP, minhaPorta);
			System.out.println("Conexão efetuada com sucesso! Carregando demais configurações e aceitando envios!");
			ServerSocket soqueteNode = new ServerSocket(minhaPorta); // preciso colocar a propria porta para eu poder abrir o 'recebimento'
			while (true) {
				Socket recebimento = soqueteNode.accept();
				DataInputStream dis = new DataInputStream(recebimento.getInputStream());
				// leitura deve ser feita na ORDEM DE ENVIO
				int idChunk = dis.readInt();
				int tamArqv = dis.readInt();
				byte[] buffer = new byte[tamArqv];
				dis.readFully(buffer);
				File arquivoMoldado = new File("chunk_"+idChunk+".part");
				FileOutputStream funil = new FileOutputStream(arquivoMoldado);
				funil.write(buffer);
				coordenadoremoto.registrarPosseChunk(idChunk, meuIP);
				System.out.println("Guardei o pedaço "+idChunk+" do arquivo com sucesso!");
				funil.close();
			}
			
			// criar um serverSocket aqui para recebimento. Socket normal serve mais para envio.
			// essa variavel de interfaceRMI é como vamos fazer requisicoes para o servidor remotamente e conversar com eles por suas funcoes
			
			
			
		}catch (Exception e) {
            System.err.println("Erro no Nó Cliente: " + e.getMessage());
            e.printStackTrace();
	}
		
}
}
