package Trabaio;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

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
				int idChunk = dis.readInt(); // primeiro o id
				if (idChunk == -1) {
					System.out.println("O principal ja enviou todos os pedaços de arquivos.");
					int totalDeChunks = dis.readInt();
					dis.close();
					recebimento.close();
					
					new Thread(() -> {
						try {
							System.out.println("Verificando posse de chunks...");
							List<Integer> meusChunks = coordenadoremoto.buscarChunkDono(meuIP); // aqui estao os chunks que preciso fazer o download.
							System.out.println("Possuo "+meusChunks.size()+" chunks: "+meusChunks);
							
							Scanner scanner = new Scanner(System.in);
							System.out.println("//////// AGORA, DIGITE 'SIM' PARA COMEÇAR A REQUISITAR O ARQUIVO INTEIRO");
							String input = scanner.nextLine();
							if (input.equalsIgnoreCase("sim")) {
								for (int i = 0; i<totalDeChunks;i++) {
									if (meusChunks.contains(i)) {
										continue;
									}
									String dono = coordenadoremoto.buscarDonoChunk(i);
									ArrayList<String> nodes = coordenadoremoto.getNosAtivos();
									int indice = nodes.indexOf(dono);
									ArrayList<Integer> portas = coordenadoremoto.getPortaNosAtivos();
									int portaReq = portas.get(indice);
									Socket envioReq = new Socket(dono,portaReq);
									DataOutputStream dos = new DataOutputStream(envioReq.getOutputStream());
									DataInputStream disResposta = new DataInputStream(envioReq.getInputStream());
									dos.writeInt(-2); // flag de requisicao do chunk
									dos.writeInt(i); // chunk especifico
									int tamResp = disResposta.readInt();
									byte[] buffer = new byte[tamResp];
									disResposta.readFully(buffer); // leitura dos bytes.
									// logica de envio acima, e pelo proprio socket aberto envioReq, vamos esperar o que falta também.
									
										 
								}
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}).start();
					
					continue;
					
				} else if (idChunk == -2) { // recebimento de PEDIDO DE ENVIO DE CHUNKS PARA OUTRO NO
					
				}
				int tamArqv = dis.readInt(); // dps o tamanho
				byte[] buffer = new byte[tamArqv]; // agr o buffer
				dis.readFully(buffer);
				File arquivoMoldado = new File("chunk_"+idChunk+".part");
				FileOutputStream funil = new FileOutputStream(arquivoMoldado);
				funil.write(buffer);
				coordenadoremoto.registrarPosseChunk(idChunk, meuIP);
				System.out.println("Guardei o pedaço "+idChunk+" do arquivo com sucesso!");
				funil.close();
				
			}
				
				
			
			// criar um serverSocket aqui para recebimento. Socket normal serve mais para envio, mesmo que também receba atraves do datainputstream, mas se quisermos APENAS recebimento, SERVERSOCKET
			// essa variavel de interfaceRMI é como vamos fazer requisicoes para o servidor remotamente e conversar com eles por suas funcoes
			}catch (Exception e) {
            System.err.println("Erro no Nó Cliente: " + e.getMessage());
            e.printStackTrace();
	}
			
		
}
}
