package Trabaio;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;

public class cliente {

	public static void main (String[] args) {
		try {
			String ipServer = "";
			// ========================================================
						// PARA TESTAR MÚLTIPLOS NÓS, MUDA APENAS A PORTA!
						// O IP é sempre 127.0.0.1 no macOS (outros loopbacks não existem)
						// Nó 1: minhaPorta = 6000
						// Nó 2: minhaPorta = 6001
						// Nó 3: minhaPorta = 6002
						// ========================================================
						String meuIP = "";
						int minhaPorta = 5000;
						int portaServer = 1099;
						
						
						File pastaNode = new File("No_" + minhaPorta);
						if (!pastaNode.exists()) {
							pastaNode.mkdir(); // Cria a pasta se não existir
						}
			
			ServerSocket soqueteNode = new ServerSocket(minhaPorta);
			//Scanner scanner_ = new Scanner(System.in);
			//System.out.println("Digite o endereço Ip do servidor: ");
			//String ipServer = scanner_.nextLine();
			Registry registry = LocateRegistry.getRegistry(ipServer, portaServer);
			InterfaceRMI coordenadoremoto = (InterfaceRMI) registry.lookup("CoordenadorServidor");
			System.out.println("Conectando ao registro RMI em: " + ipServer);
			coordenadoremoto.NoduloAtivo(meuIP, minhaPorta);
			System.out.println("Conexão efetuada com sucesso! Carregando demais configurações e aceitando envios!");
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
							List<Integer> meusChunks = coordenadoremoto.buscarChunkDono(meuIP + ":" + minhaPorta); // aqui estao os chunks que preciso fazer o download.
							System.out.println("Possuo "+meusChunks.size()+" chunks: "+meusChunks);
							
							Scanner scanner = new Scanner(System.in);
							System.out.println("//////// AGORA, DIGITE 'SIM' PARA COMEÇAR A REQUISITAR O ARQUIVO INTEIRO");
							String input = scanner.nextLine();
							if (input.equalsIgnoreCase("sim")) {
								for (int i = 0; i<totalDeChunks;i++) {
									if (meusChunks.contains(i)) {
										continue;
									}
									String dono = coordenadoremoto.buscarDonoChunk(i); // formato "IP:porta"
									String[] partes = dono.split(":");
									String donoIp = partes[0];
									int donoPorta = Integer.parseInt(partes[1]);
									Socket envioReq = new Socket(donoIp, donoPorta);
									DataOutputStream dos = new DataOutputStream(envioReq.getOutputStream());
									DataInputStream disResposta = new DataInputStream(envioReq.getInputStream());
									dos.writeInt(-2); // flag de requisicao do chunk
									dos.writeInt(i); // chunk especifico
									int tamResp = disResposta.readInt();
									byte[] buffer = new byte[tamResp];
									disResposta.readFully(buffer); // leitura dos bytes enviados
									// logica de envio acima, e pelo proprio socket aberto envioReq, vamos esperar o que falta também.
									File arqvMolde = new File(pastaNode, "chunk_"+i+".part");
									FileOutputStream funil = new FileOutputStream(arqvMolde);
									
									funil.write(buffer);
									funil.close();
									envioReq.close();
								}
								// parte de reconstrucao do arquivo aqui
									String nomeArqv = coordenadoremoto.getNomeArquivo();
									File arquivoTotal = new File (pastaNode, "Reconstruido_"+nomeArqv);
									try (FileOutputStream funil = new FileOutputStream(arquivoTotal, true)){
										for (int j = 0;j<totalDeChunks;j++) {
											File chunkLocal = new File (pastaNode,"chunk_"+j+".part");
											try (FileInputStream canudo = new FileInputStream(chunkLocal)){
												byte[] buffer = canudo.readAllBytes();
												funil.write(buffer);
												canudo.close();
												System.out.println("Parte "+j+" do arquivo anexada!");
											}catch (IOException e) {
												e.printStackTrace();
											}
										}
										
										funil.close();
										System.out.println("Arquivo reconstruído para este nó!");
									}catch (IOException e) {
										e.printStackTrace();
									}
								
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}).start();
					
					continue;
					
				} else if (idChunk == -2) { // recebimento de PEDIDO DE ENVIO DE CHUNKS PARA OUTRO NO
					int chunkReq = dis.readInt();
					System.out.println("Um dos nós requisitou o chunk "+chunkReq);
					File chunkLocal = new File(pastaNode, "chunk_"+chunkReq+".part");
					if (chunkLocal.exists()) {
						try(FileInputStream canudo = new FileInputStream(chunkLocal)){
							byte[] bufferChunk = canudo.readAllBytes();
							
							DataOutputStream envio = new DataOutputStream(recebimento.getOutputStream());
							envio.writeInt((int) chunkLocal.length()); // primeiro o tamanho
							envio.write(bufferChunk); // agora o chunk de fato
							canudo.close();
							envio.close();
							recebimento.close();
							System.out.println("Envio feito com sucesso!");
						} catch (IOException e) {
							e.printStackTrace();
					}
				}
					continue;
			} else if (idChunk == -3) { // ping do servidor - responde ack e continua
				DataOutputStream ack = new DataOutputStream(recebimento.getOutputStream());
				ack.writeInt(1);
				ack.flush();
				recebimento.close();
				continue;
			} else {
				int tamArqv = dis.readInt(); // dps o tamanho
				byte[] buffer = new byte[tamArqv]; // agr o buffer
				dis.readFully(buffer);
				File arquivoMoldado = new File(pastaNode, "chunk_"+idChunk+".part");
				FileOutputStream funil = new FileOutputStream(arquivoMoldado);
				funil.write(buffer);
				coordenadoremoto.registrarPosseChunk(idChunk, meuIP + ":" + minhaPorta);
				System.out.println("Guardei o pedaço "+idChunk+" do arquivo com sucesso!");
				funil.close();
				recebimento.close();
			}
			}
				
				
			
			// criar um serverSocket aqui para recebimento. Socket normal serve mais para envio, mesmo que também receba atraves do datainputstream, mas se quisermos APENAS recebimento, SERVERSOCKET
			// essa variavel de interfaceRMI é como vamos fazer requisicoes para o servidor remotamente e conversar com eles por suas funcoes
			}catch (Exception e) {
            System.err.println("Erro no Nó Cliente: " + e.getMessage());
            e.printStackTrace();
	}
			
		
}
}
