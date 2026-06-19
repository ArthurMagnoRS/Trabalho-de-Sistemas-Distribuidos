package Trabaio;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.io.File;
import java.util.ArrayList;
import java.util.Scanner;
import java.io.FileInputStream;
import java.io.IOException;
public class ServidorPrincipal {

		public static void main (String[] args) {
		
			try {
	            Registry registry;
	            
	            try {
	                // Tenta criar o Registry do zero
	                registry = LocateRegistry.createRegistry(1099);
	                System.out.println("Novo RMI Registry criado na porta 1099.");
	            } catch (ExportException e) {
	                // Se a porta já estiver em uso, ele captura o erro e apenas conecta no existente
	                registry = LocateRegistry.getRegistry(1099);
	                System.out.println("RMI Registry já estava rodando. Aproveitando o existente.");
	            }
				Coordenad cordenador = new Coordenad ();				
				registry.rebind("CoordenadorServidor", cordenador);
				System.out.println("Servidor RMI está online e aguardando conexões.");
				Scanner scanner = new Scanner(System.in);
				while (true) {
					System.out.println("---------//MENU//------------");
					System.out.println("Digite o caminho para o arquivo que quer compartilhar");
					System.out.println("Digite 'sair' para encerrar o servidor." );
					
					String input = scanner.nextLine();
					
					if (input.equalsIgnoreCase("sair")){
						System.out.println("Desligando servidor.");
						System.exit(0);
					}
					else {
						System.out.println("Preparando para compartilhar o arquivo "+input);
						System.out.println("Existem "+cordenador.getNosAtivos().size() + " nós disponíveis para compartilhar.");
					
					File arquivo = new File(input);
					cordenador.setNomeArquivo(arquivo.getName());
					try (FileInputStream funil = new FileInputStream(arquivo)){
					
					byte[] bufferArqv = new byte[1024*1024];
					System.out.println("Tamanho do arquivo em bytes: " + arquivo.length());
					int bytesLidos = 0;
					int idChunk = 0;
					ArrayList<String> nosAtivos = cordenador.getNosAtivos();
					System.out.println(nosAtivos);
					ArrayList<Integer> portaRespec = cordenador.getPortaNosAtivos();
					System.out.println(portaRespec);
					int numNosAtivos = nosAtivos.size();
					bytesLidos = funil.read(bufferArqv);
					while ((bytesLidos = funil.read(bufferArqv)) != -1) {
						int noAtual = idChunk % numNosAtivos;
						cordenador.enviarPartes(bufferArqv,nosAtivos.get(noAtual), portaRespec.get(noAtual), bytesLidos, idChunk);	
						idChunk++;
					}
					
					funil.close();
					System.out.println("Arquivo inteiro dividido e compartilhado com sucesso!");
					byte[] bufferAviso = new byte[0];
					for (int i=0;i<nosAtivos.size();i++) {
						cordenador.enviarPartes(bufferAviso, nosAtivos.get(i), portaRespec.get(i), idChunk, -1); 
						// mandando o idChunk no lugar dos bytesLidos pq ja mostra quandos chunks foram enviados
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				}
			}
				
				
			
			}catch (Exception e) {
	            e.printStackTrace();
			
		}
}
}
