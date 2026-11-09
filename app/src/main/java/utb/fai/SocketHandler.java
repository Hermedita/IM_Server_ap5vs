package utb.fai;

import java.io.*;
import java.net.*;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.*;

public class SocketHandler {
	volatile String name = null;
	Set<String> groups = ConcurrentHashMap.newKeySet();
	/** mySocket je socket, o který se bude tento SocketHandler starat */
	Socket mySocket;

	/** client ID je retìzec ve formátu <IP_adresa>:<port> */
	String clientID;

	/**
	 * activeHandlers je reference na mnozinu vsech právì bìzících SocketHandlerù.
	 * Potrebujeme si ji udrzovat, abychom mohli zprávu od tohoto klienta
	 * poslat vsem ostatním!
	 */
	ActiveHandlers activeHandlers;

	/**
	 * messages je fronta príchozích zpráv, kterou musí mít kazý klient svoji
	 * vlastní - pokud bude je pretízená nebo nefunkcní klientova sít,
	 * cekají zprávy na dorucení právì ve frontì messages
	 */
	ArrayBlockingQueue<String> messages = new ArrayBlockingQueue<String>(20);

	/**
	 * startSignal je synchronizacní závora, která zarizuje, aby oba tasky
	 * OutputHandler.run() a InputHandler.run() zacaly ve stejný okamzik.
	 */
	CountDownLatch startSignal = new CountDownLatch(2);

	/** outputHandler.run() se bude starat o OutputStream mého socketu */
	OutputHandler outputHandler = new OutputHandler();
	/** inputHandler.run() se bude starat o InputStream mého socketu */
	InputHandler inputHandler = new InputHandler();
	/**
	 * protoze v outputHandleru nedovedu detekovat uzavrení socketu, pomùze mi
	 * inputFinished
	 */
	volatile boolean inputFinished = false;

	public SocketHandler(Socket mySocket, ActiveHandlers activeHandlers) {
		this.mySocket = mySocket;
		clientID = mySocket.getInetAddress().toString() + ":" + mySocket.getPort();
		this.activeHandlers = activeHandlers;
		groups.add("public");
	}

	// reply from server
	private void reply(String text) {
		if (!messages.offer(text))
			System.err.printf("Fronta zprav je plna!\n", clientID);
	}

	private void processLine(String request) {
		String[] parts = request.split("\\s+", 2);
		String vstup = parts[0];
		String arg = parts.length > 1 ? parts[1].trim() : "";
 
		// kdyz klient jeste nema jmeno
		if (name == null) {
			if (vstup.equals("#setMyName"))
				setName(arg);
			else if (vstup.startsWith("#"))
				reply("Prvni si nastavte jmeno:");
			else
				setName(request); // jméno bez příkazu #setMyName
			return;
		}
 
		switch (vstup) {
			case "#setMyName":
				setName(arg);
				break;
			case "#sendPrivate":
				sendPrivate(arg);
				break;
			case "#join":
				join(arg);
				break;
			case "#leave":
				leave(arg);
				break;
			case "#groups":
				reply(String.join(",", new TreeSet<String>(groups)));
				break;
			default:
				if (vstup.startsWith("#"))
					reply("Neznamy command: " + vstup);
				else
					activeHandlers.sendMessageToGroups(this, "[" + name + "] >> " + request);
		}
	}
 
	private void setName(String candidate) {
		if (candidate.isEmpty() || candidate.matches(".*\\s.*")) { 
			//.* = libovolny pocet znaku
			//\s = 1 whitespace
			reply("Jmeno nesmi byt prazdne a nebo s mezerami. Vymyslete si jine:");
		} else if (!activeHandlers.trySetName(this, candidate)) {
			reply("Jmeno '" + candidate + "' je jiz zabrano. Vymyslete si jine:");
		} else {
			reply("Vase jmeno je: " + candidate);
		}
	}
 
	private void sendPrivate(String arg) {
		String[] p = arg.split("\\s+", 2);
		//\s = 1 whitespace
		//+ = za sebou
		if (p.length < 2 || p[0].isEmpty() || p[1].trim().isEmpty()) {
			reply("Argument #sendPrivate: <name> <message>");
			return;
		}
		if (!activeHandlers.sendPrivate(p[0], "[" + name + "] >> " + p[1].trim()))
			reply("User '" + p[0] + "' nenalezen.");
	}
 
	private void join(String room) {
		if (room.isEmpty() || room.matches(".*\\s.*")) {
			//.* = libovolny pocet znaku
			//\s = 1 whitespace
			reply("Argument #join: <room>");
			return;
		}
		if (groups.add(room))
			reply("Vstoupili jste do mistnosti " + room);
		else
			reply("Jiz jste v mistnosti " + room);
	}
 
	private void leave(String room) {
		if (room.isEmpty() || room.matches(".*\\s.*")) {
			//.* = libovolny pocet znaku
			//\s = 1 whitespace
			reply("Argument #leave: <room>");
			return;
		}
		if (groups.remove(room))
			reply("Opustili jste mistnost " + room);
		else
			reply("Nejste v mistnosti " + room);
	}

	class OutputHandler implements Runnable {
		public void run() {
			OutputStreamWriter writer;
			try {
				System.err.println("DBG>Output handler starting for " + clientID);
				startSignal.countDown();
				startSignal.await();
				System.err.println("DBG>Output handler running for " + clientID);
				writer = new OutputStreamWriter(mySocket.getOutputStream(), "UTF-8");
				writer.write("\nYou are connected from " + clientID + "\n");
				writer.flush();
				while (!inputFinished) {
					String m = messages.take();// blokující ctení - pokud není ve frontì zpráv nic, uspi se!
					writer.write(m + "\r\n"); // pokud nìjaké zprávy od ostatních máme,
					writer.flush(); // posleme je nasemu klientovi
					System.err.println("DBG>Message sent to " + clientID + ":" + m + "\n");
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.err.println("DBG>Output handler for " + clientID + " has finished.");

		}
	}

	class InputHandler implements Runnable {
		public void run() {
			try {
				System.err.println("DBG>Input handler starting for " + clientID);
				startSignal.countDown();
				startSignal.await();
				System.err.println("DBG>Input handler running for " + clientID);
				String request = "";
				/**
				 * v okamziku, kdy nás Thread pool spustí, pridáme se do mnoziny
				 * vsech aktivních handlerù, aby chodily zprávy od ostatních i nám
				 */
				//activeHandlers.add(SocketHandler.this);
				reply("Zadejte sve jmeno: ");
				BufferedReader reader = new BufferedReader(new InputStreamReader(mySocket.getInputStream(), "UTF-8"));
				while ((request = reader.readLine()) != null) { 
					request = request.trim();
					if (request.isEmpty())
						continue;
					
					// prisla od mého klienta nìjaká zpráva?
					// ano - posli ji vsem ostatním klientùm
					//request = "From client " + clientID + ": " + request;
					System.out.println("From client " + clientID + ": " + request);
					//activeHandlers.sendMessageToGroups(SocketHandler.this, request);
				}
				inputFinished = true;
				messages.offer("OutputHandler, wakeup and die!");
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				// remove yourself from the set of activeHandlers
				synchronized (activeHandlers) {
					activeHandlers.remove(SocketHandler.this);
				}
			}
			System.err.println("DBG>Input handler for " + clientID + " has finished.");
		}

	}
}
