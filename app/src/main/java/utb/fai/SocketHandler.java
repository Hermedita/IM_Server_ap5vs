package utb.fai;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class SocketHandler {
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
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
				activeHandlers.add(SocketHandler.this);
				BufferedReader reader = new BufferedReader(new InputStreamReader(mySocket.getInputStream(), "UTF-8"));
				while ((request = reader.readLine()) != null) { // prisla od mého klienta nìjaká zpráva?
					// ano - posli ji vsem ostatním klientùm
					request = "From client " + clientID + ": " + request;
					System.out.println(request);
					activeHandlers.sendMessageToAll(SocketHandler.this, request);
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
