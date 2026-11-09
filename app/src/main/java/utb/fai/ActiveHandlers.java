package utb.fai;

import java.util.*;

public class ActiveHandlers {
    //private static final long serialVersionUID = 1L;
    private HashSet<SocketHandler> activeHandlersSet = new HashSet<SocketHandler>();

    /**
     * sendMessageToAll - Pole zprávu vem aktivním klientùm kromì sebe sama
     * 
     * @param sender  - reference odesílatele
     * @param message - øetìzec se zprávou
     */

    synchronized void sendMessageToGroups(SocketHandler sender, String message) {
        for (SocketHandler handler : activeHandlersSet) {
            if (handler == sender || handler.name == null)
                continue;
            if (sharesGroup(sender, handler))
                deliver(handler, message);
        }
    }
/*
    synchronized void sendMessageToAll(SocketHandler sender, String message) {
        for (SocketHandler handler : activeHandlersSet) // pro vechny aktivní handlery
            if (handler != sender) {
                if (!handler.messages.offer(message)) // zkus pøidat zprávu do fronty jeho zpráv
                    System.err.printf("Client %s message queue is full, dropping the message!\n", handler.clientID);
            }
    }
*/
    /** Pošle soukromou zprávu klientovi se zadaným jménem. */
    synchronized boolean sendPrivate(String targetName, String message) {
        SocketHandler target = findByName(targetName);
        if (target == null)
            return false;
        deliver(target, message);
        return true;
    }
 
    private boolean sharesGroup(SocketHandler a, SocketHandler b) {
        for (String g : a.groups)
            if (b.groups.contains(g))
                return true;
        return false;
    }
 
    private void deliver(SocketHandler handler, String message) {
        if (!handler.messages.offer(message))
            System.err.printf("Fronta zprav je plna!\n", handler.clientID);
    }
 
    synchronized SocketHandler findByName(String name) {
        for (SocketHandler handler : activeHandlersSet)
            if (name.equals(handler.name))
                return handler;
        return null;
    }
 
    /**
     * Atomicky zkontroluje unikátnost jména a nastaví ho.
     * 
     * @return true pokud se jméno podařilo nastavit, false pokud je již obsazené
     */
    synchronized boolean trySetName(SocketHandler handler, String name) {
        SocketHandler existing = findByName(name);
        if (existing != null && existing != handler)
            return false;
        handler.name = name;
        return true;
    }

    /**
     * add pøidá do mnoziny aktivních handlerù nový handler.
     * Metoda je sychronizovaná, protoze HashSet neumí multithreading.
     * 
     * @param handler - reference na handler, který se má pøidat.
     * @return true if the set did not already contain the specified element.
     */
    synchronized boolean add(SocketHandler handler) {
        return activeHandlersSet.add(handler);
    }

    /**
     * remove odebere z mnoziny aktivních handlerù nový handler.
     * Metoda je sychronizovaná, protoze HashSet neumí multithreading.
     * 
     * @param handler - reference na handler, který se má odstranit
     * @return true if the set did not already contain the specified element.
     */
    synchronized boolean remove(SocketHandler handler) {
        return activeHandlersSet.remove(handler);
    }
}
