package impl;

import static impl.Leader.mainLoop;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class LocalExampleRun {
  public static void main(String[] args)
  {
       /*
        * We implemented a local example run.
        */

        System.out.println("**********************************************************************");
        System.out.println("* This executes a preconfigured example run.                         *");
        System.out.println("* 8 threads are spawned and execute the protocol.                    *");
        System.out.println("* The leader prints when it is elected.                              *");
        System.out.println("* All threads are stopped after one second.                          *");
        System.out.println("**********************************************************************");

        String localhost = "::1";
        
        // You are welcome to change the parameters
        int numOfThreads = 8;
        int basePort = 50000;
        int maxGapBetweenIds = 5;


        int[][] idAndPort = new int[numOfThreads][2];
        // populate ids and ports
        List<Integer> ids = new ArrayList<Integer>();
        Random rand = new Random(System.currentTimeMillis());
        
        int lastId = 0;
        for (int i = 0; i < numOfThreads; i += 1) {
          lastId += 1 + rand.nextInt(maxGapBetweenIds);
          ids.add(lastId);
        }
        Collections.shuffle(ids);
        for (int i = 0; i < numOfThreads; i += 1) {
          idAndPort[i][0] = ids.get(i);
          idAndPort[i][1] = basePort + i;
        }

        // create threads running the protocol
        Thread[] threads = new Thread[idAndPort.length];
        for (int i = 0; i < idAndPort.length; i += 1) {
          int id = idAndPort[i][0];
          int fromPort = idAndPort[i == 0 ? idAndPort.length-1 : i-1][1];
          int toPort = idAndPort[i][1]; 

          threads[i] = new Thread("T" + i){
            public void run(){
              mainLoop(localhost, id, fromPort, toPort);
            }
          };
        }

        // start all threads
        for(Thread t : threads) {
          t.start();
        }

        // wait 1 second
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ignored) { }

        // kill all threads
        for(Thread t : threads) {
          t.stop();
        }

        System.out.println("\nall servers stopped");
  }
}