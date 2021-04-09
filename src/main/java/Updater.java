import io.ipfs.api.IPFS;
import io.ipfs.multihash.Multihash;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Quintet;
import org.javatuples.Triplet;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Updater extends Thread{
    IPFS ipfs;
    MyIPFSClass ipfsClass;
    double a = 0.6;


    public Multihash _Upload_File(List<Double> Weights, MyIPFSClass ipfsClass, String filename) throws IOException {
        //Serialize the Partition model into a file
        FileOutputStream fos = new FileOutputStream(filename);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(Weights);
        oos.close();
        fos.close();
        // Add file into ipfs system
        return  ipfsClass.add_file(filename);
    }

    /*
     * @todo Change Partiton by partition and use snake case.
     */
    /*
     * This function is invoked when a new Quintet is received from a Peer
     * and pulled out from the Queue in the main run() infinite loop.
     */
    public void _Update(List<Double> Gradient,int Partiton,String Origin,int iteration,boolean from_clients) throws InterruptedException {
        int i,counter = 0;
        double weight = PeerData.previous_iter_active_workers.get(Partiton);
        //Aggregate gradients from pub/sub
        if(!from_clients) {
            System.out.println("From pubsub");
            //System.out.println("GAMW TO SPITI");

            if(PeerData.isSynchronous){
                /*
                 * Acquire the semaphore over the current PeerData.mtx.
                 * This is used for ensuring no racing conditions occurs.
                 */
                PeerData.mtx.acquire();

                /*
                 * Origin refers to the peerId from which the Gradients were
                 * received.
                 */
                if(
                    /*
                     * If Replica of the node for the given partition number
                     * is present and we are at the synchronization iteration.
                     */
                    (PeerData.Replica_holders.get(Partiton).contains(Origin) &&
                     iteration == PeerData.middleware_iteration) || 
                    /*
                     * Or the origin peer is present in the new replicas for
                     * the current partition.
                     */
                    (PeerData.New_Replicas.get(Partiton).contains(Origin) && 
                     iteration == PeerData.middleware_iteration)){
                    /*
                     * Use this loop for going one by one through all the
                     * gradients and modifying the aggregated gradients
                     * of the peer partition.
                     */
                    for( i = 0; i < PeerData.Aggregated_Gradients.get(Partiton).size(); i++){
                        PeerData.Aggregated_Gradients.get(Partiton).set(i, PeerData.Aggregated_Gradients.get(Partiton).get(i) + Gradient.get(i));
                    }
                    /*
                     * Remove the flag for this iteration
                     */
                    PeerData.Replica_Wait_Ack.remove(new Triplet<>(Origin,Partiton,iteration));
                }
                else if(
                    /*
                     * If the origin peer is in the replica holders of its
                     * partition and the iteration number is higher than the
                     * middleware_iteration set for this peer
                     */
                    (PeerData.Replica_holders.get(Partiton).contains(Origin) 
                     && iteration > PeerData.middleware_iteration) || 
                    /*
                     * Or it is in the new replicas and the same condition
                     * is present for the iteration number.
                     */
                    (PeerData.New_Replicas.get(Partiton).contains(Origin) && 
                     iteration > PeerData.middleware_iteration)){
                    // Do something for replica holders only. Aggregate in future buffer
                    /*
                     * Add this element to the Replica_Wait_Ack_from_future,
                     * note that this is not the same as the prior
                     * Replica_Wait_Ack.
                     */
                    PeerData.Replica_Wait_Ack_from_future.add(new Triplet<>(Origin,Partiton,iteration));
                }

                /*
                 * Free the PeerData.mtx semaphore.
                 */
                PeerData.mtx.release();
            }
            else{
                /*
                 * In the case where there is no synchronization, then directly
                 * update the corresponding partition Weights by using
                 * a formula for weighted averaging.
                 *
                 * @todo Check if 0.25 should not go in front of the
                 * Gradient.get(i) phrase.
                 * Check the LeavingPeer weighted approach below.
                 */
                for (i = 0; i < PeerData.Weights.get(Partiton).size(); i++) {
                    PeerData.Weights.get(Partiton).set(i, 0.75*PeerData.Weights.get(Partiton).get(i) + Gradient.get(i));
                }
            }
            return;
        }
        //System.out.println(Origin + " , " + Origin==null + " , " + Origin.equals(null));
        //Aggregate weights from a leaving peer
        /*
         * If origin is from a leaving peer, then we should not wait for
         * synchronization for him, directly update the partition weights.
         *
         * It could be seen that here there is a weighted averaging process
         * where a and (1-a) are used as factor. This a value is equal to
         * 0.6 in the head of the current class definition.
         */
        else if(Origin.equals("LeavingPeer")){
            for(i = 0; i < PeerData.Weights.get(Partiton).size(); i++){
                PeerData.Weights.get(Partiton).set(i, a*PeerData.Weights.get(Partiton).get(i) + (1-a)*Gradient.get(i));
            }
        }
        //Aggregate gradients from other peers requests
        else if(from_clients) {
            /*
             * Red light in the PeerData.mtx semaphore.
             */
            PeerData.mtx.acquire();
          
            /*
             * Update the Gradients for the current partition and
             * conditionally update the weights if no synchronization
             * is had.
             */
            for (i = 0; i < PeerData.Weights.get(Partiton).size(); i++) {
                PeerData.Aggregated_Gradients.get(Partiton).set(i, PeerData.Aggregated_Gradients.get(Partiton).get(i) + Gradient.get(i));
                if(!PeerData.isSynchronous){
                    PeerData.Weights.get(Partiton).set(i, PeerData.Weights.get(Partiton).get(i) - Gradient.get(i)/weight);
                }
            }
            /*
             * If Origin is not in the set of workers, then just add it
             * there for the current peer partition.
             */
            if(!PeerData.workers.get(Partiton).contains(Origin)) {
                PeerData.workers.get(Partiton).add(Origin);
            }
            if(PeerData.isSynchronous){
                // In case a node is so fast sto that he sends the new gradients before the other can finish or if there is a new peer then keep the gradients in a buffer
                // and use them in the next iteration
                /*
                 * @todo Check what happens if this behavior happens twice.
                 * If a node sends three updates before the other can finish.
                 */
                if(
                    /*
                     * If data for the current synchronization phase is not
                     * had for the current peer and that peer is in the
                     * list of clients for the its partition.
                     */
                    (!PeerData.Client_Wait_Ack.contains(new Triplet<>(Origin,Partiton,iteration)) && 
                     PeerData.Clients.get(Partiton).contains(Origin)) ||
                    /*
                     * Or that same peer is in the new_clients vector.
                     */
                    (PeerData.New_Clients.get(Partiton).contains(Origin))) {
                    System.out.println("RECEIVED GRADIENTS FROM FUTURE ? :^)");
                    //System.out.println(PeerData.Client_Wait_Ack);
                    //System.out.println(new Triplet<>(Origin,Partiton,iteration));
                    /*
                     * Add the origin peer to the Client_Wait_Ack_from_future 
                     * vector.
                     *
                     * This is not the same than Replica_Wait_Ack_from_future 
                     */
                    PeerData.Client_Wait_Ack_from_future.add(new Triplet<>(Origin,Partiton,iteration));
                }
                else if(PeerData.Clients.containsKey(Origin) &&
                    PeerData.middleware_iteration < iteration){
                    /*
                     * If the number of iterations is less than the 
                     * synchronization iterations and the peer is in the list
                     * of the current node clients.
                     */
                    System.out.println("RECEIVED GRADIENTS FROM FUTURE ? :^)");
                    /* 
                     * Add to Client_Wait_Ack_from_future and remove from the
                     * Client_Wait_Ack if present.
                     */
                    PeerData.Client_Wait_Ack_from_future.add(new Triplet<>(Origin,Partiton,iteration));
                    for(int j = 0; j < PeerData.Client_Wait_Ack.size(); j++){
                        if(PeerData.Client_Wait_Ack.contains(new Triplet<>(Origin,Partiton,PeerData.middleware_iteration))){
                            PeerData.Client_Wait_Ack.remove(new Triplet<>(Origin,Partiton,PeerData.middleware_iteration));
                            break;
                        }
                    }
                }
                else{
                    /*
                     * We are not waiting anymore for the origin peer to send
                     * data for the current iteration.
                     */
                    PeerData.Client_Wait_Ack.remove(new Triplet<>(Origin,Partiton,iteration));
                }


            }
            /*
             * Green light for the PeerData.mtx semaphore.
             * mtx -> Mutex -> Mutually Exclusive
             */
            PeerData.mtx.release();

        }
     }


    /*
     * run() function contains the loop for processing queue elements
     * that other peers would add.
     *
     * This run() function is in the "Updater.java" file, which defines the
     * Updater thread class. This means that a new process is forked from the
     * main process, which runs the code shown below. This code contains
     * an "infinite" loop, which would live until this child process
     * get the termination signal.
     *
     * Some dark functions as "getValue1", "getValue2" and so on are used here.
     * These functions get the ordered components from the elements placed in
     * the PeerData.queue Queue. These are functions for grabbing the elements
     * of a Quintet element, which is nothing more than a Tuple with
     * five elements of distinct types.
     */
    public void run(){
        /*
         * Create a raw IPFS object for the given PeerData path
         */
        ipfs = new IPFS(PeerData.Path);
        /*
         * ipfsClass contains a decorated IPFS object. This class is defined
         * under the MyIPFSClass.java source file.
         * Internally, it creates an ipfsObj raw IPFS object as the one
         * created before, so theoretically, ipfs is a duplicated instance.
         */
        ipfsClass = new MyIPFSClass(PeerData.Path);
        int partition,iteration;
        boolean from_clients;
        List<Double> Gradient;
        String PeerId,reply;
        Quintet<String,Integer,Integer,Boolean, List<Double>> request;
        Multihash hash;
        try {
            /*
             * Infinite loop for the current thread.
             * 
             * This loop get elements out of the Queue using a blocking
             * function for that purpose (.take()).
             * Then, it extracts each one of the elements of the Quintet
             * pulled out of the Queue, assigning each one of them to a
             * variable which was previously allocated.
             */
            while (true) {
                request = PeerData.queue.take();
                partition = request.getValue1();
                Gradient = request.getValue4();
                from_clients = request.getValue3();
                PeerId = request.getValue0();
                iteration = request.getValue2();
                /*
                 * 
                 */
                _Update(Gradient, partition,PeerId,iteration,from_clients);

                /*
                 * @todo isBootsraper -> isBootstrapper
                 * That change has to be made at:
                 * GlobalGradientPool.java
                 * Model.java
                 * IPLS.java
                 * PeerData.java
                 * Updater.java
                 */
                /*
                 * If the current node is the Bootstrapper, then no more actions
                 * are performed.
                 */
                if(PeerData.isBootsraper){
                    continue;
                }
                if(
                    /*
                     * If peer id is present
                     */
                    PeerId != null && 
                    /*
                     * And it is not the same that the current node id
                     */
                    PeerId.equals(ipfs.id().get("ID").toString()) == false && 
                    /*
                     * And we do not use synchronization.
                     */
                    !PeerData.isSynchronous) {
                    /*
                     * Publish a message to the PeerId
                     *
                     * @todo Replace request.getValue1() by partition
                     */
                    ipfs.pubsub.pub(PeerId,ipfsClass.Marshall_Packet(PeerData.Weights.get(request.getValue1()),ipfs.id().get("ID").toString(),partition,(short)4));
                }
                else if(PeerId != null && !PeerData.isSynchronous){
                	for(int i = 0; i < PeerData.Aggregated_Gradients.get(partition).size(); i++) {
                		PeerData.Aggregated_Gradients.get(partition).set(i, 0.25*PeerData.Weights.get(partition).get(i));
                	}
                    if(!PeerData.isSynchronous){
                        ipfs.pubsub.pub(new Integer(partition).toString(),ipfsClass.Marshall_Packet(PeerData.Aggregated_Gradients.get(partition),ipfs.id().get("ID").toString(),partition,(short) 3));
                    }
                	//Clean Aggregated_Gradients vector
                    for(int i = 0; i < PeerData.Aggregated_Gradients.get(partition).size(); i++){
                        PeerData.Aggregated_Gradients.get(partition).set(i,0.0);
                    }
                }

            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}
