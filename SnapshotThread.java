public class SnapshotThread extends Thread{
    int snapshotdelay;
    Node node0;
    public SnapshotThread(int snapshotdelay, Node node0){
        this.snapshotdelay = snapshotdelay;
        this.node0 = node0;
    }

    public void run(){
        while (!node0.terminate.get()) {
            try {
                Thread.sleep(snapshotdelay);
                if (!node0.endSnapshot.get()){
                    continue;
                }
                node0.markerActions(-1, null);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            }
        }
        System.out.println("broke out of loop");
    }
}
