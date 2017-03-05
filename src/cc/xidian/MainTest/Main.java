package cc.xidian.MainTest;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.Stack;

/**
 * Created by hadoop on 2017/2/16.
 */

class NetworkLinkInfo{
    public int beginNeworkNodeID;
    public int endNeworkNodeID;
    public int sumLinkBandWidth;
    public int perLeaseCharge;
    public NetworkLinkInfo(){}
    public NetworkLinkInfo(int beginNeworkNodeID,int endNeworkNodeID,int sumLinkBandWidth,int perLeaseCharge){
        this.beginNeworkNodeID = beginNeworkNodeID;
        this.endNeworkNodeID = endNeworkNodeID;
        this.sumLinkBandWidth = sumLinkBandWidth;
        this.perLeaseCharge = perLeaseCharge;
    }
    public String toString(){
        return this.beginNeworkNodeID + " " + this.endNeworkNodeID + " "
                +this.sumLinkBandWidth + " " + this.perLeaseCharge;
    }
}

class ConsumptionNodeInfo{
    public int consumptionNodeID;
    public int conNetworkNodeID;
    public int sumConsumptionBandWidth;

    public ConsumptionNodeInfo(){}
    public ConsumptionNodeInfo(int consumptionNodeID,int conNetworkNodeID,int sumConsumptionBandWidth){
        this.consumptionNodeID = consumptionNodeID;
        this.conNetworkNodeID = conNetworkNodeID;
        this.sumConsumptionBandWidth = sumConsumptionBandWidth;
    }
    public String toString(){
        return this.consumptionNodeID + " " + this.conNetworkNodeID + " " + sumConsumptionBandWidth;
    }
}

public class Main {
    public static final int MAXSUMNETWORKNODES = 1000;
    public static final int MAXSUMNETWORKLINKS = 10000;
    public static final int MAXSUMCONSUMPTIONNODES = 500;
    public static void main(String[] graphContent){
/**do your work here**/
        //-----lwd-----

        String[] strSumsArray = graphContent[0].split(" ");
        int sumNetworkNodes = Integer.parseInt(strSumsArray[0]);
        int sumNetworkLinks = Integer.parseInt(strSumsArray[1]);
        int sumConsumptionBandWidth = Integer.parseInt(strSumsArray[2]);
        System.out.println(sumNetworkNodes + "," + sumNetworkLinks + "," + sumConsumptionBandWidth);

        int costPerVideoServer = Integer.parseInt(graphContent[2]);
        System.out.println(costPerVideoServer);

        ArrayList<NetworkLinkInfo> networkLinkInfos = new ArrayList<NetworkLinkInfo>();
        //NetworkLinkInfo[][] networkLinkAdjacentMatrix;
        ArrayList<LinkedList<NetworkLinkInfo>> adjacentTable = new ArrayList<LinkedList<NetworkLinkInfo>>(1000);
        String[] strNetworkLinkArray;
        for (int i=4;i<sumNetworkLinks+4;i++){
            strNetworkLinkArray = graphContent[i].split(" ");
            NetworkLinkInfo n = new NetworkLinkInfo();
            n.beginNeworkNodeID = Integer.parseInt(strNetworkLinkArray[0]);
            n.endNeworkNodeID = Integer.parseInt(strNetworkLinkArray[1]);
            n.sumLinkBandWidth = Integer.parseInt(strNetworkLinkArray[2]);
            n.perLeaseCharge = Integer.parseInt(strNetworkLinkArray[3]);
            networkLinkInfos.add(n);
            //Building AdjacentTable
            if (adjacentTable.get(n.beginNeworkNodeID) == null){
                LinkedList<NetworkLinkInfo> linkedList = new LinkedList<NetworkLinkInfo>();
                linkedList.add(n);
                adjacentTable.add(linkedList);
            }else if (adjacentTable.get(n.endNeworkNodeID) == null){
                LinkedList<NetworkLinkInfo> linkedList = new LinkedList<NetworkLinkInfo>();
                linkedList.add(n);
                adjacentTable.add(linkedList);
            }else{
                adjacentTable.get(n.beginNeworkNodeID).add(n);
                adjacentTable.get(n.endNeworkNodeID).add(n);
            }
            //networkLinkAdjacentMatrix[n.beginNeworkNodeID][n.endNeworkNodeID] = n;

        }
        for(LinkedList<NetworkLinkInfo> linkedList : adjacentTable){
            for(NetworkLinkInfo n : linkedList){
                System.out.print(n.toString()+"#");
            }
            System.out.println();
        }
        //for (NetworkLinkInfo n : networkLinkInfos){
        //System.out.println(n.toString());
        //}

        ArrayList<ConsumptionNodeInfo> consumptionNodeInfos = new ArrayList<ConsumptionNodeInfo>();
        String[] strConsumptionNodeArray;
        for (int i=sumNetworkLinks+5;i<sumNetworkLinks+5+sumConsumptionBandWidth;i++){
            strConsumptionNodeArray = graphContent[i].split(" ");
            ConsumptionNodeInfo c = new ConsumptionNodeInfo();
            c.consumptionNodeID = Integer.parseInt(strConsumptionNodeArray[0]);
            c.conNetworkNodeID = Integer.parseInt(strConsumptionNodeArray[1]);
            c.sumConsumptionBandWidth = Integer.parseInt(strConsumptionNodeArray[2]);
            consumptionNodeInfos.add(c);
        }

    }
}
