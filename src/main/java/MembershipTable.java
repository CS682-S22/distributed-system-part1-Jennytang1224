import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class MembershipTable {
    int id;
    MemberInfo memberInfo;
    ConcurrentHashMap<Integer, MemberInfo> membershipTable = new ConcurrentHashMap<>();


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }


    // add a new broker to the table
    public void put(int id, MemberInfo memberInfo){
        if(membershipTable.size() != 0) {
            int newId = createHighestId();
//            System.out.println("new Id: " + newId);
            membershipTable.put(newId, memberInfo);
        }else{
            membershipTable.put(id, memberInfo);
//            System.out.println("leader new Id: " + id);

        }

    }


    // mark a living broker as dead from the table
    public void markDead(int id){
        membershipTable.get(id).setAlive(false);
    }


    // mark new leader
    public void switchLeaderShip(int oldLeaderId, int newLeaderId){
        membershipTable.get(oldLeaderId).setLeader(false);
        membershipTable.get(oldLeaderId).setAlive(false);
        membershipTable.get(newLeaderId).setLeader(true);
    }


    // create a highest id for the new item in the table
    public int createHighestId(){
        int lastIdx = membershipTable.size();
        return ((int) membershipTable.keySet().toArray()[lastIdx-1] + 1);
    }

    //get member info by id
    public MemberInfo getMemberInfo(int id){
        return membershipTable.get(id);
    }

    public int size(){
        return membershipTable.size();
    }

    public int getLeaderID(){
        int leaderId = -1;
        for(int key: membershipTable.keySet()) {
            memberInfo = membershipTable.get(key);
            if(memberInfo.isLeader){
                leaderId = key;
                break;
            }
        }
        return leaderId;
    }

    public void print() {
        for(int key: membershipTable.keySet()){
            memberInfo = membershipTable.get(key);
            System.out.println(
                    "MembershipTable = " + key + ": "
                            + memberInfo.getHostName()
                            + " " + memberInfo.getPort()
                            + " " + memberInfo.isLeader
                            + " " + memberInfo.isAlive) ;

        }

    }
}
