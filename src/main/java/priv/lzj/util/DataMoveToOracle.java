package priv.lzj.util;

import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 向oracle插入数据，解决字段过长的问题
 */
public class DataMoveToOracle {
    Connection connection = null;
    String url = "jdbc:oracle:thin:@192.168.2.198:1521:orcl";
    String userName = "arask";
    String password = "arask1234";
    PreparedStatement stmt = null;
    public void initConnection(){
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            this.connection = DriverManager.getConnection(url, userName, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void initConnection(String url,String userName,String password){
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            this.connection = DriverManager.getConnection(url, userName, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void initStmt(){
        String sql = "insert into TX_ASR_FLOW_RECORD_LZJ values(?,?)";
        try {
            this.stmt = this.connection.prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void closeStmt(){
        try {
            this.stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void closeConnection(){
        try {
            this.connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void insertMessage(String message){
        String sp[] = message.split(",");
        try {
            Reader clobReader = new StringReader(message);
            Reader stringReader = new StringReader(sp[0]);
            this.stmt.setCharacterStream(1, stringReader, sp[0].length());
            this.stmt.setCharacterStream(2, clobReader, message.length());
            this.stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws SQLException {
        String text = "体育    体育,辽足签约危机引注册难关高层威逼利诱合同笑里藏刀新浪体育讯　2月24日，辽足爆发了集体拒签风波，不签合同，辽足的注册问题就很棘手。与往年一样，辽足总会遇到注册危机，但最后都是跌跌撞撞地蒙混过关。今年，辽宁宏运集团派遣在俱乐部的黄姓秘书和副总隋言在海埂遭到的不同于往年的抵抗。这次去签约，辽足的合同是备受质疑的焦点。他们用五年月薪1000元的合同困住了从预备队调上来的年轻球员，用恐吓和威逼迫使他们在合同上签字。这样的合同无疑是包身工般的合同，从20岁到25岁，这些球员年收入12000元，还不如做饭店服务员多。而球队的老队员，对于这种场面已经习以为常，他们空前团结地拒绝签字。老队员的合同虽然有每年增加20%的条款，但是决定性的前提，如果冲超不成功，工资将减半。这是一个典型的阴谋，以辽足俱乐部目前的各项工作进展，冲超可能性很小，也就是说，这样的合同是一个明摆着笑里藏刀的合同，表面上工资奖金有提高的可能，实际上又大幅削减，倒不如俱乐部站出来说不冲超了，主力球员的工资奖金减半，这也似乎符合中甲俱乐部的现状，而俱乐部偏偏要拿一个冲超的幌子来作秀。辽足从1995年降级到宏运接手之前，每年都遇到合同的问题，当年在海埂，球员们也曾经集体拒绝俱乐部的合同，很多球员一直到年底都没有签，只是如数拿工资和奖金。这种情况在1999年到2003年都有存在，但是那时候辽足的边缘型替补球员都能拿到一万块钱，而预备队球员的工资最高也有8000左右的。张曙光在辽足最困难的时候也曾经遇到过这种情况，一方面是要债的人到足协提供材料，一方面是球员们拒签，最后也是通过各种关系摆平危机，但前提必须答应给人钱。最差的，也是给当事人留下欠条，虽然当时换不上，但答应以后换上，这也给债权人一个底，实在不行可以通过法律程序来解决。而2008赛季，宏运集团在支付球队各项费用方面非常困难，虽然俱乐部老板一再强调有钱，但是支付起来却拖沓得很。在辽足保级的关键阶段，球队只有主力球员可以拿到工资，18人名单以外的人根本拿不到钱。在赛季结束之后，俱乐部勉强把2008年的工资补齐了，但比赛奖金却迟迟没有到位，累计有300万之多。而与此同时，离开辽足的球员孙峰、王超等球员多次索要工资未果，只能到足协求救，这其中还有离开辽足的教练赵发庆。据说，前任辽足总经理程鹏辉也到足协递交了材料，准备向辽足讨薪，但是24日这天程鹏辉否认了这个说法，他认为自己的工资奖金不成问题，他也相信宏运集团会妥善解决拖欠问题，“我觉得还没到那种程度呢。”离开辽足的很多教练，其中包括现在长春亚泰担任主教练的李树斌都认为，2009赛季是辽足冲超的最好机会，“因为今年中甲各个队的投入都不大，很多俱乐部都缺乏冲超的积极性，都在减缩开支，如果辽足真想冲超的话，2009赛季希望很大。”辽足冲超，拿什么冲？辽足自己都不知道！对于2009年的目标，辽足有些迷茫，虽然主力阵容保持得比较完整，但是俱乐部的投入随之锐减。很多时候俱乐部的员工都在感叹，钱从哪里来啊？最近一段时间，俱乐部又有几个员工离开，辽足成了真正的袖珍俱乐部。而这些人对来年冲超有多大信心，很难说。有人曾经感叹说：“我们必须喊中超，但是冲不冲得上去，那就看行动了。口号是必须要喊的……”喊口号谁都会，辽足也会喊，可是喊出来之后怎么做，宏运似乎有自己的打算了。这次签约行动就是一个开始。可以肯定的是，球员不签约，俱乐部拖欠的款项不支付，辽联赛管理做准备、尝试。这是一个进步。我认为是起到了积极作用。现在还有人在诟病投票问题(联赛委员会投票)。我要说这是管理的一大进步，我完全可以采用过去的方式，就是篮管中心做个决定，说谁进CBA谁不进。但投票的方式反映所有联赛参与者的意志，这是符合职业联赛发展要求的。 投票过程中出现的问题，是前进中的问题，不能说这个制度不好。我是体制的受益者，也受制于体制赋闲的李元伟常常会参加一些体育研讨会、学术沙龙，或是体育总局政策性课题的研究评审。在这些场合，李元伟坚持自己的改革主张，甚至比在任时更坚定。中国新闻周刊：很长时间以来，每次大赛都会引发对中国体育改革的一次讨论。李元伟：这实际上反映了北京奥运会以后，群众对于体育改革的呼声日益强烈，对体育价值的评判标准正在发生变化。探讨未来中国体育发展的模式到底是什么样，对于中国体育未来的发展是有积极作用的。旧的体制有历史存在的必然性。首先要肯定这个管理体制在中国体育发展的历史上起到过积极作用，做出过巨大贡献。但同时也要看到这种体制是脱胎于高度集中的计划经济，几十年没有大的改变。中国新闻周刊：上世纪90年代中期，中国体育的改革力度很大，但为什么突然停滞了？李元伟：当时的体育改革是迈出了比较大的步伐。比如，当时明确提出中国体育要走社会化、产业化的发展方向，当时提出全民健身计划、奥运争光计划。推出了体育彩票，大大缓解了资金不足的问题。尤其是推出运动项目管理体制改革，成立了众多的运动项目管理中心。当时非常明确，管理中心这种形式，是由事业单位向协会实体化过渡的产物。一系列改革取得了明显效果，但没有继续推行下去，重要一个原因就在于北京奥运会申办成功。进一步加强竞技体育优先发展的位置，从某种程度上大家可以理解，毕竟奥运第一次在中国举行，运动成绩当然非常关键。客观上体育管理体制改革就减缓了。中国新闻周刊：在现行的体制内，体育官员要改革，你认为可以做到什么程度？李元伟：我们工作的时候也是这种体制的受益者。这种高度集中的体制让你可以强有力地实现你的目标。但同时你也受制于这种体制。改革，只能在一定程度做，比如如果影响到奥运备战那就不行。这种体制也为项目领导人提供了一定的空间，关键看你对所领导的项目生存发展的环境怎么认识，对于运动项目发展规律怎么认识，同时有没有积极的进取心，有没有强烈的改变项目面貌的动机、决心。这非常关键。";
        DataMoveToOracle lzj = new DataMoveToOracle();
        lzj.initConnection();
        lzj.initStmt();
        lzj.insertMessage(text);
        lzj.closeStmt();
        lzj.closeConnection();
    }
}
