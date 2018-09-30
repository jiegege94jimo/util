package priv.lzj.util;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;

import java.util.List;

public class HanFenci {
    public static String participleWord(String text) {
        List<Term> termList = HanLP.segment(text);
        String gang = "/";
        String res = "";
        for (Term txt : termList) {
            int subscrip = txt.toString().indexOf(gang) + 1;
            String word = txt.toString().substring(0, subscrip - 1);
            if(!word.equals(",")&&!word.equals("，")) {
                res = res + word + ",";
            }

        }
        if(res.length()>0){
            res = res.substring(0,res.length()-1);
        }
        return res;
    }
}
