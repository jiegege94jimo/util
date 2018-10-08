package priv.lzj.util;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;

import java.util.List;

/**
 *汉语言分词，输入待分词的String，返回由,分割的String
 * @author diaoye
 */
public class HanFenci {
    public static String participleWord(String text) {
        List<Term> termList = HanLP.segment(text);
        String gang = "/";
        StringBuffer res = new StringBuffer("");
        for (Term txt : termList) {
            int subscrip = txt.toString().indexOf(gang) + 1;
            String word = txt.toString().substring(0, subscrip - 1);
            if(!(",").equals(word)&&!("，").equals(word)) {
                res = res.append(word).append(",");
            }

        }
        if(res.length()>0){
            res.substring(0,res.length()-1);
            return res.toString();
        }
        return res.toString();
    }
}
