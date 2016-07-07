package JinYongsJiangHu;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;


/**
 * Created by lubuntu on 16-7-5.
 */
public class Driver {

    public static void main(String[] args) {
        System.out.println(
                "Welcome to --" + '\n' +
                "********************* JinYong's JiangHu *********************" + '\n' +
                "-- Analyse the relation of characters in JinYong's novels. --" + '\n'
        );
        /*
        String[] args4InteractionExtractor = {"./input/people_name_list.txt", "./intput/novels", "./output"};
        InteractionExtractor.main(args4InteractionExtractor);
        */
        /*
        UserDefineLibrary.insertWord("ansj中文分词", "userDefine", 1000);
        Result results = ToAnalysis.parse("我觉得ansj中文分词是一个不错的系统!我是王婆!");
        for(Term term : results) {
            if (term.getNatureStr().equals("userDefine"))
                System.out.println(term.getName());
        }
        // System.out.println("增加新词例子:" + result.toString());
        */
        // String names = readNames("./input/people_name_list.txt");
        // System.out.println("names:" + names);
    }
}
