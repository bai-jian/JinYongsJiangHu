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

        String[] args4InteractionExtractor = {"./input/people_name_list.txt", "./input/novels", "./output"};
        InteractionExtractor.main(args4InteractionExtractor);
    }
}
