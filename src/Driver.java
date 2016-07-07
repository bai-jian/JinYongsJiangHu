
import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by lubuntu on 16-7-5.
 */
public class Driver {

    /**
     * Read from the file and get the name list string, separated with colon
     * @param fileName  the name of the people_name_list file
     * @return the string of names, seperated with ','
     */
    private static String readNames(String fileName) {
        StringBuilder names = new StringBuilder("");
        int num = 0;  // the number of names in the file
        File file = new File(fileName);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = reader.readLine();
            names.append(line.trim());
            ++num;
            while((line = reader.readLine()) != null) {
                names.append(',' + line.trim());
                ++num;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("The number of names is " + num + ".");
        return names.toString();
    }

    public static void main(String[] args) {
        System.out.println("Hello, World!");
        /*
        UserDefineLibrary.insertWord("ansj中文分词", "userDefine", 1000);
        Result results = ToAnalysis.parse("我觉得ansj中文分词是一个不错的系统!我是王婆!");
        for(Term term : results) {
            if (term.getNatureStr().equals("userDefine"))
                System.out.println(term.getName());
        }
        // System.out.println("增加新词例子:" + result.toString());
        */
        String names = readNames("./input/people_name_list.txt");
        System.out.println("names:" + names);
    }
}
