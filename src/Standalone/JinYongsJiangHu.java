package Standalone;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by lubuntu on 16-7-13.
 */
public class JinYongsJiangHu {

    public static void main(String[] args) {
        System.out.println(
                "**************************************************************" + '\n' +
                "********** Welcome to JinYong's JiangHu (Standalone) *********" + '\n' +
                "***********************  金 庸 的 江 湖  **********************" + '\n' +
                "*** Analyse the relation of characters in JinYong's novels. **" + '\n' +
                "**************************************************************" + '\n'
        );

        System.out.println("Character interaction list begins...");
        ArrayList<ArrayList<String>> cil = characterInteractionList("./input/novels", "./input/people_name_list.txt");
        // ArrayList<ArrayList<String>> cil = characterInteractionList("./testInput/novels", "./testInput/people_name_list.txt");
        /* System.out.println("Character interaction list prints below...");
        Iterator<ArrayList<String>> iter = cil.iterator();
        while(iter.hasNext()) {
            Iterator<String> jter = iter.next().iterator();
            while(jter.hasNext()) {
                System.out.print(jter.next() + " ");
            }
            System.out.print("\n");
        } */
        System.out.println("Character interaction list finishes successfully...");


    }

    /**
     * Character Interaction List
     * @param novelsPath the path of novels
     * @param namesFile the file of character names
     * @return cil(character interaction list), a 2D ArrayList
     */
    private static ArrayList<ArrayList<String>> characterInteractionList(String novelsPath, String namesFile) {
        // Insert the names into the dictionary
        File file = new File(namesFile);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            int num = 0;  // the number of names in the file
            while((line = reader.readLine()) != null) {
                UserDefineLibrary.insertWord(line, "characters", 1000);
                ++num;
            }
            System.out.println("The number of names is " + num + ".");
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Character interaction list
        ArrayList<ArrayList<String>> cil = new ArrayList<>();
        File[] novels = new File(novelsPath).listFiles();
        for(File novel: novels) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(novel));
                String line;
                while((line = reader.readLine()) != null) {
                    ArrayList<String> names = new ArrayList<>();
                    Result result = DicAnalysis.parse(line);
                    for(Term term : result) {
                        if (term.getNatureStr().equals("characters"))
                            names.add(term.getName());
                    }
                    if (names.size() >= 2)
                        cil.add(names);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return cil;
    }
}
