package Standalone;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.apache.commons.math3.util.Pair;

import java.io.*;
import java.util.*;

/**
 * Created by lubuntu on 16-7-13.
 */
public class JinYongsJiangHu {

    public static void main(String[] args) {
        long tStart = System.currentTimeMillis();

        System.out.println(
                "**************************************************************" + '\n' +
                "********** Welcome to JinYong's JiangHu (Standalone) *********" + '\n' +
                "***********************  金 庸 的 江 湖  **********************" + '\n' +
                "*** Analyse the relation of characters in JinYong's novels. **" + '\n' +
                "**************************************************************" + '\n'
        );

        boolean debug = true, timer = false;
        if (args.length < 3) {
            System.err.println("usage: JinYong'sJiangHu <character_list> <novels> <out> [<debug> <timer>]");
            System.exit(2);
        } else {
            if (args.length == 3) {
                debug = true;
                timer = false;
            }
            if (args.length == 4) {
                debug = Boolean.parseBoolean(args[3]);
                timer = false;
            }
            if (args.length == 5) {
                debug = Boolean.parseBoolean(args[3]);
                timer = Boolean.parseBoolean(args[4]);
            }
        }

        System.out.println("Character interaction list begins...");
        ArrayList<ArrayList<String>> cil = characterInteractionList(args[0], args[1]);
        if (debug) {
            System.out.println("Character interaction list prints below...");
            Iterator<ArrayList<String>> iter = cil.iterator();
            while (iter.hasNext()) {
                Iterator<String> jter = iter.next().iterator();
                while (jter.hasNext()) {
                    System.out.print(jter.next() + " ");
                }
                System.out.print("\n");
            }
        }
        System.out.println("Character interaction list finishes successfully...");

        System.out.println("Character co-occurrence matrix begins...");
        Map<Pair<String, String>, Integer> ccm = characterCooccurrenceMatrix(cil);
        if (debug) {
            System.out.println("Character co-occurrence matrix prints below...");
            for (Map.Entry<Pair<String, String>, Integer> e : ccm.entrySet()) {
                System.out.println("<" + e.getKey().getFirst() + "," + e.getKey().getSecond() + ">\t" + e.getValue());
            }
        }
        System.out.println("Character co-occurrence matrix finishes successfully...");

        System.out.println("Character relation graph begins...");
        Map<String, ArrayList<Pair<String, Double>>> crg = characterRelationGraph(ccm);
        if (debug) {
            System.out.println("Character relation graph prints below...");
            for (Map.Entry<String, ArrayList<Pair<String, Double>>> e : crg.entrySet()) {
                String key = e.getKey();
                System.out.print(key + ":");
                Iterator<Pair<String, Double>> iter1 = e.getValue().iterator();
                if (iter1.hasNext()) {
                    Pair pair = iter1.next();
                    System.out.print(pair.getFirst() + "," + pair.getSecond());
                }
                while (iter1.hasNext()) {
                    Pair pair = iter1.next();
                    System.out.print(";" + pair.getFirst() + "," + pair.getSecond());
                }
                System.out.print("\n");
            }
        }
        System.out.println("Character relation graph finishes successfully...");

        System.out.println("PageRank begins...");
        pageRank(crg, 10, 0.85, args[2]);
        System.out.println("PageRank finishes successfully...");

        System.out.println(
                "**************************************************************" + '\n' +
                "*** Analyse the relation of characters in JinYong's novels. **" + '\n' +
                "***********************  金 庸 的 江 湖  **********************" + '\n' +
                "********** Good bye, JinYong's JiangHu (Standalone) **********" + '\n' +
                "**************************************************************" + '\n'
        );
        
        if (timer) {
            long tEnd = System.currentTimeMillis();
            long tDelta = tEnd - tStart;
            System.out.println("\nThe elapsed time is " + tDelta / 1000.0 + ".\n");
        }
    }

    /**
     * Character Interaction List
     * @param novelsPath the path of novels
     * @param namesFile the file of character names
     * @return cil(character interaction list), ArrayList<ArrayList<String>>
     */
    private static ArrayList<ArrayList<String>> characterInteractionList(String namesFile, String novelsPath) {
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

    /**
     * Character Co-occurrence Matrix
     * @param cil Character Interaction List, ArrayList<ArrayList<String>>
     * @return ccm Character Co-occurrence Matrix, Map<Pair<String, String>, Integer>
     */
    private static Map<Pair<String, String>, Integer> characterCooccurrenceMatrix(ArrayList<ArrayList<String>> cil) {
        // Character Co-occurrence Matrix
        Map<Pair<String, String>, Integer> ccm = new HashMap<>();
        for(int i = 0; i < cil.size(); ++i) {
            ArrayList<String> list = cil.get(i);
            for(int j = 0; j < list.size(); ++j) {
                for(int k = j + 1; k < list.size(); ++k) {
                    if (!list.get(j).equals(list.get(k))) {
                        Pair<String, String> key = new Pair(list.get(j), list.get(k));
                        Integer value = ccm.get(key);
                        ccm.put(key, value == null ? 1 : value + 1);
                    }
                }
            }
        }
        return ccm;
    }

    /**
     * Character Relation Graph
     * @param ccm Character Co-occurrence Matrix, Map<Pair<String, String>, Integer>
     * @return crg Character Relation Graph, Map<String, ArrayList<Pair<String, Double>>>
     */
    private static Map<String, ArrayList<Pair<String, Double>>> characterRelationGraph(Map<Pair<String, String>, Integer> ccm) {
        Map<String, ArrayList<Pair<String, Double>>> crg = new HashMap();
        for(Map.Entry<Pair<String, String>, Integer> e : ccm.entrySet()) {
            String key1 = e.getKey().getFirst();
            String key2 = e.getKey().getSecond();
            int value = e.getValue();
            ArrayList<Pair<String, Double>> list;
            // Put <key1, <key2, value>>
            list = crg.get(key1);
            if (list == null)
                list = new ArrayList();
            list.add(new Pair(key2, new Double(value)));
            crg.put(key1, list);
            // Put <key2, <key1, value>>
            list = crg.get(key2);
            if (list == null)
                list = new ArrayList();
            list.add(new Pair(key1, new Double(value)));
            crg.put(key2, list);
        }
        // the normalization of edge weight
        for(Map.Entry<String, ArrayList<Pair<String, Double>>> e : crg.entrySet() ) {
            ArrayList<Pair<String, Double>> value = e.getValue();
            double sum = 0;
            for(Pair<String, Double> pair : value)
                sum += pair.getSecond();
            for(int i = 0; i < value.size(); ++i) {
                String first = value.get(i).getFirst();
                Double second = value.get(i).getSecond();
                Pair pair = new Pair(first, second / sum);
                value.set(i, pair);
            }
        }
        return crg;
    }

    /**
     * Page Rank
     * @param crg Character Relation Graph, ArrayList<Pair<String,Double>>>
     * @param times the times of iteration
     * @param outputPath the name of output path
     */
    private static void pageRank(Map<String, ArrayList<Pair<String,Double>>> crg, int times, double d, String outputPath) {
        // GraphBuilder
        Map<String, ArrayList<String>> graph = new HashMap();
        for(Map.Entry<String, ArrayList<Pair<String, Double>>> e : crg.entrySet()) {
            String key = e.getKey();
            ArrayList<Pair<String, Double>> value = e.getValue();
            ArrayList<String> valueOut = new ArrayList();
            for(Pair<String, Double> pair : value) {
                valueOut.add(pair.getFirst());
            }
            graph.put(key, valueOut);
        }
        // PageRankIter
        Map<String, Double> pageRank = new HashMap();
        for(String key : graph.keySet()) {
            pageRank.put(key, 1.0);
        }
        for(int i = 0; i < times; ++i) {
            for(String key : graph.keySet()) {
                double pr = 1 - d;
                for(String target : graph.get(key)) {
                    pr += pageRank.get(target) / graph.get(target).size();
                }
                pageRank.put(key, pr);
            }
        }
        // PageRankViewer
        List<Map.Entry<String, Double>> list = new ArrayList(pageRank.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
            public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                return - o1.getValue().compareTo(o2.getValue());
            }
        });
        // Output the result into the given file
        try {
            PrintWriter writer = new PrintWriter(outputPath + "/PageRank", "UTF-8");
            for(Map.Entry<String, Double> e : list) {
                writer.println(e.getKey() + "\t" + e.getValue());
            }
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
