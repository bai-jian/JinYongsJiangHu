package MapReduce;

import MapReduce.LabelPropagation.LabelPropagationDriver;
import MapReduce.PageRank.PageRankDriver;

/**
 * Created by lubuntu on 16-7-5.
 */
public class JinYongsJiangHu {

    public static void main(String[] args) {
        long tStart = System.currentTimeMillis();

        System.out.println(
                "**************************************************************" + '\n' +
                "********** Welcome to JinYong's JiangHu (MapReduce) **********" + '\n' +
                "***********************  金 庸 的 江 湖  **********************" + '\n' +
                "*** Analyse the relation of characters in JinYong's novels. **" + '\n' +
                "**************************************************************" + '\n'
        );

        if (args.length < 2) {
            // In Apache Hadoop MapReduce, <character_list> is in Linux FS while <novels> is in HDFS.
            // So, the two arguments have to be separated.
            System.err.println("usage: JinYong'sJiangHu <character_list> <novels> <out>");
            System.exit(2);
        }

        System.out.println("InteractionExtractor begins...");
        String[] args4InteractionExtractor = {args[0], args[1], args[2] + "/temp1"};
        InteractionExtractor.main(args4InteractionExtractor);
        System.out.println("InteractionExtractor finishes successfully...");

        System.out.println("CharacterCooccurrence begins...");
        String[] args4CharacterCooccurrence = {args[2] + "/temp1", args[2] + "/temp2"};
        CharacterCooccurrence.main(args4CharacterCooccurrence);
        System.out.println("CharacterCooccurrence finishes successfully...");

        System.out.println("CharacterRelationGraph begins...");
        String[] args4CharacterRelationGraph = {args[2] + "/temp2", args[2] + "/temp3"};
        CharacterRelationGraph.main(args4CharacterRelationGraph);
        System.out.println("CharacterRelationGraph finishes successfully...");

        System.out.println("PageRank begins...");
        String[] args4PageRank = {args[2] + "/temp3", args[2] + "/temp4.", args[2] + "/temp4", args[2] + "/temp6-1"};
        PageRankDriver.main(args4PageRank);
        System.out.println("PageRank finishes successfully...");

        System.out.println("LabelPropagation begins...");
        String[] args4LabelPropagation = {args[2] + "/temp3", args[2] + "/temp5.", args[2] + "/temp5", args[2] + "/temp6-2"};
        LabelPropagationDriver.main(args4LabelPropagation);
        System.out.println("LabelPropagation finishes successfully...");

        System.out.println(
                "**************************************************************" + '\n' +
                "*** Analyse the relation of characters in JinYong's novels. **" + '\n' +
                "***********************  金 庸 的 江 湖  **********************" + '\n' +
                "********** Good bye, JinYong's JiangHu (MapReduce) ***********" + '\n' +
                "**************************************************************" + '\n'
        );

        long tEnd = System.currentTimeMillis();
        long tDelta = tEnd - tStart;
        System.out.println("\nThe elapsed time is " + tDelta / 1000.0 + "s.\n");
    }
}
