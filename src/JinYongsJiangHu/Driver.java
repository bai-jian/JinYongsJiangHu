package JinYongsJiangHu;

import JinYongsJiangHu.LabelPropagation.LabelPropagationDriver;
import JinYongsJiangHu.PageRank.PageRankDriver;

/**
 * Created by lubuntu on 16-7-5.
 */
public class Driver {

    public static void main(String[] args) {
        System.out.println(
                "**************************************************************" + '\n' +
                "********** Welcome to JinYong's JiangHu (MapReduce) **********" + '\n' +
                "***********************  金 庸 的 江 湖  **********************" + '\n' +
                "*** Analyse the relation of characters in JinYong's novels. **" + '\n' +
                "**************************************************************" + '\n'
        );

        if (args.length < 2) {
            System.err.println("usage: JinYong'sJiangHu <in> <out>");
            System.exit(2);
        }

        System.out.println("InteractionExtractor begins...");
        String[] args4InteractionExtractor = {args[0] + "/people_name_list.txt", args[0] + "/novels", args[1] + "/temp1"};
        InteractionExtractor.main(args4InteractionExtractor);
        System.out.println("InteractionExtractor finishes successfully...");

        System.out.println("CharacterCooccurrence begins...");
        String[] args4CharacterCooccurrence = {args[1] + "/temp1", args[1] + "/temp2"};
        CharacterCooccurrence.main(args4CharacterCooccurrence);
        System.out.println("CharacterCooccurrence finishes successfully...");

        System.out.println("CharacterRelationGraph begins...");
        String[] args4CharacterRelationGraph = {args[1] + "/temp2", args[1] + "/temp3"};
        CharacterRelationGraph.main(args4CharacterRelationGraph);
        System.out.println("CharacterRelationGraph finishes successfully...");

        System.out.println("PageRank begins...");
        String[] args4PageRank = {args[1] + "/temp3", args[1] + "/temp4.", args[1] + "/temp4", args[1] + "/temp6-1"};
        PageRankDriver.main(args4PageRank);
        System.out.println("PageRank finishes successfully...");

        System.out.println("LabelPropagation begins...");
        String[] args4LabelPropagation = {args[1] + "/temp3", args[1] + "/temp5.", args[1] + "/temp5", args[1] + "/temp6-2"};
        LabelPropagationDriver.main(args4LabelPropagation);
        System.out.println("LabelPropagation finishes successfully...");
    }
}
