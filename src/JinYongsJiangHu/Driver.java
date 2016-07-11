package JinYongsJiangHu;

import JinYongsJiangHu.PageRank.PageRankDriver;

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
        System.out.println("InteractionExtractor begins...");
        String[] args4InteractionExtractor = {"./input/people_name_list.txt", "./input/novels", "./temp/temp1"};
        // String[] args4InteractionExtractor = {"./testInput/people_name_list.txt", "./testInput/novels", "./temp/temp1"};
        InteractionExtractor.main(args4InteractionExtractor);
        System.out.println("InteractionExtractor finishes successfully...");

        System.out.println("CharacterCooccurrence begins...");
        String[] args4CharacterCooccurrence = {"./temp/temp1", "./temp/temp2"};
        CharacterCooccurrence.main(args4CharacterCooccurrence);
        System.out.println("CharacterCooccurrence finishes successfully...");

        System.out.println("CharacterRelationGraph begins...");
        String[] args4CharacterRelationGraph = {"./temp/temp2", "./temp/temp3"};
        CharacterRelationGraph.main(args4CharacterRelationGraph);
        System.out.println("CharacterRelationGraph finishes successfully...");
        */

        System.out.println("PageRank begins...");
        String[] args4PageRank = { };
        PageRankDriver.main(args4PageRank);
        System.out.println("PageRank finishes successfully...");

    }
}
