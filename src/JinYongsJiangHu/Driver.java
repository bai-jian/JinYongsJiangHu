package JinYongsJiangHu;

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

        System.out.println("InteractionExtractor begins...");
        String[] args4InteractionExtractor = {"./input/people_name_list.txt", "./input/novels", "./output"};
        InteractionExtractor.main(args4InteractionExtractor);
        System.out.println("InteractionExtractor finished successfully...");

    }
}
