package JinYongsJiangHu.PageRank;

/**
 * Created by lubuntu on 16-7-11.
 */
public class PageRankDriver {

    private static int times = 10;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("usage: PageRankDriver <in> <out>");
            System.exit(2);
        }
        String[] args4GraphBuilder = {args[0], args[1]};
        GraphBuilder.main(args4GraphBuilder);


    }
}
