package MapReduce.PageRank;

/**
 * Created by lubuntu on 16-7-11.
 */
public class PageRankDriver {

    private static int times = 10;

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("usage: PageRankDriver <in> <mid> <out-1> <out-2>");
            System.exit(2);
        }
        System.out.println("PageRank.GraphBuilder");
        String[] args4GraphBuilder = {args[0], args[1] + "0"};
        GraphBuilder.main(args4GraphBuilder);
        System.out.println("PageRank.PageRankIter");
        String[] args4PageRankIter = {"", ""};
        for(int i = 0; i < times; ++i) {
            if (i < times - 1) {
                args4PageRankIter[0] = args[1] + String.valueOf(i);
                args4PageRankIter[1] = args[1] + String.valueOf(i + 1);
            } else {
                args4PageRankIter[0] = args[1] + String.valueOf(i);
                args4PageRankIter[1] = args[2];
            }
            PageRankIter.main(args4PageRankIter);
        }
        System.out.println("PageRank.PageRankViewer");
        String[] args4PageRankViewer = {args[2], args[3]};
        PageRankViewer.main(args4PageRankViewer);
    }
}
