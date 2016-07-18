package MapReduce.LabelPropagation;


/**
 * Created by lubuntu on 16-7-12.
 */
public class LabelPropagationDriver {

    private static int times = 10;

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("usage: LabelPropagationDriver <in> <mid> <out-1> <out-2>");
            System.exit(2);
        }
        System.out.println("LabelPropagation.GraphBuilder");
        String[] args4GraphBuilder = {args[0], args[1] + "0"};
        GraphBuilder.main(args4GraphBuilder);
        System.out.println("LabelPropagation.LabelPropagationIter");
        String[] args4LabelPropagationIter = {"", ""};
        for(int i = 0; i < times; ++i) {
            if (i < times - 1) {
                args4LabelPropagationIter[0] = args[1] + String.valueOf(i);
                args4LabelPropagationIter[1] = args[1] + String.valueOf(i + 1);
            } else {
                args4LabelPropagationIter[0] = args[1] + String.valueOf(i);
                args4LabelPropagationIter[1] = args[2];
            }
            LabelPropagationIter.main(args4LabelPropagationIter);
        }
        System.out.println("LabelPropagation.LabelPropagationViewer");
        String[] args4LabelPropagationViewer = {args[2], args[3]};
        LabelPropagationViewer.main(args4LabelPropagationViewer);
    }
}
