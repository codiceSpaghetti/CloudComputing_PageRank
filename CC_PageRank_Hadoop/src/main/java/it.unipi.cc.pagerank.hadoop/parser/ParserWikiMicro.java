package it.unipi.cc.pagerank.hadoop.parser;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParserWikiMicro implements Parser {
    private String stringToParse;
    Matcher matcher;

    public ParserWikiMicro() {
    }

    @Override
    public void setStringToParse(String stringToParse) {
        this.stringToParse = stringToParse;
    }

    @Override
    public String getTitle() {
        this.matcher = parse("<title.*?>(.*?)<\\/title>");    // ".*?" matches any character (except for line
                                                                    // terminators) between zero and unlimited times,
                                                                    // as few times as possible
        if(matcher.find()) {
            return matcher.group(1).replace("\t", " "); //remove /t to be able to use it as a separator
        } else {
            return null;
        }
    }

    @Override
    public List<String> getOutLinks() {
        List<String> outLinks = new LinkedList<>();

        this.matcher = parse("\\[\\[(.*?)\\]\\]");
        while(matcher.find()) {
            outLinks.add(matcher.group(1).replace("\t", " ")); //remove /t to be able to use it as a separator
        }
        /* If we want to extract only links between <text></text> tags
        this.matcher = parse("<text.*?>(.*?)<\\/text>");
        if(matcher.find()) {
            final String textBody = matcher.group(1);
            this.matcher = parse("\\[\\[(.*?)\\]\\]", textBody);
            while(matcher.find()) {
                outLinks.add(matcher.group(1).replace("\t", " ")); //remove /t to be able to use it as a separator
            }
        }
        */
        return outLinks;
    }

    private Matcher parse(final String regex) {
        Pattern pattern = Pattern.compile(regex);
        return pattern.matcher(this.stringToParse);
    }

    private Matcher parse(final String regex, final String text) {
        Pattern pattern = Pattern.compile(regex);
        return pattern.matcher(text);
    }
}
