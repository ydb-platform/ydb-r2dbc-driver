package tech.ydb.io.r2dbc.query;

/**
 * @author kuleshovegor
 */
public class YdbParserUtils {
    static boolean parseAlterKeyword(char[] query, int offset) {
        if (query.length < (offset + 6)) {
            return false;
        }

        return (query[offset] | 32) == 'a'
                && (query[offset + 1] | 32) == 'l'
                && (query[offset + 2] | 32) == 't'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 'r'
                && isSpace(query[offset + 5]);
    }

    static boolean parseCreateKeyword(char[] query, int offset) {
        if (query.length < (offset + 7)) {
            return false;
        }

        return (query[offset] | 32) == 'c'
                && (query[offset + 1] | 32) == 'r'
                && (query[offset + 2] | 32) == 'e'
                && (query[offset + 3] | 32) == 'a'
                && (query[offset + 4] | 32) == 't'
                && (query[offset + 5] | 32) == 'e'
                && isSpace(query[offset + 6]);
    }

    static boolean parseDropKeyword(char[] query, int offset) {
        if (query.length < (offset + 5)) {
            return false;
        }

        return (query[offset] | 32) == 'd'
                && (query[offset + 1] | 32) == 'r'
                && (query[offset + 2] | 32) == 'o'
                && (query[offset + 3] | 32) == 'p'
                && isSpace(query[offset + 4]);
    }

    static boolean parseSelectKeyword(char[] query, int offset) {
        if (query.length < (offset + 7)) {
            return false;
        }

        return (query[offset] | 32) == 's'
                && (query[offset + 1] | 32) == 'e'
                && (query[offset + 2] | 32) == 'l'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 'c'
                && (query[offset + 5] | 32) == 't'
                && isSpace(query[offset + 6]);
    }

    static boolean parseUpdateKeyword(char[] query, int offset) {
        if (query.length < (offset + 7)) {
            return false;
        }

        return (query[offset] | 32) == 'u'
                && (query[offset + 1] | 32) == 'p'
                && (query[offset + 2] | 32) == 'd'
                && (query[offset + 3] | 32) == 'a'
                && (query[offset + 4] | 32) == 't'
                && (query[offset + 5] | 32) == 'e'
                && isSpace(query[offset + 6]);
    }

    static boolean parseUpsertKeyword(char[] query, int offset) {
        if (query.length < (offset + 7)) {
            return false;
        }

        return (query[offset] | 32) == 'u'
                && (query[offset + 1] | 32) == 'p'
                && (query[offset + 2] | 32) == 's'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 'r'
                && (query[offset + 5] | 32) == 't'
                && isSpace(query[offset + 6]);
    }

    static boolean parseInsertKeyword(char[] query, int offset) {
        if (query.length < (offset + 7)) {
            return false;
        }

        return (query[offset] | 32) == 'i'
                && (query[offset + 1] | 32) == 'n'
                && (query[offset + 2] | 32) == 's'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 'r'
                && (query[offset + 5] | 32) == 't'
                && isSpace(query[offset + 6]);
    }

    static boolean parseDeleteKeyword(char[] query, int offset) {
        if (query.length < (offset + 7)) {
            return false;
        }

        return (query[offset] | 32) == 'd'
                && (query[offset + 1] | 32) == 'e'
                && (query[offset + 2] | 32) == 'l'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 't'
                && (query[offset + 5] | 32) == 'e'
                && isSpace(query[offset + 6]);
    }

    static boolean parseReplaceKeyword(char[] query, int offset) {
        if (query.length < (offset + 8)) {
            return false;
        }

        return (query[offset] | 32) == 'r'
                && (query[offset + 1] | 32) == 'e'
                && (query[offset + 2] | 32) == 'p'
                && (query[offset + 3] | 32) == 'l'
                && (query[offset + 4] | 32) == 'a'
                && (query[offset + 5] | 32) == 'c'
                && (query[offset + 8] | 32) == 'e'
                && isSpace(query[offset + 7]);
    }

    private static boolean isSpace(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f';
    }
}
