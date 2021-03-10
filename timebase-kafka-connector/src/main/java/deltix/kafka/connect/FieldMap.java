package deltix.kafka.connect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FieldMap {
    private char pathSeparator;
    private Map<String,String> forward = new HashMap<>(); //src to dest
    private Map<String,String> backward = new HashMap<>(); // dest to src
    private Map<String,String[]> sourcePaths = new HashMap<>(); //dest to src path

    public FieldMap(String names, char pathSeparator) {
        this.pathSeparator = pathSeparator;

        if (names != null && !names.trim().isEmpty()) {
            for (String namePair : names.split(",")) {
                String[] aliases = namePair.trim().split(":");
                if (aliases.length != 2) {
                    throw new IllegalArgumentException("Invalid field mapping format: " + names);
                }
                addAlias(aliases[0], aliases[1]);
            }
        }
    }

    public void addAlias(String srcField, String destField) {
        // validate and normalize parameters by removing any spaces
        String[] srcPath = splitPath(srcField);
        srcField = makePathString(srcPath);
        destField = destField.trim();

        if (srcField.isEmpty() || destField.isEmpty() || srcField.equals(destField)) {
            throw new IllegalArgumentException("Invalid field mapping: \"" + srcField + "\" -> \"" + destField + "\"");
        }
        if (forward.put(srcField, destField) != null) {
            throw new IllegalArgumentException("Source fields names must be unique");
        }
        if (backward.put(destField, srcField) != null) {
            throw new IllegalArgumentException("Destination fields names must be unique");
        }
        sourcePaths.put(destField, srcPath);
    }

    public Set<String> getSourceFields() {
      return forward.keySet();
    }

    public Set<String> getDestinationFields() {
        return backward.keySet();
    }

    public boolean hasSourceAlias(String srcField) {
        return forward.containsKey(srcField);
    }

    public String getDestination(String srcField) {
        return hasSourceAlias(srcField) ? forward.get(srcField) : srcField;
    }

    public boolean hasDestinationAlias(String destField) {
        return backward.containsKey(destField);
    }

    public String getSource(String destField) {
        return hasDestinationAlias(destField) ? backward.get(destField) : destField;
    }

    public String[] getSourcePath(String destField) {
        String[] path = sourcePaths.get(destField);
        if (path == null) {
            path = splitPath(destField);
            sourcePaths.put(destField, path);
        }
        return path;
    }

    public String addToPath(String fieldPath, String fieldName) {
        return fieldPath + pathSeparator + fieldName;
    }

    public String makePathString(String[] path) {
        if (path.length == 0)
            throw new IllegalArgumentException("Invalid path parameter");

        if (path.length == 1)
            return path[0];

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < path.length; i++) {
            if (i > 0)
                sb.append(pathSeparator);
            sb.append(path[i]);
        }
        return sb.toString();
    }

    public String[] splitPath(String path) {
        int index = path.indexOf(pathSeparator);
        if (index < 0)
            return new String[] { path.trim() };

        String element;
        int start = 0;
        ArrayList<String> pathElements = new ArrayList<>();
        while (index >= 0) {
            element = path.substring(start, index).trim();
            if (element.isEmpty())
                throw new IllegalArgumentException("Invalid path: " + path);
            pathElements.add(element);
            start = index + 1;
            index = path.indexOf(start, pathSeparator);
        }

        element = path.substring(start).trim();
        if (element.isEmpty())
            throw new IllegalArgumentException("Invalid path: " + path);
        pathElements.add(element);

        return pathElements.toArray(new String[0]);
    }

    public boolean isFieldPath(String fieldName) {
        return fieldName != null && fieldName.indexOf(pathSeparator) >= 0;
    }
}
